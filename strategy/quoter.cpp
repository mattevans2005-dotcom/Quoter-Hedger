// Basic quoting (market-making) strategy in C++, using nats.c.
//
// Design notes:
//  - All NATS subscription callbacks do ONE thing: push the raw payload onto
//    a thread-safe queue. All decision-making and order sending happens on
//    the main thread, which drains the queue. This avoids making blocking
//    request/reply calls from inside a library callback thread, which is a
//    known hazard with several NATS client architectures.
//  - Because of that, Quoter's internal state is only ever touched from the
//    main thread and needs no locking of its own.
//  - Tick size: rather than depend on an unverified JetStream KV read via
//    nats.c (I could not compile/test that API shape in this environment),
//    tick size is read from $TICK_SIZE (default 1). If you confirm the
//    nats.c KV API on your machine, swap fetchTickSize() below for a real
//    EX_META read -- the call site is isolated for exactly that reason.

#include <nats.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_running{true};

void onSignal(int) { g_running = false; }

std::string envOr(const char* name, const std::string& fallback) {
    const char* v = std::getenv(name);
    return v ? std::string(v) : fallback;
}

int envOrInt(const char* name, int fallback) {
    const char* v = std::getenv(name);
    return v ? std::atoi(v) : fallback;
}

double envOrDouble(const char* name, double fallback) {
    const char* v = std::getenv(name);
    return v ? std::atof(v) : fallback;
}

std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream iss(s);
    std::string tok;
    while (iss >> tok) out.push_back(tok);
    return out;
}

// See header note: swap this out for a real EX_META KV read once you've
// confirmed the nats.c JetStream KV API compiles on your setup.
long long fetchTickSize() {
    return envOrInt("TICK_SIZE", 1);
}

struct RestingOrder {
    std::string oid;
    char side;   // 'B' or 'S'
    long long price;
    int volume;
    int remaining;
};

// ---------------------------------------------------------------------
// Thread-safe inbound queue: callbacks push, main thread pops.
// ---------------------------------------------------------------------

struct InboundMsg {
    std::string source;  // "BBO" or "MD"
    std::string data;
};

std::mutex g_qmu;
std::condition_variable g_qcv;
std::queue<InboundMsg> g_queue;

void enqueue(const std::string& source, const std::string& data) {
    {
        std::lock_guard<std::mutex> lk(g_qmu);
        g_queue.push(InboundMsg{source, data});
    }
    g_qcv.notify_one();
}

// ---------------------------------------------------------------------
// Quoter: all methods assumed called from a single thread (main loop).
// ---------------------------------------------------------------------

class Quoter {
public:
    Quoter(natsConnection* nc, long long tickSize)
        : nc_(nc), tickSize_(tickSize) {
        std::random_device rd;
        oidCounter_ = rd() % 80000000UL;
    }

    std::string feed;
    std::string sender;
    int clip = 3;
    int maxPos = 30;
    int minSpreadTicks = 4;
    int improveTicks = 1;
    double skewTicksPerUnit = 0.05;

    void onBbo(const std::string& payload) {
        auto f = split(payload);
        if (f.size() < 6) return;
        bestBid_ = (f[2] == "-") ? std::nullopt
                                  : std::optional<long long>(std::stoll(f[2]));
        bestAsk_ = (f[4] == "-") ? std::nullopt
                                  : std::optional<long long>(std::stoll(f[4]));
        requote();
    }

    void onOwnMd(const std::string& payload) {
        // <ts> E <incoming:17> <resting:17> <volume> <price> <matchid> <B|S>
        auto f = split(payload);
        if (f.size() < 8 || f[1] != "E") return;
        const std::string& incoming = f[2];
        const std::string& resting = f[3];
        int volume = std::stoi(f[4]);
        long long price = std::stoll(f[5]);
        char aggSide = f[7][0];

        for (auto* order : {&bidOrder_, &askOrder_}) {
            if (!order->has_value()) continue;
            std::string fullId = sender + ":" + (*order)->oid;
            char side;
            if (fullId == incoming) {
                side = aggSide;
            } else if (fullId == resting) {
                side = (aggSide == 'B') ? 'S' : 'B';
            } else {
                continue;
            }
            applyFill(side, volume, price);
            (*order)->remaining -= volume;
            fills_++;
            if ((*order)->remaining <= 0) order->reset();
        }
    }

    std::string statusLine() const {
        std::ostringstream oss;
        oss << "pos=" << position_ << " cash=" << (long long)cash_
            << " pnl=" << (long long)pnl() << " fills=" << fills_
            << " bid=" << (bidOrder_ ? std::to_string(bidOrder_->price) : "-")
            << " ask=" << (askOrder_ ? std::to_string(askOrder_->price) : "-");
        return oss.str();
    }

    void shutdown() {
        if (bidOrder_) sendCancel(*bidOrder_);
        if (askOrder_) sendCancel(*askOrder_);
        bidOrder_.reset();
        askOrder_.reset();
    }

private:
    natsConnection* nc_;
    long long tickSize_;

    std::optional<long long> bestBid_, bestAsk_;
    long long position_ = 0;
    double cash_ = 0.0;
    long fills_ = 0;

    std::optional<RestingOrder> bidOrder_, askOrder_;
    unsigned long oidCounter_;

    std::optional<double> mid() const {
        if (!bestBid_ || !bestAsk_) return std::nullopt;
        return (*bestBid_ + *bestAsk_) / 2.0;
    }

    double pnl() const {
        auto m = mid();
        return cash_ + (m ? position_ * (*m) : 0.0);
    }

    void applyFill(char side, int qty, long long px) {
        long long signedQty = (side == 'B') ? qty : -qty;
        position_ += signedQty;
        cash_ -= signedQty * (double)px;
    }

    std::string nextOid() {
        oidCounter_++;
        char buf[9];
        std::snprintf(buf, sizeof(buf), "%08lu", oidCounter_ % 100000000UL);
        return std::string(buf);
    }

    std::vector<std::string> sendRequest(const std::string& body) {
        natsMsg* reply = nullptr;
        std::string subject = "ex.req." + sender;
        natsStatus s = natsConnection_RequestString(&reply, nc_, subject.c_str(),
                                                     body.c_str(), 1000);
        if (s != NATS_OK || reply == nullptr) {
            std::fprintf(stderr, "[quoter] request failed (%d): %s\n", (int)s,
                         body.c_str());
            return {};
        }
        std::string data(natsMsg_GetData(reply), natsMsg_GetDataLength(reply));
        natsMsg_Destroy(reply);
        auto parts = split(data);
        if (parts.size() >= 2 && parts[1] == "N") {
            std::string code = parts.size() > 2 ? parts[2] : "?";
            std::fprintf(stderr, "[quoter] rejected: %s (req: %s)\n", code.c_str(),
                         body.c_str());
        }
        return parts;
    }

    void sendCancel(const RestingOrder& order) {
        std::string body = sender + " C " + feed + " " + order.oid;
        sendRequest(body);  // fire-and-forget; Y/N result not needed here
    }

    std::optional<RestingOrder> sendAdd(char side, long long price, int volume) {
        std::string oid = nextOid();
        std::string body = sender + " A " + feed + " " + oid + " " +
                            std::string(1, side) + " " + std::to_string(volume) +
                            " " + std::to_string(price) + " L";
        auto parts = sendRequest(body);
        if (parts.size() >= 3 && parts[1] == "Y") {
            int tradedNow = std::stoi(parts[2]);
            RestingOrder order{oid, side, price, volume, volume - tradedNow};
            if (tradedNow > 0) applyFill(side, tradedNow, price);
            return order;
        }
        return std::nullopt;
    }

    void updateSide(char side, long long desiredPrice,
                    std::optional<RestingOrder>& current, bool posOk) {
        if (!posOk) {
            if (current) {
                sendCancel(*current);
                current.reset();
            }
            return;
        }
        if (current && current->price == desiredPrice) return;
        if (current) sendCancel(*current);
        current = sendAdd(side, desiredPrice, clip);
    }

    void requote() {
        if (!bestBid_ || !bestAsk_) return;

        double spreadTicks = (double)(*bestAsk_ - *bestBid_) / (double)tickSize_;
        if (spreadTicks < minSpreadTicks) {
            if (bidOrder_) {
                sendCancel(*bidOrder_);
                bidOrder_.reset();
            }
            if (askOrder_) {
                sendCancel(*askOrder_);
                askOrder_.reset();
            }
            return;
        }

        double m = *mid();
        double skewShift = -skewTicksPerUnit * (double)tickSize_ * (double)position_;

        long long desiredBid = *bestBid_ + (long long)improveTicks * tickSize_;
        long long desiredAsk = *bestAsk_ - (long long)improveTicks * tickSize_;

        desiredBid = std::min<long long>(desiredBid, *bestAsk_ - tickSize_);
        desiredAsk = std::max<long long>(desiredAsk, *bestBid_ + tickSize_);

        long long dBid = (long long)std::llround((double)desiredBid + skewShift);
        long long dAsk = (long long)std::llround((double)desiredAsk + skewShift);
        if (dBid >= dAsk) dBid = dAsk - tickSize_;

        updateSide('B', dBid, bidOrder_, position_ < maxPos);
        updateSide('S', dAsk, askOrder_, position_ > -maxPos);
    }
};

Quoter* g_quoter = nullptr;

void onBboMsg(natsConnection*, natsSubscription*, natsMsg* msg, void*) {
    std::string data(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    natsMsg_Destroy(msg);
    enqueue("BBO", data);
}

void onOwnMdMsg(natsConnection*, natsSubscription*, natsMsg* msg, void*) {
    std::string data(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    natsMsg_Destroy(msg);
    enqueue("MD", data);
}

}  // namespace

int main() {
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    std::string natsUrl = envOr("NATS_URL", "nats://127.0.0.1:4222");
    std::string feed = envOr("FEED", "AAH6");
    std::string sender = envOr("SENDER", "CPPQTR01");
    long long tickSize = fetchTickSize();

    natsOptions* opts = nullptr;
    natsConnection* nc = nullptr;
    natsOptions_Create(&opts);
    natsOptions_SetURL(opts, natsUrl.c_str());
    natsStatus s = natsConnection_Connect(&nc, opts);
    if (s != NATS_OK) {
        std::fprintf(stderr, "[quoter] connect failed: %d\n", (int)s);
        return 1;
    }

    Quoter quoter(nc, tickSize);
    quoter.feed = feed;
    quoter.sender = sender;
    quoter.clip = envOrInt("QUOTER_CLIP", 3);
    quoter.maxPos = envOrInt("QUOTER_MAX_POS", 30);
    quoter.minSpreadTicks = envOrInt("QUOTER_MIN_SPREAD", 4);
    quoter.improveTicks = envOrInt("QUOTER_IMPROVE_TICKS", 1);
    quoter.skewTicksPerUnit = envOrDouble("QUOTER_SKEW_TICKS", 0.05);
    g_quoter = &quoter;

    std::fprintf(stderr,
                 "[quoter] %s quoting %s ticksize=%lld clip=%d min_spread=%dt "
                 "improve=%dt\n",
                 sender.c_str(), feed.c_str(), tickSize, quoter.clip,
                 quoter.minSpreadTicks, quoter.improveTicks);

    natsSubscription* bboSub = nullptr;
    natsSubscription* mdSub = nullptr;
    natsConnection_Subscribe(&bboSub, nc, ("ex.bbo." + feed).c_str(), onBboMsg,
                              nullptr);
    natsConnection_Subscribe(&mdSub, nc, ("ex.md." + feed + "." + sender).c_str(),
                              onOwnMdMsg, nullptr);

    auto lastStatus = std::chrono::steady_clock::now();
    while (g_running) {
        std::vector<InboundMsg> batch;
        {
            std::unique_lock<std::mutex> lk(g_qmu);
            g_qcv.wait_for(lk, std::chrono::milliseconds(100),
                           [] { return !g_queue.empty() || !g_running; });
            while (!g_queue.empty()) {
                batch.push_back(std::move(g_queue.front()));
                g_queue.pop();
            }
        }

        for (auto& m : batch) {
            if (m.source == "BBO") {
                quoter.onBbo(m.data);
            } else if (m.source == "MD") {
                quoter.onOwnMd(m.data);
            }
        }

        auto now = std::chrono::steady_clock::now();
        if (now - lastStatus >= std::chrono::seconds(1)) {
            std::string line = quoter.statusLine();
            std::fprintf(stdout, "[quoter] %s\n", line.c_str());
            std::fflush(stdout);
            natsConnection_PublishString(nc, ("strat." + sender + ".status").c_str(),
                                         line.c_str());
            lastStatus = now;
        }
    }

    std::fprintf(stderr, "[quoter] shutting down, cancelling resting orders\n");
    quoter.shutdown();

    natsSubscription_Destroy(bboSub);
    natsSubscription_Destroy(mdSub);
    natsConnection_Destroy(nc);
    natsOptions_Destroy(opts);
    return 0;
}