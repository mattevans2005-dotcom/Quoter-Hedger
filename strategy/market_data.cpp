#include "market_data.h"
#include "strategy.h"
#include "strategy_status.h"
#include <iostream>
#include <sstream>


natsSubscription* BBOSubscription(
    natsConnection* connection,
    StrategyState* state
)
{
    natsSubscription* subscription = nullptr;

    natsStatus status =
        natsConnection_Subscribe(
            &subscription,
            connection,
            "ex.bbo.*",
            bboCallback,
            state
        );

    if (status != NATS_OK)
    {
        std::cerr << "Failed to subscribe to BBO: "
                  << natsStatus_GetText(status)
                  << std::endl;

        return nullptr;
    }

    std::cout << "Subscribed to BBO market data."
              << std::endl;

    return subscription;
}

natsSubscription* MarketDataSubscription(
    natsConnection* connection,
    StrategyState* state
)
{
    natsSubscription* subscription = nullptr;

    // Subscribe to our own market data
    std::string subject =
        "ex.md.*." + state->sender;

    natsStatus status =
        natsConnection_Subscribe(
            &subscription,
            connection,
            subject.c_str(),
            marketDataCallback,
            state
        );

    if (status != NATS_OK)
    {
        std::cerr << "Failed to subscribe to market data: "
                  << natsStatus_GetText(status)
                  << std::endl;

        return nullptr;
    }

    std::cout << "Subscribed to own market data: "
              << subject
              << std::endl;

    return subscription;
}

void bboCallback(
    natsConnection* connection,
    natsSubscription* subscription,
    natsMsg* message,
    void* closure)
{
    StrategyState* state =
        static_cast<StrategyState*>(closure);

    std::string data =
        natsMsg_GetData(message);

    std::stringstream ss(data);

    long long timestamp;
    std::string feed;

    std::string bidPrice;
    std::string bidVolume;

    std::string askPrice;
    std::string askVolume;

    ss >> timestamp
       >> feed
       >> bidPrice
       >> bidVolume
       >> askPrice
       >> askVolume;

    // -----------------------------------------
    // Check parsing
    // -----------------------------------------

    if (ss.fail())
    {
        std::cerr << "Failed to parse BBO: "
                  << data
                  << std::endl;

        natsMsg_Destroy(message);
        return;
    }

    // -----------------------------------------
    // Get BBO for this specific feed
    // -----------------------------------------

    BBO& bbo =
        state->bboMap[feed];

    bbo.feed = feed;

    // -----------------------------------------
    // Bid
    // -----------------------------------------

    if (bidPrice == "-")
    {
        bbo.bidPrice = 0;
        bbo.bidVolume = 0;
    }
    else
    {
        bbo.bidPrice =
            std::stoi(bidPrice);

        bbo.bidVolume =
            std::stoi(bidVolume);
    }

    // -----------------------------------------
    // Ask
    // -----------------------------------------

    if (askPrice == "-")
    {
        bbo.askPrice = 0;
        bbo.askVolume = 0;
    }
    else
    {
        bbo.askPrice =
            std::stoi(askPrice);

        bbo.askVolume =
            std::stoi(askVolume);
    }

    // -----------------------------------------
    // Calculate mid
    // -----------------------------------------

    if (bbo.bidPrice != 0 &&
        bbo.askPrice != 0)
    {

        // Calculate new mid
        bbo.mid =
            (bbo.bidPrice + bbo.askPrice)
            / 2.0;
        bbo.microprice = 
            (bbo.askPrice * bbo.bidVolume + bbo.bidPrice * bbo.askVolume)
            / (double)(bbo.bidVolume + bbo.askVolume);

        // -----------------------------------------
        // Add mid to history
        // -----------------------------------------

        bbo.midHistory.push_back(bbo.mid);

        if (bbo.midHistory.size() > VOL_WINDOW)
        {
            bbo.midHistory.pop_front();
        }
        // -----------------------------------------
        // Calculate statistics
        // -----------------------------------------


        if (bbo.midHistory.size() >= VOL_WINDOW)
        {
            // Volatility
            bbo.volatility =
                calculateVolatility(
                    bbo.midHistory
                );

            // -----------------------------------------
            // Calculate new MA
            // -----------------------------------------

            double sum = 0.0;

            for (double mid :
                bbo.midHistory)
            {
                sum += mid;
            }

            bbo.movingAverage =
                sum /
                bbo.midHistory.size();

            state->dataready = true;
            // -----------------------------------------
            // Main quoting feed
            // -----------------------------------------

            if (bbo.feed == state->quoteFeed)
            {
                state->movingAverage =
                    bbo.movingAverage;

                state->volatility =
                    bbo.volatility;


                // -----------------------------------------
                // PnL
                // -----------------------------------------

                state->pnl =
                    state->cash +
                    state->position * state->referenceMid;

                 

                std::cout
                    << "BBO | Feed: "
                    << bbo.feed

                    << " | Bid: ";

                if (bbo.bidPrice != 0)
                {
                    std::cout
                        << bbo.bidPrice
                        << " x "
                        << bbo.bidVolume;
                }
                else
                {
                    std::cout << "EMPTY";
                }

                std::cout
                    << " | Ask: ";

                if (bbo.askPrice != 0)
                {
                    std::cout
                        << bbo.askPrice
                        << " x "
                        << bbo.askVolume;
                }
                else
                {
                    std::cout << "EMPTY";
                }

                std::cout
                    << " | Mid: "
                    << bbo.mid

                    << " | MA: "
                    << state->movingAverage

                    << " | Vol: "
                    << state->volatility

                    << " | Res: "
                    << state->reservationPrice

                    << " | Cash: "
                    << state->cash

                    << " | PnL: "
                    << state->pnl

                    << " | Fills: "
                    << state->fills

                    << " | Position: "
                    << state->position

                    << std::endl;
            }
        }
    }
    else
    {
        bbo.mid = 0.0;
        bbo.quoteMove = 0.0;
    }
 

    natsMsg_Destroy(message);
}



void marketDataCallback(
    natsConnection* connection,
    natsSubscription* subscription,
    natsMsg* message,
    void* closure)
{
    StrategyState* state =
        static_cast<StrategyState*>(closure);

    std::string data =
        natsMsg_GetData(message);

    std::stringstream ss(data);

    long long timestamp;
    char type;

    ss >> timestamp >> type;

    std::cout << "\n--- MARKET DATA ---\n";
    std::cout << data << std::endl;


    // --------------------------------------------------
    // ORDER ADDED
    // --------------------------------------------------

    if (type == 'A')
    {
        std::string orderId;
        char side;
        int volume;
        int price;

        ss >> orderId
           >> side
           >> volume
           >> price;

        std::cout << "Order added: "
                  << orderId
                  << std::endl;
    }


    // --------------------------------------------------
    // EXECUTION
    // --------------------------------------------------

    else if (type == 'E')
    {
        std::string incoming;
        std::string resting;
        int volume;
        int price;
        std::string matchId;
        char aggressorSide;

        ss >> incoming
           >> resting
           >> volume
           >> price
           >> matchId
           >> aggressorSide;

        std::cout << "EXECUTION!"
                  << std::endl;

        std::cout << "Incoming: "
                  << incoming
                  << std::endl;

        std::cout << "Resting: "
                  << resting
                  << std::endl;

        std::cout << "Volume: "
                  << volume
                  << std::endl;

        std::cout << "Price: "
                  << price
                  << std::endl;

        std::cout << "Aggressor: "
                  << aggressorSide
                  << std::endl;


        // --------------------------------------------------
        // Check whether RESTING order is ours
        // --------------------------------------------------

        auto separator = resting.find(':');

        if (separator != std::string::npos)
        {
            std::string sender =
                resting.substr(0, separator);

            std::string orderId =
                resting.substr(separator + 1);

            if (sender == state->sender)
            {
                auto it =
                    state->orders.find(orderId);

                if (it != state->orders.end())
                {
                    Order& order = it->second;

                    order.filledVolume += volume;


                    // Update position and cash
                    if (order.side == 'B')
                    {
                        state->position += volume;
                        state->cash -= volume * price;
                    }
                    else if (order.side == 'S')
                    {
                        state->position -= volume;
                        state->cash += volume * price;
                    }

                    order.remaining = order.originalVolume - order.filledVolume;

                    state->fills++;


                    std::cout << "MY FILL"
                              << std::endl;

                    std::cout << "Order: "
                              << orderId
                              << std::endl;

                    std::cout << "Side: "
                              << order.side
                              << std::endl;

                    std::cout << "Execution volume: "
                              << volume
                              << std::endl;

                    std::cout << "Filled: "
                              << order.filledVolume
                              << " / "
                              << order.originalVolume
                              << std::endl;

                    std::cout << "Position: "
                              << state->position
                              << std::endl;


                    // --------------------------------------------------
                    // Order completely filled
                    // --------------------------------------------------

                    if (order.filledVolume >=
                        order.originalVolume)
                    {
                        std::cout
                            << "ORDER COMPLETELY FILLED"
                            << std::endl;


                        // Clear current BID ID
                        if (orderId ==
                            state->currentBidId)
                        {
                            std::cout
                                << "Clearing BID ID: "
                                << orderId
                                << std::endl;

                            state->currentBidId.clear();
                            state->currentBidPrice = 0;
                        }


                        // Clear current ASK ID
                        if (orderId ==
                            state->currentAskId)
                        {
                            std::cout
                                << "Clearing ASK ID: "
                                << orderId
                                << std::endl;

                            state->currentAskId.clear();
                            state->currentAskPrice = 0;
                        }


                        // Remove completed order
                        state->orders.erase(it);
                    }
                }
            }
        }
    }


    // --------------------------------------------------
    // ORDER CANCELLED / REMOVED
    // --------------------------------------------------

    else if (type == 'C')
    {
        std::string orderRef;
        ss >> orderRef;

        std::cout
            << "Order removed: "
            << orderRef
            << std::endl;

        auto separator = orderRef.find(':');

        if (separator != std::string::npos)
        {
            std::string sender =
                orderRef.substr(0, separator);

            std::string orderId =
                orderRef.substr(separator + 1);

            if (sender == state->sender)
            {
                std::cout
                    << "Our order removed: "
                    << orderId
                    << std::endl;

                // Remember that this order is no longer active.
                state->removedOrders.insert(orderId);

                // Remove from our local active-order map.
                state->orders.erase(orderId);

                // Clear current bid if this was the bid.
                if (state->currentBidId == orderId)
                {
                    state->currentBidId.clear();
                    state->currentBidPrice = 0;
                }

                // Clear current ask if this was the ask.
                if (state->currentAskId == orderId)
                {
                    state->currentAskId.clear();
                    state->currentAskPrice = 0;
                }
            }
        }
    }

    // --------------------------------------------------
    // TRADE
    // --------------------------------------------------

    else if (type == 'T')
    {
        std::cout << "Trade reported."
                  << std::endl;

        std::string incoming;
        std::string resting;
        int volume;
        int price;
        std::string matchId;
        char aggressorSide;

        ss >> incoming
           >> resting
           >> volume
           >> price
           >> matchId
           >> aggressorSide;


        std::cout << "Incoming: "
                  << incoming
                  << std::endl;

        std::cout << "Resting: "
                  << resting
                  << std::endl;

        std::cout << "Volume: "
                  << volume
                  << std::endl;

        std::cout << "Price: "
                  << price
                  << std::endl;

        std::cout << "Match ID: "
                  << matchId
                  << std::endl;

        std::cout << "Aggressor: "
                  << aggressorSide
                  << std::endl;


        // --------------------------------------------------
        // Check INCOMING order
        // --------------------------------------------------

        auto incomingSeparator =
            incoming.find(':');

        if (incomingSeparator !=
            std::string::npos)
        {
            std::string incomingSender =
                incoming.substr(
                    0,
                    incomingSeparator);

            std::string incomingOrderId =
                incoming.substr(
                    incomingSeparator + 1);


            if (incomingSender ==
                state->sender)
            {
                std::cout
                    << "Our INCOMING order traded: "
                    << incomingOrderId
                    << std::endl;


                // Clear current BID ID
                if (incomingOrderId ==
                    state->currentBidId)
                {
                    std::cout
                        << "Clearing BID ID: "
                        << incomingOrderId
                        << std::endl;

                    state->currentBidId.clear();
                    state->currentBidPrice = 0;
                }


                // Clear current ASK ID
                if (incomingOrderId ==
                    state->currentAskId)
                {
                    std::cout
                        << "Clearing ASK ID: "
                        << incomingOrderId
                        << std::endl;

                    state->currentAskId.clear();
                    state->currentAskPrice = 0;
                }


                // If exchange has already removed
                // the order, remove it locally too.
                state->orders.erase(
                    incomingOrderId);
            }
        }


        // --------------------------------------------------
        // Check RESTING order
        // --------------------------------------------------

        auto restingSeparator =
            resting.find(':');

        if (restingSeparator !=
            std::string::npos)
        {
            std::string restingSender =
                resting.substr(
                    0,
                    restingSeparator);

            std::string restingOrderId =
                resting.substr(
                    restingSeparator + 1);


            if (restingSender ==
                state->sender)
            {
                std::cout
                    << "Our RESTING order traded: "
                    << restingOrderId
                    << std::endl;


                // Clear current BID ID
                if (restingOrderId ==
                    state->currentBidId)
                {
                    std::cout
                        << "Clearing BID ID: "
                        << restingOrderId
                        << std::endl;

                    state->currentBidId.clear();
                    state->currentBidPrice = 0;
                }


                // Clear current ASK ID
                if (restingOrderId ==
                    state->currentAskId)
                {
                    std::cout
                        << "Clearing ASK ID: "
                        << restingOrderId
                        << std::endl;

                    state->currentAskId.clear();
                    state->currentAskPrice = 0;
                }


                // Remove from local active orders
                state->orders.erase(
                    restingOrderId);
            }
        }
    }


    natsMsg_Destroy(message);
}
