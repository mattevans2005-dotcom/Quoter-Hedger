#ifndef STATE_H
#define STATE_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <chrono>

const std::string SENDER = "QUOTE001"; //grading environment will proivde sender, use this for local testing

const int VOL_WINDOW = 1500;

struct InstrumentMeta
{
    int tickSize = 0;
    int refPrice = 0;
    int band = 0;

    int minVolume = 0;
    int maxVolume = 0;

    int positionLimit = 0;
    int maxTps = 0;

    int lastTradedPrice = 0;
};


struct Order
{
    std::string id;
    std::string feed; 

    char side;

    int originalVolume;
    int filledVolume;
    int remaining;

    int price;
};


struct BBO
{
    std::string feed;

    int bidPrice = 0;
    int bidVolume = 0;
    int askPrice = 0;
    int askVolume = 0;
    double microprice = 0;

    double mid = 0.0;
    double previousMid = 0.0;
    double quoteMove = 0.0;
    double standardisedMove = 0.0;

    double volatility = 0.0;
    double moveVolatility = 0.0;

    std::deque<double> midHistory;
    std::deque<double> moveHistory;
    std::deque<double> standardisedMoveHistory;

    double movingAverage = 0.0;
    double previousMovingAverage = 0.0;
};

struct StrategyState
{
    bool dataready = false;


    std::string sender;
    std::unordered_map<std::string, Order> orders;
    std::unordered_set<std::string> removedOrders;

    double cash = 0.0;
    double pnl = 0.0;
    int fills = 0;
    int position = 0;
    double movingAverage = 0.0;
    double volatility = 0.0;


    std::string currentBidId;
    std::string currentAskId;

    int currentBidPrice = 0;
    int currentAskPrice = 0;
    int desiredBid = 0;
    int desiredAsk = 0;
    int orderCounter = 0;
    int referenceMid = 0.0;

    std::unordered_map<std::string, BBO> bboMap;
    std::string quoteFeed = "AAM6";
    std::string signalFeed1 = "AAH6";
    std::string signalFeed2 = "AAU6";


    InstrumentMeta meta;


    double gamma = 0.1;
    double k = 1.0;
    double horizon = 1.5; // seconds
    std::chrono::steady_clock::time_point startTime;
    double reservationPrice;
};


#endif