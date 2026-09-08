#include "strategy.h"
#include "orders.h"
#include <cmath>
#include <iostream>
#include <algorithm>
#include <chrono>

void quote(
    natsConnection* connection,
    StrategyState& state)
{

    BBO& bbo =
        state.bboMap[state.quoteFeed];

    if (bbo.bidPrice == 0 ||
        bbo.askPrice == 0)
    {
        return;
    }
    int volume = 3;

    // ---------------------------------
    // BID
    // ---------------------------------

    if (state.position < 3 && state.desiredBid != 0)
    {
        if (state.currentBidId.empty())
        {
            std::string orderId =
                generateOrderId(state);

            if (sendLimitOrder(
                    connection,
                    &state,
                    state.quoteFeed,
                    orderId,
                    'B',
                    volume,
                    state.desiredBid))
            {
                state.currentBidId = orderId;
                state.currentBidPrice = state.desiredBid;
            }
        }

    }


    // ---------------------------------
    // ASK
    // ---------------------------------

    if (state.position > -3 && state.desiredAsk != 0)
    {
        if (state.currentAskId.empty())
        {
            std::string orderId =
                generateOrderId(state);

            if (sendLimitOrder(
                    connection,
                    &state,
                    state.quoteFeed,
                    orderId,
                    'S',
                    volume,
                    state.desiredAsk))
            {
                state.currentAskId = orderId;
                state.currentAskPrice = state.desiredAsk;
            }
        }

    }
} 
void cancelCurrentBid(
    natsConnection* connection,
    StrategyState& state)
{
    if (state.currentBidId.empty())
        return;

    // Check whether the order is still active
    if (state.orders.find(state.currentBidId) == state.orders.end())
    {
        state.currentBidId.clear();
        state.currentBidPrice = 0;
        return;
    }

    if (cancelOrder(
            connection,
            &state,
            state.quoteFeed,
            state.currentBidId))
    {
        state.currentBidId.clear();
        state.currentBidPrice = 0;
    }
}

void cancelCurrentAsk(
    natsConnection* connection,
    StrategyState& state)
{
    if (state.currentAskId.empty())
        return;

    // Check whether the order is still active
    if (state.orders.find(state.currentAskId) == state.orders.end())
    {
        state.currentAskId.clear();
        state.currentAskPrice = 0;
        return;
    }

    if (cancelOrder(
            connection,
            &state,
            state.quoteFeed,
            state.currentAskId))
    {
        state.currentAskId.clear();
        state.currentAskPrice = 0;
    }
}

double calculateVolatility(
    const std::deque<double>& midHistory)
{
    if (midHistory.size() < 2)
    {
        return 0.0;
    }

    // Calculate mean price change
    double sum = 0.0;

    for (size_t i = 1; i < midHistory.size(); i++)
    {
        double change =
            midHistory[i] - midHistory[i - 1];

        sum += change;
    }

    double mean =
        sum / (midHistory.size() - 1);

    // Calculate variance
    double variance = 0.0;

    for (size_t i = 1; i < midHistory.size(); i++)
    {
        double change =
            midHistory[i] - midHistory[i - 1];

        variance +=
            (change - mean) * (change - mean);
    }

    variance /=
        (midHistory.size() - 1);

    // Standard deviation
    return std::sqrt(variance);
}


// -------------------------------
// Volatility-scaled tick offset
// -------------------------------
// Converts a raw price-diff volatility estimate into an integer
// number of ticks to improve the market by. Higher vol -> larger
// offset is "free" (would get filled anyway) and also riskier to
// sit inside for too long without moving; lower vol -> smaller
// offset since improving by more than necessary just gives away
// edge for no extra fill probability.
int computeImproveTicks(
    double volatility,
    double sensitivity,
    int minTicks,
    int maxTicks)
{
    int offset =
        static_cast<int>(
            std::round(sensitivity * volatility));

    if (offset < minTicks)
    {
        offset = minTicks;
    }
    else if (offset > maxTicks)
    {
        offset = maxTicks;
    }

    return offset;
}

void updateReferenceMid(StrategyState& state, const BBO& bbo)
{
    bool bidIsOurs = !state.currentBidId.empty() &&
                      bbo.bidPrice == state.currentBidPrice
                      && bbo.bidVolume <= state.orders[state.currentBidId].remaining;
    bool askIsOurs = !state.currentAskId.empty() &&
                      bbo.askPrice == state.currentAskPrice
                      && bbo.askVolume <= state.orders[state.currentAskId].remaining;

    // Only trust the live mid as a reference when we're not the one
    // setting either side of the touch -- otherwise we're just measuring
    // our own quote and feeding it back into itself.
    if (!bidIsOurs && !askIsOurs)
    {
        state.referenceMid = bbo.microprice;
    }
}


void generateQuotes(
    StrategyState& state,
    int& desiredBid,
    int& desiredAsk)
{
    // -------------------------------
    // Parameters
    // -------------------------------

    BBO& bbo =
        state.bboMap[state.quoteFeed];

    updateReferenceMid(state, bbo);

    double minimumHalfSpread = 1.0;
    double marketHalfSpread =
        (bbo.askPrice - state.referenceMid);

    double marketSpread =
        (bbo.askPrice - bbo.bidPrice);

    // Only quote at all when the market is wide enough to be
    // worth improving. Tune this empirically per-instrument
    // rather than treating 4 as universal (see spread-percentile
    // discussion if you want to make this adaptive later).
    double minimumSpreadToQuote = 1.0;

    // Tick-offset tuning
    double offsetSensitivity = 0.5; // ticks of improvement per unit of vol
    int    minImproveTicks   = 1;


    double sigma =
        state.volatility;

    // -------------------------------
    // Volatility-scaled tick improvement
    // -------------------------------
    // Cap below halfSpread so the improvement can never eat the
    // entire quoted half-spread (which would risk desiredBid
    // crossing desiredAsk once positionSkew/signal shift things).

    int maxImproveTicks =
        std::max(
            minImproveTicks,
            static_cast<int>(marketHalfSpread) - 1);

    int improveTicks =
        computeImproveTicks(
            state.volatility,
            offsetSensitivity,
            minImproveTicks,
            maxImproveTicks);


    state.startTime =
        std::chrono::steady_clock::now();

    double elapsed =
        std::chrono::duration<double>(
            std::chrono::steady_clock::now()
            - state.startTime
        ).count();

    double timeRemaining =
        std::max(0.0, state.horizon - elapsed);

    /*double inventoryRatio =
    static_cast<double>(state.position) / 50.0;

    double positionSkew =
        state.gamma
        * inventoryRatio
        * std::abs(inventoryRatio);*/

    double positionSkew = 
        state.gamma
        * state.position;

    state.reservationPrice = state.referenceMid;

    /*double halfSpread =
        0.5
        * state.gamma
        * sigma
        * sigma
        * timeRemaining
        +
        (1.0 / state.gamma)
        * std::log(
            1.0 + state.gamma / state.k
        );*/

    // -------------------------------
    // Generate quotes
    // -------------------------------

    state.desiredBid =
        static_cast<int>(
            std::round(
                state.referenceMid
                -improveTicks
            )
        );

    state.desiredAsk =
        static_cast<int>(
            std::round(
                state.referenceMid
                +improveTicks
            )
        );


    // -------------------------------
    // Prevent crossing the market
    // -------------------------------

    if (bbo.askPrice != 0 &&
        state.desiredBid >= bbo.askPrice)
    {


        state.desiredBid = 0;
    }

    if (bbo.bidPrice != 0 &&
        state.desiredAsk <= bbo.bidPrice)
    {


        state.desiredAsk = 0;
    }


    // -------------------------------
    // Prevent our own quotes
    // from crossing each other
    // -------------------------------

    if (state.desiredBid != 0 &&
        state.desiredAsk != 0 &&
        state.desiredBid >= state.desiredAsk)
    {

        state.desiredBid = 0;
        state.desiredAsk = 0;
    }
}

