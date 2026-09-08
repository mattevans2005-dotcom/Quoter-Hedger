#ifndef STRATEGY_H
#define STRATEGY_H

#include "nats/nats.h"
#include "state.h"

int computeImproveTicks(double sigma, double tick_size, double mid_price,
                         int min_ticks, int max_ticks, double sensitivity);

void quote(
    natsConnection* connection,
    StrategyState& state);

void cancelCurrentBid(
    natsConnection* connection,
    StrategyState& state);

void cancelCurrentAsk(
    natsConnection* connection,
    StrategyState& state);

double calculateVolatility(
    const std::deque<double>& midHistory);

void generateQuotes(
    StrategyState& state,
    int& desiredBid,
    int& desiredAsk);

void updateReferenceMid(StrategyState& state, const BBO& bbo);

#endif

    /*main.cpp
    con_nats.cpp
    market_data.cpp
    orders.cpp
    metadata.cpp
    strategy.cpp
    strategy_status.cpp*/
