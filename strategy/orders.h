#ifndef ORDERS_H
#define ORDERS_H

#include <nats/nats.h>
#include <string>
#include <unordered_map>
#include "state.h"



// Generate a new unique 8-character order ID
std::string generateOrderId(StrategyState& state);

// Send a limit order to the exchange
bool sendLimitOrder(
    natsConnection* connection,
    StrategyState* state,
    const std::string& feed,
    const std::string& orderId,
    char side,
    int volume,
    int price
);

// Cancel one of our orders
bool cancelOrder(
    natsConnection* connection,
    StrategyState* state,
    const std::string& feed,
    const std::string& orderId
);


// Cancel both current quotes
void cancelQuotes(
    natsConnection* connection,
    StrategyState* state,
    std::string& currentBidId,
    std::string& currentAskId
);

void test(bool orderSent, natsConnection* connection, StrategyState& state);

#endif