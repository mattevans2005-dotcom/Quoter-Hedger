#ifndef MARKET_DATA_H
#define MARKET_DATA_H

#include <nats/nats.h>
#include <string>
#include "state.h"



void bboCallback(
    natsConnection* connection,
    natsSubscription* subscription,
    natsMsg* message,
    void* closure
);

void marketDataCallback(
    natsConnection* connection,
    natsSubscription* subscription,
    natsMsg* message,
    void* closure
);

natsSubscription* BBOSubscription(
    natsConnection* connection,
    StrategyState* state
);

natsSubscription* MarketDataSubscription(
    natsConnection* connection,
    StrategyState* state
);

#endif