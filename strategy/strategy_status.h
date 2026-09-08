#ifndef STRATEGY_STATUS_H
#define STRATEGY_STATUS_H

#include "strategy.h"
#include <nats/nats.h>

void publishStatus(
    natsConnection* connection,
    StrategyState& state
);

#endif