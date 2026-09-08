#include "strategy_status.h"
#include <iostream>

void publishStatus(
    natsConnection* connection,
    StrategyState& state)
{
    std::string subject =
        "strat." + state.sender + ".status";

    std::string message =
        "position=" + std::to_string(state.position) +
        " cash=" + std::to_string(state.cash) +
        " pnl=" + std::to_string(state.pnl) +
        " fills=" + std::to_string(state.fills);

    // Print status
    std::cout << "[strategy] "
              << message
              << std::endl;

    // Publish status
    natsStatus status =
        natsConnection_PublishString(
            connection,
            subject.c_str(),
            message.c_str()
        );

    if (status != NATS_OK)
    {
        std::cerr << "Failed to publish status: "
                  << natsStatus_GetText(status)
                  << std::endl;
    }
}