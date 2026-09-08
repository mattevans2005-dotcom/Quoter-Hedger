#include "con_nats.h"

#include <iostream>

natsConnection* ConnectToNATS(const std::string& natsUrl)
{
    std::cout << "Connecting to NATS at "
              << natsUrl
              << std::endl;

    natsConnection* connection = nullptr;

    natsStatus status =
        natsConnection_ConnectTo(
            &connection,
            natsUrl.c_str()
        );

    if (status != NATS_OK)
    {
        std::cerr << "Failed to connect to NATS: "
                  << natsStatus_GetText(status)
                  << std::endl;

        return nullptr;
    }

    std::cout << "Connected to NATS!"
              << std::endl;

    return connection;
}


