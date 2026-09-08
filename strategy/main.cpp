#include <iostream>
#include <cstdlib>
#include <string>
#include <sstream>
#include <iomanip>
#include <thread>
#include <chrono>
// NATS C++ client
#include "con_nats.h"
#include "market_data.h"
#include "orders.h"
#include "state.h"
#include "metadata.h"
#include "strategy.h"
#include "strategy_status.h"




int main()
{

    StrategyState state;

    state.sender = SENDER;

    bool orderSent = false;

    // Connect to NATS
    const char* envUrl = std::getenv("NATS_URL");

    std::string natsUrl;

    if (envUrl != nullptr)
    {
        natsUrl = envUrl;
    }
    else
    {
        natsUrl = "nats://localhost:4222";
    }

    natsConnection* connection =
        ConnectToNATS(natsUrl);


    // Subscribe to BBO market data
    natsSubscription* bboSubscription =
        BBOSubscription(
            connection,
            &state
        );

    if (bboSubscription == nullptr)
    {
        natsConnection_Destroy(connection);
        return 1;
    }



    // Subscribe to Market Data
    natsSubscription* marketDataSubscription =
        MarketDataSubscription(
            connection,
            &state
        );

    if (marketDataSubscription == nullptr)
    {
        natsConnection_Destroy(connection);
        return 1;
    }

    if (!getInstrumentMeta(
        connection,
        state.quoteFeed, 
        state.meta))
    {
        std::cerr << "Could not load instrument metadata."
              << std::endl;

        return 1;
    }
    std::cout << "Tick size: "
          << state.meta.tickSize
          << std::endl;

    std::cout << "Reference price: "
          << state.meta.refPrice
          << std::endl;

    std::cout << "Price band: "
          << state.meta.band
          << std::endl;

    std::cout << "Position limit: "
          << state.meta.positionLimit
          << std::endl;




    auto lastStatus = std::chrono::steady_clock::now();
    // --------------------------------------------------
    // Receive messages
    // --------------------------------------------------
    while (true)
    {



        auto now =
            std::chrono::steady_clock::now();

        if (now - lastStatus >= std::chrono::milliseconds(100) && state.dataready == true)
        {

            quote(connection, state);


            lastStatus = now;
        }

        generateQuotes(
            state,
            state.desiredBid,
            state.desiredAsk
        );    

        if (!state.currentBidId.empty())
        {
            auto it = state.orders.find(state.currentBidId);

            if (it != state.orders.end() && state.currentBidPrice != state.desiredBid)
            {
                cancelCurrentBid(connection, state);
            }
        }

        if (!state.currentAskId.empty())
        {
            auto it = state.orders.find(state.currentAskId);

            if (it != state.orders.end() && state.currentAskPrice != state.desiredAsk)
            {
                cancelCurrentAsk(connection, state);
            }
        }


    }


    // -----------------------------------------
    // Cleanup
    // -----------------------------------------

    natsSubscription_Destroy(marketDataSubscription);
    natsSubscription_Destroy(bboSubscription);

    natsConnection_Destroy(connection);

    return 0;
}