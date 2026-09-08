#include "orders.h"

#include <iostream>
#include <sstream>
#include <iomanip>
#include <thread>
#include <chrono>

#include <iomanip>
#include <sstream>

std::string generateOrderId(StrategyState& state)
{
    state.orderCounter++;

    std::stringstream ss;

    ss << "QUOTE"
       << std::setfill('0')
       << std::setw(3)
       << state.orderCounter;

    return ss.str();
}


bool sendLimitOrder(
    natsConnection* connection,
    StrategyState* state,
    const std::string& feed,
    const std::string& orderId,
    char side,
    int volume,
    int price)

{

    std::string subject = "ex.req." + state->sender;

    std::string request =
        state->sender + " A " +
        feed + " " +
        orderId + " " +
        side + " " +
        std::to_string(volume) + " " +
        std::to_string(price) + " L";

    std::cout << "\nSending order:\n"
              << request << std::endl;

    natsMsg* reply = nullptr;

    natsStatus status =
        natsConnection_RequestString(
            &reply,
            connection,
            subject.c_str(),
            request.c_str(),
            2000
        );

    if (status != NATS_OK)
    {
        std::cerr << "Order request failed: "
                  << natsStatus_GetText(status)
                  << std::endl;

        return false;
    }

    std::string response = natsMsg_GetData(reply);

    std::cout << "Exchange reply: "
              << response
              << std::endl;


    natsMsg_Destroy(reply);


    std::stringstream ss(response);

    std::string exchange;
    std::string result;

    ss >> exchange >> result;

    if (result == "Y") // NEED to check if immediate fills are double counted to position
    {
        int tradedVolume = 0;
        ss >> tradedVolume;

        Order order;

        order.id = orderId;
        order.feed = feed;
        order.side = side;
        order.originalVolume = volume;
        order.filledVolume = tradedVolume;
        order.price = price;

        state->orders[orderId] = order;

        // Update current quote
        if (side == 'B')
        {
            state->currentBidId = orderId;
            state->currentBidPrice = price;
        }
        else if (side == 'S')
        {
            state->currentAskId = orderId;
            state->currentAskPrice = price;
        }

        // Account for immediate fills
        if (side == 'B')
        {
            state->position += tradedVolume;
            state->cash -= tradedVolume * price;
        }
        else if (side == 'S')
        {
            state->position -= tradedVolume;
            state->cash += tradedVolume * price;
        }

        std::cout << "Order accepted."
                << std::endl;

        return true;
    }

    if (result == "N")
    {
        std::string code;
        ss >> code;

        std::cerr << "Order rejected. Code: "
                  << code
                  << std::endl;

        return false;
    }

    std::cerr << "Unknown exchange response: "
              << response
              << std::endl;

    return false;
}


bool cancelOrder(
    natsConnection* connection,
    StrategyState* state,
    const std::string& feed,
    const std::string& orderId)
{
    std::string subject = "ex.req." + state->sender;

    std::string request =
        state->sender + " C " +
        feed + " " +
        orderId;

    std::cout << "\nCancelling order:\n"
              << request
              << std::endl;

    natsMsg* reply = nullptr;

    natsStatus status =
        natsConnection_RequestString(
            &reply,
            connection,
            subject.c_str(),
            request.c_str(),
            2000
        );

    if (status != NATS_OK)
    {
        std::cerr << "Cancel request failed: "
                  << natsStatus_GetText(status)
                  << std::endl;

        return false;
    }

    std::string response =
        natsMsg_GetData(reply);

    std::cout << "Exchange reply: "
              << response
              << std::endl;

    std::stringstream ss(response);

    std::string exchange;
    std::string result;

    ss >> exchange >> result;

    if (result == "Y")
    {
        int cancelledVolume = 0;

        ss >> cancelledVolume;

        std::cout << "Order cancelled."
                  << std::endl;

        // Remove from our active-order map
        state->orders.erase(orderId);

        natsMsg_Destroy(reply);

        return true;
    }

    if (result == "N")
    {
        std::string code;
        std::string text;

        ss >> code;
        std::getline(ss, text);

        std::cerr << "Cancel rejected. Code: "
                << code
                << text
                << std::endl;

        if (code == "305")
        {
            std::cout
                << "Order "
                << orderId
                << " is already inactive. Removing locally."
                << std::endl;

            state->orders.erase(orderId);

            if (state->currentBidId == orderId)
            {
                state->currentBidId.clear();
                state->currentBidPrice = 0;
            }

            if (state->currentAskId == orderId)
            {
                state->currentAskId.clear();
                state->currentAskPrice = 0;
            }
        }

        natsMsg_Destroy(reply);
        return false;
    }
    std::cerr << "Unknown exchange response: "
              << response
              << std::endl;

    natsMsg_Destroy(reply);

    return false;
}



