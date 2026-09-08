#include "metadata.h"

#include <iostream>
#include <sstream>


bool getInstrumentMeta(
    natsConnection* connection,
    const std::string& feed,
    InstrumentMeta& meta)
{
    jsCtx* js = nullptr;
    natsStatus status;

    // Create JetStream context
    status = natsConnection_JetStream(
        &js,
        connection,
        nullptr
    );

    if (status != NATS_OK)
    {
        std::cerr << "Failed to create JetStream context: "
                  << natsStatus_GetText(status)
                  << std::endl;

        return false;
    }

    // Open the EX_META KV bucket
    kvStore* kv = nullptr;

    status = js_KeyValue(
        &kv,
        js,
        "EX_META"
    );

    if (status != NATS_OK)
    {
        std::cerr << "Failed to open EX_META: "
                  << natsStatus_GetText(status)
                  << std::endl;

        jsCtx_Destroy(js);

        return false;
    }

    // Get the value for this feed
    kvEntry* entry = nullptr;

    status = kvStore_Get(
        &entry,
        kv,
        feed.c_str()
    );

    if (status != NATS_OK)
    {
        std::cerr << "Failed to get metadata for "
                  << feed << ": "
                  << natsStatus_GetText(status)
                  << std::endl;

        kvStore_Destroy(kv);
        jsCtx_Destroy(js);

        return false;
    }


    std::string data =
        kvEntry_ValueString(entry);

    std::cout << "EX_META " << feed
              << ": " << data
              << std::endl;

              
    if (!parseMetadata(data, meta))
    {
    std::cerr << "Failed to parse metadata."
              << std::endl;

    kvEntry_Destroy(entry);
    kvStore_Destroy(kv);
    jsCtx_Destroy(js);

    return false;
    }

    // Cleanup
    kvEntry_Destroy(entry);
    kvStore_Destroy(kv);
    jsCtx_Destroy(js);

    return true;
}

bool parseMetadata(
    const std::string& data,
    InstrumentMeta& meta)
{
    std::stringstream ss(data);

    std::string field;

    while (ss >> field)
    {
        auto separator = field.find('=');

        if (separator == std::string::npos)
        {
            continue;
        }

        std::string key =
            field.substr(0, separator);

        std::string value =
            field.substr(separator + 1);

        std::cout << "Parsing: "
                  << key
                  << " = "
                  << value
                  << std::endl;

        if (key == "ticksize")
        {
            meta.tickSize = std::stoi(value);
        }
        else if (key == "ref_price")
        {
            meta.refPrice = std::stoi(value);
        }
        else if (key == "band")
        {
            meta.band = std::stoi(value);
        }
        else if (key == "min_volume")
        {
            meta.minVolume = std::stoi(value);
        }
        else if (key == "max_volume")
        {
            meta.maxVolume = std::stoi(value);
        }
        else if (key == "position_limit")
        {
            meta.positionLimit = std::stoi(value);
        }
        else if (key == "max_tps")
        {
            meta.maxTps = std::stoi(value);
        }
        else if (key == "last_traded_price")
        {
            meta.lastTradedPrice = std::stoi(value);
        }
    }

    return true;
}