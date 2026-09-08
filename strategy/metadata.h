#ifndef METADATA_H
#define METADATA_H

#include <nats/nats.h>
#include "state.h"

bool getInstrumentMeta(
    natsConnection* connection,
    const std::string& feed,
    InstrumentMeta& meta
);

bool parseMetadata(
    const std::string& data,
    InstrumentMeta& meta
);


#endif