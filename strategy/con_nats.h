#ifndef CON_NATS_H
#define CON_NATS_H

#include <nats/nats.h>
#include <string>


natsConnection* ConnectToNATS(const std::string& natsUrl);


#endif