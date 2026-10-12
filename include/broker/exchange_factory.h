#pragma once

#include <memory>
#include <string>
#include <utility>

#include "broker/exchange.h"
#include "broker/direct_exchange.h"
#include "broker/fanout_exchange.h"

// Factory: builds the Exchange subclass matching `type`, so callers (the
// Broker) never need to know the concrete classes.
// Returns nullptr for types that are not implemented yet (Topic).
inline std::unique_ptr<Exchange> makeExchange(std::string name, ExchangeType type) {
    switch (type) {
        case ExchangeType::Direct:
            return std::make_unique<DirectExchange>(std::move(name));
        case ExchangeType::Fanout:
            return std::make_unique<FanoutExchange>(std::move(name));
        case ExchangeType::Topic:
            return nullptr;                           // not implemented yet
    }
    return nullptr;
}