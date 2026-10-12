#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "broker/exchange.h"
#include "broker/message.h"
#include "broker/queue.h"

// Owns every queue and exchange and refers to them by name.
// Not thread-safe (yet).
class Broker {
public:
    // Create the queue if it does not exist; do nothing if it does.
    // Returns true: the queue now exists (declaring is idempotent).
    bool declareQueue(const std::string& queue_name);

    // Create the exchange if it does not exist. Re-declaring with the SAME
    // type is fine (true). Re-declaring with a DIFFERENT type is a conflict:
    // returns false and the existing exchange is left untouched.
    bool declareExchange(const std::string& exchange_name, ExchangeType exchange_type);

    // Bind a queue to an exchange under a routing key. Returns false if the
    // exchange or queue is not declared, or the binding already exists.
    // A failed bind creates nothing.
    bool bind(const std::string& exchange_name,
              const std::string& queue_name,
              const std::string& routing_key);

    // Route a message through the named exchange.
    //   nullopt   -> no such exchange (caller error)
    //   0         -> exchange exists but no queue matched (message dropped)
    //   n > 0     -> number of queues that received a copy
    std::optional<size_t> publish(const std::string& exchange_name, const Message& msg);

    // Borrowed (non-owning) access to a queue, or nullptr if not declared.
    // The pointer stays valid for the lifetime of the Broker.
    Queue* getQueue(const std::string& queue_name);

private:
    // Declaration order matters: members are destroyed in reverse order, so
    // exchanges (which hold raw Queue*) are destroyed BEFORE the queues.
    std::unordered_map<std::string, std::unique_ptr<Queue>> m_queues;
    std::unordered_map<std::string, std::unique_ptr<Exchange>> m_exchanges;
};