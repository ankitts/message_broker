#pragma once

#include <cstddef>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "broker/message.h"
#include "broker/queue.h"

// How an exchange decides which queues get a message.
//   Direct -> queues bound under a key exactly equal to the message's key
//   Fanout -> every bound queue, the key is ignored
//   Topic  -> reserved; not implemented yet (the factory returns nullptr)
enum class ExchangeType { Direct, Fanout, Topic };

// Abstract base for all exchanges. It owns the name and type and implements
// publish(); subclasses only decide WHERE a message goes (bind + targets).
// Holds borrowed Queue* pointers: the Broker owns the queues and must outlive
// the exchanges. Not copyable. Not thread-safe (yet).
class Exchange {
public:
    Exchange(std::string name, ExchangeType type)
        : m_name(std::move(name)), m_type(type) {}

    virtual ~Exchange() = default;

    Exchange(const Exchange&) = delete;
    Exchange& operator=(const Exchange&) = delete;

    const std::string& name() const { return m_name; }
    ExchangeType get_exchange_type() const { return m_type; }

    // Attach a queue under a routing key. Returns false if the queue is null
    // or already bound (a rejected bind changes nothing).
    virtual bool bind(const std::string& routing_key, Queue* queue) = 0;

    // Copy the message into every queue targets() picks. Returns how many
    // queues received it (0 means the message was dropped).
    size_t publish(const Message& msg);

protected:
    // The routing decision: which queues should receive this message.
    // Must never return null pointers.
    virtual std::vector<Queue*> targets(const Message& msg) = 0;

private:
    std::string m_name;
    ExchangeType m_type;
};