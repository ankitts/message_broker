#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "broker/sink.h"

// Who consumes which queue, and which deliveries each connection still holds
// (delivered but not yet acked). Queues are referred to by name.
//
// No lock of its own: the CommandHandler's mutex protects it (see the
// locking rules in command_handler.h).
class ConsumerRegistry {
public:
    using SinkPtr = std::shared_ptr<Sink>;
    using Held = std::pair<std::string, uint64_t>;   // (queue, tag)

    // Returns false if `sink` already consumes `queue`.
    bool addConsumer(const std::string& queue, const SinkPtr& sink);

    // Next consumer of `queue` in round-robin order, or null if none.
    SinkPtr pickConsumer(const std::string& queue);

    void recordDelivery(const SinkPtr& sink, const std::string& queue, uint64_t tag);

    // True only if `sink` holds (queue, tag); the pair is removed. This is
    // the ownership check for ACK.
    bool confirmAck(const SinkPtr& sink, const std::string& queue, uint64_t tag);

    // Names of queues with at least one consumer.
    std::vector<std::string> queuesWithConsumers() const;

    // Drops `sink` everywhere. Returns what it still held, so a later step
    // can requeue those messages.
    std::vector<Held> removeConnection(const SinkPtr& sink);

private:
    struct Consumers {
        std::vector<SinkPtr> sinks;
        size_t next = 0;    // index of the next sink to pick
    };

    std::unordered_map<std::string, Consumers> m_consumers;
    // Keyed by the sink's address. Safe because a sink with held deliveries
    // is also in m_consumers, which keeps it alive until removeConnection().
    std::unordered_map<Sink*, std::set<Held>> m_held;
};
