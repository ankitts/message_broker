#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <unordered_map>

#include "broker/message.h"

// What deliver_next() hands back: the message plus the tag that identifies
// this particular delivery (used later to ack it).
struct Delivery {
    Message message;
    uint64_t tag;
};

// In-memory FIFO queue with ack tracking.
//   ready   -> waiting to be delivered
//   unacked -> delivered, waiting for the consumer to confirm
// Not thread-safe (yet).
class Queue {
public:
    explicit Queue(std::string name) : m_name(std::move(name)) {}

    const std::string& name() const { return m_name; }

    // Add a message to the back of the ready line.
    void push(Message msg);

    // Move the oldest ready message to unacked and return it with a fresh tag.
    // Returns nullopt if nothing is ready.
    std::optional<Delivery> deliver_next();

    // Confirm a delivery. Returns false if the tag is unknown (never issued
    // or already acked); the queue is left unchanged in that case.
    bool ack(uint64_t tag);

    size_t readyCount() const;
    size_t unackedCount() const;

private:
    std::string m_name;
    std::deque<Message> m_ready;
    std::unordered_map<uint64_t, Message> m_unacked;
    uint64_t m_last_tag = 0;
};