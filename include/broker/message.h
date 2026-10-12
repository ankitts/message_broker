#pragma once

#include <cstdint>
#include <string>
#include <utility>

// A single message. Plain data: copyable and movable by default.
// (Do not declare your own copy constructor: it silently disables the
// implicit move constructor, so every std::move on a Message would copy.)
struct Message {
    Message(uint64_t id, std::string routing_key, std::string body)
        : m_id(id),
          m_routing_key(std::move(routing_key)),
          m_body(std::move(body)) {}

    uint64_t m_id;
    std::string m_routing_key;
    std::string m_body;
};