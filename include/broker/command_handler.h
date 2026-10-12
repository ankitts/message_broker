#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "broker/broker.h"
#include "broker/protocol.h"

// Turns a parsed Command into a Broker call and a single-line JSON reply.
// Knows nothing about sockets (the transport adds the trailing "\n").
//
// Contract:
//   - every call returns exactly one reply: {"type":"OK"|"ERROR", ...}
//   - thread-safe: one instance is shared by all connection threads
//   - a bad command never changes broker state
//   - never throws
//
// Decisions (see the 3.1b design):
//   - Locking: one mutex here, held only around the broker call. The broker
//     has no locks of its own, so this handler must stay its only entry point.
//     Limitation: it serializes all clients. Per-queue locks are the way to
//     scale. Don't assume the lock is only ever held briefly (3.2 pushes to
//     consumers from the publishing thread).
//   - Message ids: 64-bit, start at 1, assigned under the lock together with
//     the publish so id order equals arrival order in the queues. Unique per
//     process; reset on restart until persistence.
//   - Newline: not part of a reply.
//   - PUBLISH replies include the assigned "id".
//   - A duplicate BIND is an ERROR (stricter than RabbitMQ).
//   - Reason strings are part of the protocol; clients may match on them.
//   - Broker false results get generic reasons for now; a result enum from
//     the broker would allow precise ones without changing the wire format.
//   - The broker is held by reference and must outlive the handler.
class CommandHandler {
public:
    explicit CommandHandler(Broker& broker) : m_broker(broker) {}

    // Parse + execute. Always returns a reply, never throws.
    std::string handleLine(const std::string& line);

    // Execute an already-parsed command. Always returns a reply, never throws.
    std::string handle(const Command& cmd);

private:
    // One overload per command type. handle() picks the right one via
    // std::visit; adding a Command alternative without an overload here
    // is a compile error. Each takes the lock only around the broker call.
    std::string execute(const DeclareQueueCmd& cmd);
    std::string execute(const DeclareExchangeCmd& cmd);
    std::string execute(const BindCmd& cmd);
    std::string execute(const PublishCmd& cmd);

    Broker& m_broker;
    std::mutex m_mutex;           // guards m_broker and m_next_id
    uint64_t m_next_id = 1;
};
