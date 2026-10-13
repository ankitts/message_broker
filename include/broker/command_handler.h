#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "broker/broker.h"
#include "broker/consumer_registry.h"
#include "broker/protocol.h"
#include "broker/sink.h"

// A line the transport must send to someone other than the caller.
struct Push {
    std::shared_ptr<Sink> to;
    std::string line;
};

// What one command produced: the reply for the sender, then pushes (DELIVER
// lines) for any connection. The transport sends the reply FIRST and then the
// pushes, which is what guarantees "OK for CONSUME before its DELIVERs".
struct Result {
    std::string reply;
    std::vector<Push> pushes;
};

// Turns a parsed Command into a Broker call and a single-line JSON reply,
// plus any DELIVER pushes. Knows nothing about sockets (the transport adds
// the trailing "\n" and does all sending).
//
// Contract:
//   - every call returns exactly one reply: {"type":"OK"|"ERROR", ...}
//   - thread-safe: one instance is shared by all connection threads
//   - a bad command never changes broker state
//   - never throws
//
// LOCKING RULES (3.2):
//   1. One lock for everything: m_mutex guards the broker, the consumer
//      registry and the id counter. No other lock protects broker state.
//   2. No socket I/O while holding it. Pushes are collected under the lock
//      and returned; the caller sends them after the lock is released.
//   3. SocketConnection::send_mutex_ is a leaf lock: nothing else is
//      acquired while holding it, so it cannot deadlock with rule 1.
//   4. A connection must not be closed or destroyed while a thread may still
//      send to it. Connections are shared_ptr, so the last holder closes it.
//
// Decisions (see the 3.1b / 3.2 designs):
//   - Locking: the one mutex serializes all clients. Per-queue locks are the
//     way to scale.
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
//   - Consuming: delivery is round-robin across a queue's consumers; ACK
//     needs the queue name because tags are per queue, and only the
//     connection that received a delivery may ack it.
//
// Known limitations (documented, not fixed yet):
//   - Two publishers can deliver to the same consumer slightly out of tag
//     order: pushes are collected in order under the lock but sent outside
//     it, so the sends can interleave. Tags stay unique and correct.
//   - A consumer that stops reading eventually fills its socket buffer and
//     blocks the publisher thread in send() (prefetch limit / send timeout
//     would fix this).
//   - Messages held by a connection that disconnects stay unacked forever
//     (requeue is milestone 3.3).
class CommandHandler {
public:
    using SinkPtr = std::shared_ptr<Sink>;

    explicit CommandHandler(Broker& broker) : m_broker(broker) {}

    // Parse + execute on behalf of `from`. Never throws.
    Result handleLine(const std::string& line, const SinkPtr& from);

    // Execute an already-parsed command. Never throws.
    Result handle(const Command& cmd, const SinkPtr& from);

    // The connection is gone: stop delivering to it.
    void onDisconnect(const SinkPtr& from);

private:
    // One overload per command type. handle() picks the right one via
    // std::visit; adding a Command alternative without an overload here
    // is a compile error.
    Result execute(const DeclareQueueCmd& cmd, const SinkPtr& from);
    Result execute(const DeclareExchangeCmd& cmd, const SinkPtr& from);
    Result execute(const BindCmd& cmd, const SinkPtr& from);
    Result execute(const PublishCmd& cmd, const SinkPtr& from);
    Result execute(const ConsumeCmd& cmd, const SinkPtr& from);
    Result execute(const AckCmd& cmd, const SinkPtr& from);

    // Hands every ready message of every consumed queue to a consumer,
    // appending a DELIVER push for each. Caller must hold m_mutex.
    void dispatch(std::vector<Push>& pushes);

    Broker& m_broker;
    ConsumerRegistry m_registry;
    std::mutex m_mutex;           // guards m_broker, m_registry and m_next_id
    uint64_t m_next_id = 1;
};
