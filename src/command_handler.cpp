#include "broker/command_handler.h"

#include <mutex>
#include <nlohmann/json.hpp>
#include <utility>
#include <variant>

#include "broker/logger.h"

using json = nlohmann::json;

namespace {

// Replies are built with nlohmann::json, never by concatenation, so names
// containing quotes or newlines can't break the output. The "replace" error
// handler means invalid UTF-8 can't make dump() throw.
std::string dumpReply(const json& reply) {
    return reply.dump(-1, ' ', false, json::error_handler_t::replace);
}

std::string okLine() {
    return dumpReply({{"type", "OK"}});
}

Result ok() { return Result{okLine(), {}}; }

Result error(const std::string& reason) {
    return Result{dumpReply({{"type", "ERROR"}, {"reason", reason}}), {}};
}

std::string deliverLine(const std::string& queue, const Delivery& d) {
    return dumpReply({{"type", "DELIVER"},
                      {"queue", queue},
                      {"tag", d.tag},
                      {"id", d.message.m_id},
                      {"routing_key", d.message.m_routing_key},
                      {"body", d.message.m_body}});
}

}  // namespace

Result CommandHandler::handleLine(const std::string& line, const SinkPtr& from) {
    try {
        // Parsing touches no shared state, so it runs outside the lock.
        ParseResult parsed = Protocol::parse(line);
        if (!parsed) return error(parsed.error);
        return handle(*parsed.command, from);
    } catch (...) {
        Logger::log("CommandHandler: unexpected exception in handleLine");
        return error("internal error");
    }
}

Result CommandHandler::handle(const Command& cmd, const SinkPtr& from) {
    try {
        // visit calls execute() with whichever command the variant holds.
        return std::visit([&](const auto& c) { return execute(c, from); }, cmd);
    } catch (...) {
        Logger::log("CommandHandler: unexpected exception in handle");
        return error("internal error");
    }
}

void CommandHandler::onDisconnect(const SinkPtr& from) {
    try {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto held = m_registry.removeConnection(from);
        if (!held.empty()) {
            Logger::log("connection dropped holding " + std::to_string(held.size()) +
                        " unacked deliveries (not requeued until 3.3)");
        }
    } catch (...) {
        Logger::log("CommandHandler: unexpected exception in onDisconnect");
    }
}

void CommandHandler::dispatch(std::vector<Push>& pushes) {
    for (const std::string& name : m_registry.queuesWithConsumers()) {
        Queue* queue = m_broker.getQueue(name);
        if (!queue) continue;
        while (auto d = queue->deliver_next()) {
            SinkPtr sink = m_registry.pickConsumer(name);   // non-null: name has consumers
            m_registry.recordDelivery(sink, name, d->tag);
            pushes.push_back({sink, deliverLine(name, *d)});
        }
    }
}

Result CommandHandler::execute(const DeclareQueueCmd& cmd, const SinkPtr&) {
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_broker.declareQueue(cmd.queue);   // idempotent: always succeeds
    }
    return ok();
}

Result CommandHandler::execute(const DeclareExchangeCmd& cmd, const SinkPtr&) {
    bool declared;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        declared = m_broker.declareExchange(cmd.exchange, cmd.type);
    }
    if (!declared) {
        return error("cannot declare exchange '" + cmd.exchange +
                     "': type conflict or unsupported type");
    }
    return ok();
}

Result CommandHandler::execute(const BindCmd& cmd, const SinkPtr&) {
    bool bound;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        bound = m_broker.bind(cmd.exchange, cmd.queue, cmd.routing_key);
    }
    if (!bound) {
        return error("cannot bind: exchange or queue not declared, or already bound");
    }
    return ok();
}

Result CommandHandler::execute(const PublishCmd& cmd, const SinkPtr&) {
    uint64_t id;
    std::optional<size_t> routed;
    std::vector<Push> pushes;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        // Assigned under the lock so id order == arrival order in the queues.
        id = m_next_id++;
        routed = m_broker.publish(cmd.exchange, Message(id, cmd.routing_key, cmd.body));
        // Broker::publish only returns a count, not which queues got a copy,
        // so sweep every consumed queue.
        if (routed && *routed > 0) dispatch(pushes);
    }
    if (!routed) {
        return error("exchange '" + cmd.exchange + "' is not declared");
    }
    // routed == 0 is a valid publish that matched no queue, not an error.
    return Result{dumpReply({{"type", "OK"}, {"routed", *routed}, {"id", id}}),
                  std::move(pushes)};
}

Result CommandHandler::execute(const ConsumeCmd& cmd, const SinkPtr& from) {
    std::vector<Push> pushes;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_broker.getQueue(cmd.queue)) {
            return error("queue '" + cmd.queue + "' is not declared");
        }
        if (!m_registry.addConsumer(cmd.queue, from)) {
            return error("already consuming '" + cmd.queue + "'");
        }
        dispatch(pushes);   // drains the backlog that was already waiting
    }
    return Result{okLine(), std::move(pushes)};
}

Result CommandHandler::execute(const AckCmd& cmd, const SinkPtr& from) {
    std::lock_guard<std::mutex> lock(m_mutex);
    Queue* queue = m_broker.getQueue(cmd.queue);
    if (!queue) return error("queue '" + cmd.queue + "' is not declared");

    // Ownership check first: only the connection that received it may ack.
    if (!m_registry.confirmAck(from, cmd.queue, cmd.tag)) {
        return error("unknown delivery tag");
    }
    if (!queue->ack(cmd.tag)) {
        // Registry and queue disagree: a bug, not a client error.
        Logger::log("ACK: registry held tag " + std::to_string(cmd.tag) + " on '" +
                    cmd.queue + "' but the queue did not know it");
    }
    return ok();
}
