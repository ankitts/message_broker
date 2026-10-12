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

std::string ok() {
    return dumpReply({{"type", "OK"}});
}

std::string error(const std::string& reason) {
    return dumpReply({{"type", "ERROR"}, {"reason", reason}});
}

}  // namespace

std::string CommandHandler::handleLine(const std::string& line) {
    try {
        // Parsing touches no shared state, so it runs outside the lock.
        ParseResult parsed = Protocol::parse(line);
        if (!parsed) return error(parsed.error);
        return handle(*parsed.command);
    } catch (...) {
        Logger::log("CommandHandler: unexpected exception in handleLine");
        return error("internal error");
    }
}

std::string CommandHandler::handle(const Command& cmd) {
    try {
        // visit calls execute() with whichever command the variant holds.
        return std::visit([this](const auto& c) { return execute(c); }, cmd);
    } catch (...) {
        Logger::log("CommandHandler: unexpected exception in handle");
        return error("internal error");
    }
}

std::string CommandHandler::execute(const DeclareQueueCmd& cmd) {
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_broker.declareQueue(cmd.queue);   // idempotent: always succeeds
    }
    return ok();
}

std::string CommandHandler::execute(const DeclareExchangeCmd& cmd) {
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

std::string CommandHandler::execute(const BindCmd& cmd) {
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

std::string CommandHandler::execute(const PublishCmd& cmd) {
    uint64_t id;
    std::optional<size_t> routed;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        // Assigned under the lock so id order == arrival order in the queues.
        id = m_next_id++;
        routed = m_broker.publish(cmd.exchange, Message(id, cmd.routing_key, cmd.body));
    }
    if (!routed) {
        return error("exchange '" + cmd.exchange + "' is not declared");
    }
    // routed == 0 is a valid publish that matched no queue, not an error.
    return dumpReply({{"type", "OK"}, {"routed", *routed}, {"id", id}});
}
