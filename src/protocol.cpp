#include <nlohmann/json.hpp>
#include <utility>

#include "broker/protocol.h"

using json = nlohmann::json;

// Helpers are file-local so they can't collide with symbols in other files.
namespace {

// Wraps an error message in a failed ParseResult.
ParseResult fail(std::string reason) {
    return ParseResult{std::nullopt, std::move(reason)};
}

// Wraps a parsed command in a successful ParseResult.
ParseResult ok(Command cmd) {
    return ParseResult{std::move(cmd), ""};
}

// Reads obj[field] as a string into `out`. On failure, sets `error` and
// returns false (field missing or not a string).
bool requireString(const json& obj, const char* field, std::string& out, std::string& error) {
    auto it = obj.find(field);
    if (it == obj.end()) {
        error = std::string("missing field '") + field + "'";
        return false;
    }
    if (!it->is_string()) {
        error = std::string("field '") + field + "' must be a string";
        return false;
    }
    out = it->get<std::string>();
    return true;
}

// Like requireString, but the value must also be a valid entity name:
// non-empty and at most kMaxNameLength bytes.
bool requireName(const json& obj, const char* field, std::string& out, std::string& error) {
    if (!requireString(obj, field, out, error)) return false;
    if (out.empty()) {
        error = std::string("field '") + field + "' must not be empty";
        return false;
    }
    if (out.size() > kMaxNameLength) {
        error = std::string("field '") + field + "' is longer than " +
                std::to_string(kMaxNameLength) + " bytes";
        return false;
    }
    return true;
}

// Reads obj[field] as a non-negative integer. A string, negative number or
// float is rejected (nlohmann stores non-negative integers as unsigned, and
// anything beyond uint64 range as a float).
bool requireUnsigned(const json& obj, const char* field, uint64_t& out, std::string& error) {
    auto it = obj.find(field);
    if (it == obj.end()) {
        error = std::string("missing field '") + field + "'";
        return false;
    }
    if (!it->is_number_unsigned()) {
        error = std::string("field '") + field + "' must be a non-negative integer";
        return false;
    }
    out = it->get<uint64_t>();
    return true;
}

// Maps the wire text ("direct"/"fanout"/"topic") to an ExchangeType.
// Returns false for anything else.
bool parseExchangeType(const std::string& text, ExchangeType& out) {
    if (text == "direct") { out = ExchangeType::Direct; return true; }
    if (text == "fanout") { out = ExchangeType::Fanout; return true; }
    if (text == "topic")  { out = ExchangeType::Topic;  return true; }
    return false;
}

}  // namespace

ParseResult Protocol::parse(const std::string& line) {
    // With allow_exceptions=false, bad input yields a "discarded" value
    // instead of throwing.
    json j = json::parse(line, /*cb=*/nullptr, /*allow_exceptions=*/false);
    if (j.is_discarded()) return fail("invalid JSON");
    if (!j.is_object())   return fail("frame must be a JSON object");

    // Every frame names its command; the rest of the fields depend on it.
    std::string cmd, error;
    if (!requireString(j, "cmd", cmd, error)) return fail(error);

    if (cmd == "DECLARE_QUEUE") {
        DeclareQueueCmd c;
        if (!requireName(j, "queue", c.queue, error)) return fail(error);
        return ok(std::move(c));
    }

    if (cmd == "DECLARE_EXCHANGE") {
        DeclareExchangeCmd c{};
        std::string type_text;
        if (!requireName(j, "exchange", c.exchange, error)) return fail(error);
        if (!requireString(j, "type", type_text, error))    return fail(error);
        if (!parseExchangeType(type_text, c.type)) {
            return fail("unknown exchange type '" + type_text + "'");
        }
        return ok(std::move(c));
    }

    if (cmd == "BIND") {
        BindCmd c;
        if (!requireName(j, "exchange", c.exchange, error))       return fail(error);
        if (!requireName(j, "queue", c.queue, error))             return fail(error);
        if (!requireString(j, "routing_key", c.routing_key, error)) return fail(error);
        return ok(std::move(c));
    }

    // routing_key and body may be empty; only the exchange name is validated.
    if (cmd == "PUBLISH") {
        PublishCmd c;
        if (!requireName(j, "exchange", c.exchange, error))         return fail(error);
        if (!requireString(j, "routing_key", c.routing_key, error)) return fail(error);
        if (!requireString(j, "body", c.body, error))               return fail(error);
        return ok(std::move(c));
    }

    if (cmd == "CONSUME") {
        ConsumeCmd c;
        if (!requireName(j, "queue", c.queue, error)) return fail(error);
        return ok(std::move(c));
    }

    if (cmd == "ACK") {
        AckCmd c{};
        if (!requireName(j, "queue", c.queue, error)) return fail(error);
        if (!requireUnsigned(j, "tag", c.tag, error)) return fail(error);
        return ok(std::move(c));
    }

    return fail("unknown cmd '" + cmd + "'");
}