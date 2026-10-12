#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <variant>

#include "broker/exchange.h"

// Upper bound (in bytes) for exchange and queue names.
constexpr size_t kMaxNameLength = 255;

struct DeclareQueueCmd{
    std::string queue;
};

struct DeclareExchangeCmd{
    std::string exchange;
    ExchangeType type;
};

struct BindCmd {
    std::string exchange;
    std::string queue;
    std::string routing_key;
};

struct PublishCmd {
    std::string exchange;
    std::string routing_key;
    std::string body;
};

// One parsed client frame; std::visit on it to dispatch.
using Command = std::variant<DeclareQueueCmd, DeclareExchangeCmd, BindCmd, PublishCmd>;

// Outcome of parsing a frame: either a Command or a human-readable error.
struct ParseResult {
    std::optional<Command> command;
    std::string error;                       // non-empty iff !command
    explicit operator bool() const { return command.has_value(); }
};

// Translates newline-delimited JSON frames into typed Commands.
class Protocol {
public:
    // Parses one frame (no trailing newline). Never throws; malformed or
    // invalid input is reported through ParseResult::error.
    static ParseResult parse(const std::string& line);
};