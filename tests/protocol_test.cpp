#include <cassert>
#include <iostream>
#include <random>
#include <string>

#include "broker/protocol.h"

// Helper: parse and expect success, returning the concrete command type.
template <typename T>
static T expect(const std::string& line) {
    ParseResult r = Protocol::parse(line);
    assert(r);
    assert(r.error.empty());
    const T* c = std::get_if<T>(&*r.command);
    assert(c != nullptr);
    return *c;
}

// Helper: parse and expect failure whose reason contains `needle`.
static void expectError(const std::string& line, const std::string& needle) {
    ParseResult r = Protocol::parse(line);
    assert(!r);
    assert(!r.command.has_value());
    assert(!r.error.empty());
    if (r.error.find(needle) == std::string::npos) {
        std::cerr << "error was: " << r.error << "\nexpected to contain: " << needle
                  << "\nfor input: " << line.substr(0, 200) << "\n";
        assert(false);
    }
}

int main() {
    {   // 1. each command parses with the right fields
        auto q = expect<DeclareQueueCmd>(R"({"cmd":"DECLARE_QUEUE","queue":"orders"})");
        assert(q.queue == "orders");

        auto e = expect<DeclareExchangeCmd>(
            R"({"cmd":"DECLARE_EXCHANGE","exchange":"events","type":"fanout"})");
        assert(e.exchange == "events");
        assert(e.type == ExchangeType::Fanout);

        auto b = expect<BindCmd>(
            R"({"cmd":"BIND","exchange":"events","queue":"orders","routing_key":"order.created"})");
        assert(b.exchange == "events" && b.queue == "orders");
        assert(b.routing_key == "order.created");

        auto p = expect<PublishCmd>(
            R"({"cmd":"PUBLISH","exchange":"events","routing_key":"k","body":"hello"})");
        assert(p.exchange == "events" && p.routing_key == "k" && p.body == "hello");
    }
    {   // 2. all three exchange types map to the enum
        auto d = expect<DeclareExchangeCmd>(
            R"({"cmd":"DECLARE_EXCHANGE","exchange":"x","type":"direct"})");
        auto f = expect<DeclareExchangeCmd>(
            R"({"cmd":"DECLARE_EXCHANGE","exchange":"x","type":"fanout"})");
        auto t = expect<DeclareExchangeCmd>(
            R"({"cmd":"DECLARE_EXCHANGE","exchange":"x","type":"topic"})");
        assert(d.type == ExchangeType::Direct);
        assert(f.type == ExchangeType::Fanout);
        assert(t.type == ExchangeType::Topic);   // parses; the Broker rejects it for now
    }
    {   // 3. unknown exchange type (including wrong case)
        expectError(R"({"cmd":"DECLARE_EXCHANGE","exchange":"x","type":"banana"})",
                    "unknown exchange type");
        expectError(R"({"cmd":"DECLARE_EXCHANGE","exchange":"x","type":"Direct"})",
                    "unknown exchange type");
        expectError(R"({"cmd":"DECLARE_EXCHANGE","exchange":"x","type":""})",
                    "unknown exchange type");
    }
    {   // 4. invalid JSON
        expectError("", "invalid JSON");
        expectError("not json", "invalid JSON");
        expectError("{", "invalid JSON");
        expectError(R"({"cmd":"DECLARE_QUEUE","queue":"q")", "invalid JSON");   // unterminated
        expectError(R"({"cmd":"DECLARE_QUEUE",})", "invalid JSON");              // trailing comma
        expectError("{'cmd':'DECLARE_QUEUE'}", "invalid JSON");                  // single quotes
        expectError(std::string("\xff\xfe{}", 4), "invalid JSON");               // bad UTF-8
    }
    {   // 5. valid JSON that is not an object
        expectError("[]", "JSON object");
        expectError("5", "JSON object");
        expectError("\"DECLARE_QUEUE\"", "JSON object");
        expectError("null", "JSON object");
        expectError("true", "JSON object");
    }
    {   // 6. cmd missing / wrong type / unknown / wrong case
        expectError("{}", "missing field 'cmd'");
        expectError(R"({"cmd":5})", "'cmd' must be a string");
        expectError(R"({"cmd":null})", "'cmd' must be a string");
        expectError(R"({"cmd":"NOPE"})", "unknown cmd");
        expectError(R"({"cmd":"declare_queue","queue":"q"})", "unknown cmd");
        expectError(R"({"cmd":""})", "unknown cmd");
    }
    {   // 7. missing required fields, one per command
        expectError(R"({"cmd":"DECLARE_QUEUE"})", "missing field 'queue'");
        expectError(R"({"cmd":"DECLARE_EXCHANGE","exchange":"x"})", "missing field 'type'");
        expectError(R"({"cmd":"DECLARE_EXCHANGE","type":"direct"})", "missing field 'exchange'");
        expectError(R"({"cmd":"BIND","exchange":"e","queue":"q"})", "missing field 'routing_key'");
        expectError(R"({"cmd":"BIND","exchange":"e","routing_key":"k"})", "missing field 'queue'");
        expectError(R"({"cmd":"PUBLISH","exchange":"e","routing_key":"k"})", "missing field 'body'");
        expectError(R"({"cmd":"PUBLISH","routing_key":"k","body":"b"})", "missing field 'exchange'");
    }
    {   // 8. wrong field types
        expectError(R"({"cmd":"DECLARE_QUEUE","queue":5})", "'queue' must be a string");
        expectError(R"({"cmd":"DECLARE_QUEUE","queue":null})", "'queue' must be a string");
        expectError(R"({"cmd":"DECLARE_QUEUE","queue":["q"]})", "'queue' must be a string");
        expectError(R"({"cmd":"PUBLISH","exchange":"e","routing_key":"k","body":123})",
                    "'body' must be a string");
        expectError(R"({"cmd":"PUBLISH","exchange":"e","routing_key":1,"body":"b"})",
                    "'routing_key' must be a string");
        expectError(R"({"cmd":"DECLARE_EXCHANGE","exchange":"e","type":3})",
                    "'type' must be a string");
    }
    {   // 9. names: empty and too long are rejected; routing_key/body may be empty
        expectError(R"({"cmd":"DECLARE_QUEUE","queue":""})", "must not be empty");
        expectError(R"({"cmd":"DECLARE_EXCHANGE","exchange":"","type":"direct"})",
                    "must not be empty");
        expectError(R"({"cmd":"BIND","exchange":"","queue":"q","routing_key":"k"})",
                    "must not be empty");
        expectError(R"({"cmd":"PUBLISH","exchange":"","routing_key":"k","body":"b"})",
                    "must not be empty");

        std::string longest(kMaxNameLength, 'a');
        std::string too_long(kMaxNameLength + 1, 'a');
        auto ok_cmd = expect<DeclareQueueCmd>(
            R"({"cmd":"DECLARE_QUEUE","queue":")" + longest + R"("})");
        assert(ok_cmd.queue == longest);
        expectError(R"({"cmd":"DECLARE_QUEUE","queue":")" + too_long + R"("})",
                    "longer than");

        auto b = expect<BindCmd>(R"({"cmd":"BIND","exchange":"e","queue":"q","routing_key":""})");
        assert(b.routing_key.empty());
        auto p = expect<PublishCmd>(
            R"({"cmd":"PUBLISH","exchange":"e","routing_key":"","body":""})");
        assert(p.routing_key.empty() && p.body.empty());
    }
    {   // 10. extra fields are ignored; field order does not matter
        auto q = expect<DeclareQueueCmd>(
            R"({"extra":[1,2,3],"queue":"q","cmd":"DECLARE_QUEUE","x":{"y":null}})");
        assert(q.queue == "q");
    }
    {   // 11. strings with escapes, unicode, embedded newlines survive intact
        auto p = expect<PublishCmd>(
            R"({"cmd":"PUBLISH","exchange":"e","routing_key":"k","body":"line1\nline2\t\"q\" é 😀"})");
        assert(p.body == "line1\nline2\t\"q\" \xc3\xa9 \xf0\x9f\x98\x80");
    }
    {   // 12. large body (within the framing limit) parses
        std::string big(500000, 'x');
        auto p = expect<PublishCmd>(
            R"({"cmd":"PUBLISH","exchange":"e","routing_key":"k","body":")" + big + R"("})");
        assert(p.body.size() == big.size());
    }
    {   // 13. a duplicate key: the parser must not crash (last one wins in nlohmann)
        ParseResult r = Protocol::parse(R"({"cmd":"DECLARE_QUEUE","queue":"a","queue":"b"})");
        assert(r);
    }
    {   // 14. hostile input: very deep nesting must not crash or throw
        std::string deep(200000, '[');
        deep += std::string(200000, ']');
        ParseResult r = Protocol::parse(deep);
        assert(!r);
        std::string deep_obj = R"({"cmd":"DECLARE_QUEUE","queue":"q","x":)" +
                               std::string(100000, '[') + std::string(100000, ']') + "}";
        r = Protocol::parse(deep_obj);          // valid JSON; extra field ignored
        assert(r);
    }
    {   // 15. random garbage never throws and never yields a bogus command
        std::mt19937 rng(12345);
        for (int i = 0; i < 5000; ++i) {
            size_t len = rng() % 200;
            std::string s;
            for (size_t k = 0; k < len; ++k) s.push_back(static_cast<char>(rng() % 256));
            ParseResult r = Protocol::parse(s);
            assert(r.command.has_value() == r.error.empty());
        }
        // random mutations of a valid frame
        const std::string valid = R"({"cmd":"PUBLISH","exchange":"e","routing_key":"k","body":"b"})";
        for (int i = 0; i < 5000; ++i) {
            std::string s = valid;
            for (int m = 0; m < 3; ++m) s[rng() % s.size()] = static_cast<char>(rng() % 256);
            ParseResult r = Protocol::parse(s);
            assert(r.command.has_value() == r.error.empty());
        }
    }
    {   // 16. result invariant: success has no error, failure has no command
        ParseResult good = Protocol::parse(R"({"cmd":"DECLARE_QUEUE","queue":"q"})");
        assert(good && good.error.empty());
        ParseResult bad = Protocol::parse("nope");
        assert(!bad && !bad.error.empty() && !bad.command);
    }
    std::cout << "all protocol tests passed\n";
}