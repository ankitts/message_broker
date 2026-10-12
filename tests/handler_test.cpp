#include <algorithm>
#include <cassert>
#include <iostream>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "broker/broker.h"
#include "broker/command_handler.h"

using json = nlohmann::json;

// Runs one line through the handler and parses the reply back. Asserts the
// reply is single-line valid JSON with a type of OK or ERROR (cases 12, 14),
// so every test below gets those checks for free.
static json reply(CommandHandler& h, const std::string& line) {
    std::string raw = h.handleLine(line);
    assert(raw.find('\n') == std::string::npos);
    json j = json::parse(raw, nullptr, /*allow_exceptions=*/false);
    assert(!j.is_discarded() && j.is_object());
    assert(j["type"] == "OK" || j["type"] == "ERROR");
    if (j["type"] == "ERROR") {
        assert(j["reason"].is_string() && !j["reason"].get<std::string>().empty());
    }
    return j;
}

static bool isOk(const json& j) { return j["type"] == "OK"; }

static bool reasonHas(const json& j, const std::string& word) {
    return j["reason"].get<std::string>().find(word) != std::string::npos;
}

static std::string declQ(const std::string& q) {
    return R"({"cmd":"DECLARE_QUEUE","queue":")" + q + R"("})";
}
static std::string declEx(const std::string& e, const std::string& type) {
    return R"({"cmd":"DECLARE_EXCHANGE","exchange":")" + e + R"(","type":")" + type + R"("})";
}
static std::string bindCmd(const std::string& e, const std::string& q, const std::string& k) {
    return R"({"cmd":"BIND","exchange":")" + e + R"(","queue":")" + q +
           R"(","routing_key":")" + k + R"("})";
}
static std::string pub(const std::string& e, const std::string& k, const std::string& body) {
    return R"({"cmd":"PUBLISH","exchange":")" + e + R"(","routing_key":")" + k +
           R"(","body":")" + body + R"("})";
}

int main() {
    {   // 1. happy path
        Broker b; CommandHandler h(b);
        assert(isOk(reply(h, declQ("q"))));
        assert(isOk(reply(h, declEx("ex", "direct"))));
        assert(isOk(reply(h, bindCmd("ex", "q", "k"))));
        json r = reply(h, pub("ex", "k", "hello"));
        assert(isOk(r) && r["routed"] == 1);
    }
    {   // 2. the message really lands in the queue
        Broker b; CommandHandler h(b);
        reply(h, declQ("q")); reply(h, declEx("ex", "direct")); reply(h, bindCmd("ex", "q", "k"));
        reply(h, pub("ex", "k", "hello"));
        auto d = b.getQueue("q")->deliver_next();
        assert(d && d->message.m_routing_key == "k" && d->message.m_body == "hello");
    }
    {   // 3. unknown exchange is an error; no matching binding is OK with routed:0
        Broker b; CommandHandler h(b);
        json r = reply(h, pub("nope", "k", "x"));
        assert(!isOk(r) && reasonHas(r, "not declared"));
        reply(h, declEx("ex", "direct"));
        r = reply(h, pub("ex", "k", "x"));
        assert(isOk(r) && r["routed"] == 0);
    }
    {   // 4. fanout to three queues
        Broker b; CommandHandler h(b);
        reply(h, declEx("fan", "fanout"));
        for (std::string q : {"a", "b", "c"}) {
            reply(h, declQ(q)); reply(h, bindCmd("fan", q, ""));
        }
        json r = reply(h, pub("fan", "", "x"));
        assert(r["routed"] == 3);
        for (const char* q : {"a", "b", "c"}) assert(b.getQueue(q)->readyCount() == 1);
    }
    {   // 5. type conflict: error, original exchange still routes
        Broker b; CommandHandler h(b);
        reply(h, declEx("ex", "direct")); reply(h, declQ("q")); reply(h, bindCmd("ex", "q", "k"));
        json r = reply(h, declEx("ex", "fanout"));
        assert(!isOk(r) && reasonHas(r, "exchange"));
        assert(reply(h, pub("ex", "k", "x"))["routed"] == 1);
        assert(reply(h, pub("ex", "other", "x"))["routed"] == 0);   // still direct
    }
    {   // 6. bind to unknown queue / unknown exchange creates nothing
        Broker b; CommandHandler h(b);
        reply(h, declEx("ex", "direct"));
        json r = reply(h, bindCmd("ex", "missing", "k"));
        assert(!isOk(r) && reasonHas(r, "bind"));
        assert(b.getQueue("missing") == nullptr);
        reply(h, declQ("q"));
        r = reply(h, bindCmd("missing", "q", "k"));
        assert(!isOk(r) && reasonHas(r, "bind"));
        assert(reply(h, pub("missing", "k", "x"))["type"] == "ERROR");
    }
    {   // 7. duplicate bind is an error
        Broker b; CommandHandler h(b);
        reply(h, declQ("q")); reply(h, declEx("ex", "direct"));
        assert(isOk(reply(h, bindCmd("ex", "q", "k"))));
        json r = reply(h, bindCmd("ex", "q", "k"));
        assert(!isOk(r) && reasonHas(r, "already bound"));
    }
    {   // 8. re-declaring a queue keeps its messages
        Broker b; CommandHandler h(b);
        reply(h, declQ("q")); reply(h, declEx("ex", "direct")); reply(h, bindCmd("ex", "q", "k"));
        reply(h, pub("ex", "k", "x"));
        assert(isOk(reply(h, declQ("q"))));
        assert(b.getQueue("q")->readyCount() == 1);
    }
    {   // 9. bad input gets the parser's reason; the next line still works
        Broker b; CommandHandler h(b);
        json r = reply(h, "{not json");
        assert(!isOk(r) && reasonHas(r, "invalid JSON"));
        r = reply(h, R"({"cmd":"NOPE"})");
        assert(!isOk(r) && reasonHas(r, "unknown cmd"));
        r = reply(h, R"({"cmd":"DECLARE_QUEUE"})");
        assert(!isOk(r) && reasonHas(r, "missing field"));
        assert(isOk(reply(h, declQ("q"))));
        assert(b.getQueue("q") != nullptr);
    }
    {   // 10. awkward names (quote, backslash, newline, unicode) keep replies valid JSON
        Broker b; CommandHandler h(b);
        for (std::string name : {std::string("a\"b"), std::string("a\\b"),
                                 std::string("a\nb"), std::string("h\xc3\xa9llo")}) {
            json ex = {{"cmd", "PUBLISH"}, {"exchange", name}, {"routing_key", "k"}, {"body", "x"}};
            json r = reply(h, ex.dump());
            assert(!isOk(r));
            assert(r["reason"].get<std::string>().find(name) != std::string::npos);
        }
    }
    {   // 11. ids are unique and increasing, in queue order
        Broker b; CommandHandler h(b);
        reply(h, declQ("q")); reply(h, declEx("ex", "direct")); reply(h, bindCmd("ex", "q", "k"));
        uint64_t last = 0;
        for (int i = 0; i < 5; ++i) {
            json r = reply(h, pub("ex", "k", "x"));
            uint64_t id = r["id"].get<uint64_t>();
            assert(id > last);
            last = id;
        }
        uint64_t prev = 0;
        while (auto d = b.getQueue("q")->deliver_next()) {
            assert(d->message.m_id > prev);
            prev = d->message.m_id;
        }
        assert(prev == last);
    }
    {   // 13. topic exchange is not supported yet
        Broker b; CommandHandler h(b);
        json r = reply(h, declEx("t", "topic"));
        assert(!isOk(r) && reasonHas(r, "exchange"));
    }
    {   // 15. a failed command leaves state alone, and handler survives odd input
        Broker b; CommandHandler h(b);
        reply(h, "");
        reply(h, "[]");
        reply(h, std::string(1000, 'x'));
        reply(h, R"({"cmd":"PUBLISH","exchange":"","routing_key":"k","body":"x"})");
        assert(isOk(reply(h, declQ("q"))));
    }
    {   // 16. concurrency: 8 threads, each with its own queue, 2000 publishes
        constexpr int kThreads = 8, kMsgs = 2000;
        Broker b; CommandHandler h(b);
        reply(h, declEx("ex", "direct"));
        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&h, t] {
                std::string q = "q" + std::to_string(t);
                std::string key = "k" + std::to_string(t);
                // reply() asserts on every response, from any thread.
                reply(h, declQ(q));
                reply(h, bindCmd("ex", q, key));
                for (int i = 0; i < kMsgs; ++i) {
                    json r = reply(h, pub("ex", key, "x"));
                    assert(r["routed"] == 1);
                }
            });
        }
        for (auto& th : threads) th.join();

        std::set<uint64_t> ids;
        for (int t = 0; t < kThreads; ++t) {
            Queue* q = b.getQueue("q" + std::to_string(t));
            assert(q->readyCount() == static_cast<size_t>(kMsgs));
            while (auto d = q->deliver_next()) ids.insert(d->message.m_id);
        }
        assert(ids.size() == static_cast<size_t>(kThreads * kMsgs));
    }

    std::cout << "all handler tests passed\n";
    return 0;
}
