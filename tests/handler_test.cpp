#include <algorithm>
#include <atomic>
#include <cassert>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "broker/broker.h"
#include "broker/command_handler.h"
#include "fake_sink.h"

using json = nlohmann::json;

// Runs one line through the handler and parses the reply back. Asserts the
// reply is single-line valid JSON with a type of OK or ERROR (cases 12, 14),
// so every test below gets those checks for free.
static json reply(CommandHandler& h, const std::string& line) {
    auto sink = std::make_shared<FakeSink>();
    std::string raw = h.handleLine(line, sink).reply;
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

// --- 3.2: consumers -------------------------------------------------------

using SinkP = std::shared_ptr<FakeSink>;

static json parseLine(const std::string& raw) {
    assert(raw.find('\n') == std::string::npos);
    json j = json::parse(raw, nullptr, false);
    assert(!j.is_discarded() && j.is_object());
    return j;
}

// Sends a command as `who`; checks the reply is OK/ERROR and returns the Result.
static Result call(CommandHandler& h, const SinkP& who, const std::string& line) {
    Result r = h.handleLine(line, who);
    json j = parseLine(r.reply);
    assert(j["type"] == "OK" || j["type"] == "ERROR");
    return r;
}

static json replyOf(const Result& r) { return parseLine(r.reply); }

static std::string consume(const std::string& q) {
    return R"({"cmd":"CONSUME","queue":")" + q + R"("})";
}
static std::string ack(const std::string& q, uint64_t tag) {
    return R"({"cmd":"ACK","queue":")" + q + R"(","tag":)" + std::to_string(tag) + "}";
}

// queue q bound to direct exchange ex with key k
static void setup(CommandHandler& h, const SinkP& admin, const std::string& q = "q") {
    call(h, admin, declQ(q));
    call(h, admin, declEx("ex", "direct"));
    call(h, admin, bindCmd("ex", q, "k"));
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

    {   // 17. CONSUME on an unknown queue is an error and registers nothing
        Broker b; CommandHandler h(b);
        auto c = std::make_shared<FakeSink>();
        json r = replyOf(call(h, c, consume("nope")));
        assert(!isOk(r) && reasonHas(r, "queue 'nope' is not declared"));
        setup(h, c, "other");
        Result p = call(h, c, pub("ex", "k", "x"));
        assert(p.pushes.empty());
    }
    {   // 18. CONSUME twice on the same queue is an error; two queues are fine
        Broker b; CommandHandler h(b);
        auto c = std::make_shared<FakeSink>();
        setup(h, c, "q"); call(h, c, declQ("q2"));
        assert(isOk(replyOf(call(h, c, consume("q")))));
        json r = replyOf(call(h, c, consume("q")));
        assert(!isOk(r) && reasonHas(r, "already consuming 'q'"));
        assert(isOk(replyOf(call(h, c, consume("q2")))));
    }
    {   // 19. backlog is pushed after the OK
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>(), c = std::make_shared<FakeSink>();
        setup(h, admin);
        call(h, admin, pub("ex", "k", "one"));
        call(h, admin, pub("ex", "k", "two"));
        Result r = call(h, c, consume("q"));
        assert(isOk(replyOf(r)));
        assert(r.pushes.size() == 2);
        for (const Push& p : r.pushes) assert(p.to == c);
        json d0 = parseLine(r.pushes[0].line), d1 = parseLine(r.pushes[1].line);
        assert(d0["type"] == "DELIVER" && d0["queue"] == "q" && d0["body"] == "one");
        assert(d0["routing_key"] == "k" && d0["tag"] == 1 && d0["id"].is_number_unsigned());
        assert(d1["body"] == "two" && d1["tag"] == 2);
        assert(b.getQueue("q")->readyCount() == 0 && b.getQueue("q")->unackedCount() == 2);
    }
    {   // 20. publish pushes to the consumer, and the publisher still gets its reply
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>(), c = std::make_shared<FakeSink>();
        setup(h, admin);
        assert(call(h, c, consume("q")).pushes.empty());
        Result r = call(h, admin, pub("ex", "k", "hello"));
        json rep = replyOf(r);
        assert(isOk(rep) && rep["routed"] == 1);
        assert(r.pushes.size() == 1 && r.pushes[0].to == c);
        json d = parseLine(r.pushes[0].line);
        assert(d["body"] == "hello" && d["id"] == rep["id"]);
    }
    {   // 21. two consumers get alternating messages
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>();
        auto c1 = std::make_shared<FakeSink>(), c2 = std::make_shared<FakeSink>();
        setup(h, admin);
        call(h, c1, consume("q")); call(h, c2, consume("q"));
        std::vector<std::shared_ptr<Sink>> got;
        for (int i = 0; i < 4; ++i) {
            Result r = call(h, admin, pub("ex", "k", "m" + std::to_string(i)));
            assert(r.pushes.size() == 1);
            got.push_back(r.pushes[0].to);
        }
        assert(got[0] == c1 && got[1] == c2 && got[2] == c1 && got[3] == c2);
    }
    {   // 22. ACK works, a second ACK errors, unackedCount drops
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>(), c = std::make_shared<FakeSink>();
        setup(h, admin);
        call(h, c, consume("q"));
        call(h, admin, pub("ex", "k", "x"));
        Queue* q = b.getQueue("q");
        assert(q->unackedCount() == 1);
        assert(isOk(replyOf(call(h, c, ack("q", 1)))));
        assert(q->unackedCount() == 0);
        json r = replyOf(call(h, c, ack("q", 1)));
        assert(!isOk(r) && reasonHas(r, "unknown delivery tag"));
    }
    {   // 23. ACK from a different connection errors and changes nothing
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>();
        auto c1 = std::make_shared<FakeSink>(), c2 = std::make_shared<FakeSink>();
        setup(h, admin);
        call(h, c1, consume("q")); call(h, c2, consume("q"));
        call(h, admin, pub("ex", "k", "x"));     // goes to c1, tag 1
        json r = replyOf(call(h, c2, ack("q", 1)));
        assert(!isOk(r) && reasonHas(r, "unknown delivery tag"));
        assert(b.getQueue("q")->unackedCount() == 1);
        assert(isOk(replyOf(call(h, c1, ack("q", 1)))));
    }
    {   // 24. ACK with the wrong queue name errors (tags are per queue)
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>(), c = std::make_shared<FakeSink>();
        setup(h, admin, "q"); call(h, admin, declQ("other"));
        call(h, c, consume("q"));
        call(h, admin, pub("ex", "k", "x"));
        json r = replyOf(call(h, c, ack("other", 1)));
        assert(!isOk(r) && reasonHas(r, "unknown delivery tag"));
        r = replyOf(call(h, c, ack("missing", 1)));
        assert(!isOk(r) && reasonHas(r, "not declared"));
        assert(b.getQueue("q")->unackedCount() == 1);
    }
    {   // 25. same tag number on two queues is unambiguous because ACK names the queue
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>(), c = std::make_shared<FakeSink>();
        call(h, admin, declEx("ex", "fanout"));
        for (std::string q : {"a", "b"}) { call(h, admin, declQ(q)); call(h, admin, bindCmd("ex", q, "")); }
        call(h, c, consume("a")); call(h, c, consume("b"));
        Result r = call(h, admin, pub("ex", "", "x"));
        assert(r.pushes.size() == 2);
        assert(isOk(replyOf(call(h, c, ack("a", 1)))));
        assert(b.getQueue("a")->unackedCount() == 0 && b.getQueue("b")->unackedCount() == 1);
        assert(isOk(replyOf(call(h, c, ack("b", 1)))));
    }
    {   // 26. after disconnect, publishes stay ready and nothing is pushed to the dead sink
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>(), c = std::make_shared<FakeSink>();
        setup(h, admin);
        call(h, c, consume("q"));
        h.onDisconnect(c);
        Result r = call(h, admin, pub("ex", "k", "x"));
        assert(isOk(replyOf(r)) && r.pushes.empty());
        assert(b.getQueue("q")->readyCount() == 1);
        h.onDisconnect(c);                      // harmless twice
        // a new consumer picks the backlog up
        auto c2 = std::make_shared<FakeSink>();
        assert(call(h, c2, consume("q")).pushes.size() == 1);
    }
    {   // 27. bodies with quotes and newlines can't break a DELIVER line
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>(), c = std::make_shared<FakeSink>();
        setup(h, admin);
        call(h, c, consume("q"));
        std::string body = "a\"b\nc\\d";
        json cmd = {{"cmd", "PUBLISH"}, {"exchange", "ex"}, {"routing_key", "k"}, {"body", body}};
        Result r = call(h, admin, cmd.dump());
        assert(r.pushes.size() == 1);
        assert(parseLine(r.pushes[0].line)["body"] == body);
    }
    {   // 28. concurrent publishers + consumers: every message delivered exactly once
        constexpr int kPublishers = 4, kConsumers = 3, kMsgs = 500;
        Broker b; CommandHandler h(b);
        auto admin = std::make_shared<FakeSink>();
        setup(h, admin);
        std::vector<SinkP> consumers;
        for (int i = 0; i < kConsumers; ++i) {
            consumers.push_back(std::make_shared<FakeSink>());
            call(h, consumers.back(), consume("q"));
        }
        std::vector<std::thread> threads;
        for (int t = 0; t < kPublishers; ++t) {
            threads.emplace_back([&] {
                auto me = std::make_shared<FakeSink>();
                for (int i = 0; i < kMsgs; ++i) {
                    Result r = h.handleLine(pub("ex", "k", "x"), me);
                    // What the transport does: send pushes outside the handler lock.
                    for (const Push& p : r.pushes) p.to->send(p.line);
                }
            });
        }
        // Consumers ack concurrently with the publishing.
        std::atomic<bool> done{false};
        std::atomic<int> acked{0};
        std::vector<std::thread> ackers;
        for (auto& c : consumers) {
            ackers.emplace_back([&, c] {
                size_t seen = 0;
                while (true) {
                    auto lines = c->snapshot();
                    for (; seen < lines.size(); ++seen) {
                        uint64_t tag = parseLine(lines[seen])["tag"].get<uint64_t>();
                        assert(isOk(parseLine(h.handleLine(ack("q", tag), c).reply)));
                        ++acked;
                    }
                    if (done && seen == c->snapshot().size()) return;
                    std::this_thread::yield();
                }
            });
        }
        for (auto& th : threads) th.join();
        done = true;
        for (auto& th : ackers) th.join();

        std::set<uint64_t> ids;
        size_t total = 0;
        for (auto& c : consumers) {
            for (auto& line : c->snapshot()) { ids.insert(parseLine(line)["id"].get<uint64_t>()); ++total; }
        }
        assert(total == static_cast<size_t>(kPublishers * kMsgs));
        assert(ids.size() == total);                 // no duplicates
        assert(acked == static_cast<int>(total));
        assert(b.getQueue("q")->readyCount() == 0 && b.getQueue("q")->unackedCount() == 0);
    }

    std::cout << "all handler tests passed\n";
    return 0;
}
