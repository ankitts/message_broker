#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "broker/broker.h"
#include "broker/command_handler.h"
#include "broker/logger.h"
#include "broker/tcp_server.h"

using json = nlohmann::json;

// A plain blocking client socket with line-oriented reads.
class Client {
public:
    explicit Client(uint16_t port) {
        fd_ = socket(AF_INET, SOCK_STREAM, 0);
        assert(fd_ >= 0);
        timeval tv{5, 0};   // a hung server fails the test instead of hanging it
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        int rc = connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        assert(rc == 0);
        (void)rc;
    }
    ~Client() { close(fd_); }

    void send(const std::string& data) {
        size_t sent = 0;
        while (sent < data.size()) {
            ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, 0);
            assert(n > 0);
            sent += static_cast<size_t>(n);
        }
    }

    // One reply line without the '\n', or nullopt on EOF / timeout.
    std::optional<std::string> readLine() {
        while (true) {
            size_t pos = buf_.find('\n');
            if (pos != std::string::npos) {
                std::string line = buf_.substr(0, pos);
                buf_.erase(0, pos + 1);
                return line;
            }
            char chunk[1024];
            ssize_t n = read(fd_, chunk, sizeof(chunk));
            if (n <= 0) return std::nullopt;
            buf_.append(chunk, static_cast<size_t>(n));
        }
    }

    // Sends one command line and returns the parsed reply.
    json call(const json& command) {
        send(command.dump() + "\n");
        return expectReply();
    }

    json expectReply() {
        auto line = readLine();
        assert(line);
        json j = json::parse(*line, nullptr, false);
        assert(!j.is_discarded());
        return j;
    }

private:
    int fd_;
    std::string buf_;
};

static json declQ(const std::string& q) { return {{"cmd", "DECLARE_QUEUE"}, {"queue", q}}; }
static json declEx(const std::string& e) {
    return {{"cmd", "DECLARE_EXCHANGE"}, {"exchange", e}, {"type", "direct"}};
}
static json bindCmd(const std::string& e, const std::string& q, const std::string& k) {
    return {{"cmd", "BIND"}, {"exchange", e}, {"queue", q}, {"routing_key", k}};
}
static json pub(const std::string& e, const std::string& k, const std::string& body = "x") {
    return {{"cmd", "PUBLISH"}, {"exchange", e}, {"routing_key", k}, {"body", body}};
}

static json consumeCmd(const std::string& q) { return {{"cmd", "CONSUME"}, {"queue", q}}; }
static json ackCmd(const std::string& q, uint64_t tag) {
    return {{"cmd", "ACK"}, {"queue", q}, {"tag", tag}};
}

// Peeks at broker state. The broker has no lock of its own, so only call
// this when no command is in flight on the queue being inspected.
static Broker* g_broker = nullptr;
static size_t broker_unacked(const std::string& q) { return g_broker->getQueue(q)->unackedCount(); }
static size_t broker_ready(const std::string& q) { return g_broker->getQueue(q)->readyCount(); }

int main() {
    // Static so they outlive the detached server thread, which blocks in
    // accept() forever; the process just exits when main returns.
    static Broker broker;
    g_broker = &broker;
    static CommandHandler handler(broker);
    static TcpServer server(handler, 0);   // port 0: the OS picks a free one
    bool started = server.start();
    assert(started);
    (void)started;
    std::thread([] { server.run(); }).detach();
    const uint16_t port = server.port();

    {   // 1. the happy path over a real socket
        Client c(port);
        assert(c.call(declEx("ex1"))["type"] == "OK");
        assert(c.call(declQ("q1"))["type"] == "OK");
        assert(c.call(bindCmd("ex1", "q1", "k"))["type"] == "OK");
        json r = c.call(pub("ex1", "k"));
        assert(r["type"] == "OK" && r["routed"] == 1 && r.contains("id"));
    }
    {   // 2. two commands in one send() -> two replies, in order
        Client c(port);
        c.send(declQ("q2").dump() + "\n" + bindCmd("nope", "q2", "k").dump() + "\n");
        assert(c.expectReply()["type"] == "OK");
        assert(c.expectReply()["type"] == "ERROR");
    }
    {   // 3. one command split across two send() calls -> one reply
        Client c(port);
        std::string line = declQ("q3").dump() + "\n";
        c.send(line.substr(0, 10));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        c.send(line.substr(10));
        assert(c.expectReply()["type"] == "OK");
    }
    {   // 4. garbage, then a valid command on the same connection
        Client c(port);
        c.send("this is not json\n");
        json r = c.expectReply();
        assert(r["type"] == "ERROR" && r["reason"] == "invalid JSON");
        assert(c.call(declQ("q4"))["type"] == "OK");
    }
    {   // 5. an oversize line -> one ERROR, then the server closes the connection
        Client c(port);
        // Exactly one byte over the 1 MiB FrameBuffer limit, with no newline.
        // (Sending much more could leave unread data behind and make the
        // close reset the connection before the client reads the ERROR.)
        c.send(std::string((1 << 20) + 1, 'a'));
        json r = c.expectReply();
        assert(r["type"] == "ERROR" && r["reason"] == "line too long");
        assert(!c.readLine());              // EOF: the server hung up

        // ...and the server keeps serving other clients.
        Client other(port);
        assert(other.call(declQ("q5"))["type"] == "OK");
    }
    {   // 6. two clients publish concurrently; every reply is correct
        constexpr int kClients = 2, kMsgs = 300;
        {
            Client setup(port);
            assert(setup.call(declEx("ex6"))["type"] == "OK");
        }
        std::vector<std::set<uint64_t>> ids(kClients);
        std::vector<std::thread> threads;
        for (int t = 0; t < kClients; ++t) {
            threads.emplace_back([&, t] {
                Client c(port);
                std::string q = "q6_" + std::to_string(t);
                std::string key = "k" + std::to_string(t);
                assert(c.call(declQ(q))["type"] == "OK");
                assert(c.call(bindCmd("ex6", q, key))["type"] == "OK");
                for (int i = 0; i < kMsgs; ++i) {
                    json r = c.call(pub("ex6", key));
                    assert(r["type"] == "OK" && r["routed"] == 1);
                    ids[t].insert(r["id"].get<uint64_t>());
                }
            });
        }
        for (auto& th : threads) th.join();

        for (int t = 0; t < kClients; ++t) {
            assert(ids[t].size() == static_cast<size_t>(kMsgs));
            assert(broker.getQueue("q6_" + std::to_string(t))->readyCount() ==
                   static_cast<size_t>(kMsgs));
        }
        // Ids are unique across the two clients too.
        std::set<uint64_t> all(ids[0]);
        all.insert(ids[1].begin(), ids[1].end());
        assert(all.size() == static_cast<size_t>(kClients * kMsgs));
    }

    {   // 7. one socket consumes while another publishes; the DELIVER arrives
        Client setup(port);
        assert(setup.call(declEx("ex7"))["type"] == "OK");
        assert(setup.call(declQ("q7"))["type"] == "OK");
        assert(setup.call(bindCmd("ex7", "q7", "k"))["type"] == "OK");

        Client consumer(port);
        assert(consumer.call(consumeCmd("q7"))["type"] == "OK");
        json r = setup.call(pub("ex7", "k", "hello"));
        assert(r["type"] == "OK" && r["routed"] == 1);

        json d = consumer.expectReply();
        assert(d["type"] == "DELIVER" && d["queue"] == "q7" && d["body"] == "hello");
        assert(d["routing_key"] == "k" && d["id"] == r["id"] && d["tag"] == 1);
        assert(consumer.call(ackCmd("q7", d["tag"].get<uint64_t>()))["type"] == "OK");
        assert(broker_unacked("q7") == 0);
    }
    {   // 8. one client consumes and publishes on the same socket; backlog comes after OK
        Client c(port);
        assert(c.call(declEx("ex8"))["type"] == "OK");
        assert(c.call(declQ("q8"))["type"] == "OK");
        assert(c.call(bindCmd("ex8", "q8", "k"))["type"] == "OK");
        assert(c.call(pub("ex8", "k", "backlog"))["type"] == "OK");

        c.send(consumeCmd("q8").dump() + "\n");
        assert(c.expectReply()["type"] == "OK");            // OK first...
        assert(c.expectReply()["body"] == "backlog");       // ...then the DELIVER

        // Publishing to itself: reply goes first, then the DELIVER.
        c.send(pub("ex8", "k", "self").dump() + "\n");
        json reply = c.expectReply();
        assert(reply["type"] == "OK" && reply["routed"] == 1);
        json d = c.expectReply();
        assert(d["type"] == "DELIVER" && d["body"] == "self");
    }
    {   // 9. four threads hammer one consumer socket: every line is valid JSON
        constexpr int kPublishers = 4, kMsgs = 200;
        Client consumer(port), setup(port);
        assert(setup.call(declEx("ex9"))["type"] == "OK");
        assert(setup.call(declQ("q9"))["type"] == "OK");
        assert(setup.call(bindCmd("ex9", "q9", "k"))["type"] == "OK");
        assert(consumer.call(consumeCmd("q9"))["type"] == "OK");

        std::vector<std::thread> threads;
        for (int t = 0; t < kPublishers; ++t) {
            threads.emplace_back([&] {
                Client c(port);
                for (int i = 0; i < kMsgs; ++i) {
                    // a long-ish body makes torn writes more likely to show
                    assert(c.call(pub("ex9", "k", std::string(2000, 'z')))["type"] == "OK");
                }
            });
        }
        std::set<uint64_t> ids;
        for (int i = 0; i < kPublishers * kMsgs; ++i) {
            json d = consumer.expectReply();             // asserts it parses
            assert(d["type"] == "DELIVER" && d["body"].get<std::string>().size() == 2000);
            ids.insert(d["id"].get<uint64_t>());
        }
        for (auto& th : threads) th.join();
        assert(ids.size() == static_cast<size_t>(kPublishers * kMsgs));
    }
    {   // 10. a consumer vanishes mid-stream: publishers keep working, server survives
        Client setup(port);
        assert(setup.call(declEx("ex10"))["type"] == "OK");
        assert(setup.call(declQ("q10"))["type"] == "OK");
        assert(setup.call(bindCmd("ex10", "q10", "k"))["type"] == "OK");
        {
            Client consumer(port);
            assert(consumer.call(consumeCmd("q10"))["type"] == "OK");
            for (int i = 0; i < 20; ++i) assert(setup.call(pub("ex10", "k"))["type"] == "OK");
            // consumer destroyed here without reading or acking
        }
        for (int i = 0; i < 200; ++i) assert(setup.call(pub("ex10", "k"))["type"] == "OK");
        // Once the server noticed the disconnect, nothing is delivered and the
        // rest stays ready. Wait for the registry to drop the dead consumer.
        for (int i = 0; i < 100 && broker_ready("q10") == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        assert(broker_ready("q10") > 0);
        Client again(port);
        assert(again.call(declQ("still_alive"))["type"] == "OK");
    }

    // The statics above are destroyed when main returns; first let every
    // detached client thread finish its disconnect handling.
    for (int i = 0; i < 200 && server.activeConnections() > 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(server.activeConnections() == 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));   // trailing log line

    Logger::log("all server tests passed");   // not cout: server threads log concurrently
    // The accept thread blocks forever and wakes with an error as soon as the
    // static server closes its fd during exit, then touches destroyed statics.
    // There is no server shutdown API yet, so skip static destruction.
    std::_Exit(0);
}
