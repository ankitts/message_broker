#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
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

int main() {
    // Static so they outlive the detached server thread, which blocks in
    // accept() forever; the process just exits when main returns.
    static Broker broker;
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

    Logger::log("all server tests passed");   // not cout: server threads log concurrently
    return 0;
}
