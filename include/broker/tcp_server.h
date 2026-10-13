#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include <netinet/in.h>

#include "broker/command_handler.h"

// Blocking TCP server speaking the broker protocol: one detached thread per
// client connection. Each connection reads newline-delimited JSON commands
// and answers every one with one JSON line. A connection that consumes a
// queue also receives DELIVER lines sent by other clients' threads.
//
// Lifetime notes:
//   - client threads are detached and hold a pointer to the server, so the
//     TcpServer object must outlive all of them (in main() it lives for the
//     whole program).
//   - the CommandHandler is borrowed, not owned, and must outlive the server.
class TcpServer {
public:
    // port 0 asks the OS for a free port; read it back with port() after start().
    explicit TcpServer(CommandHandler& cmd_handler, uint16_t port, int backlog = 10);
    ~TcpServer();

    // Non-copyable: the object owns a listening socket fd.
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Create, configure, bind and listen. Returns false (after logging) on failure.
    bool start();

    // Accept loop. Blocks forever; spawns one thread per connection.
    void run();

    // The port actually bound (differs from the constructor argument for port 0).
    uint16_t port() const { return port_; }

    int activeConnections() const { return active_connections_.load(); }

private:
    void handleClient(int client_fd, std::string peer);

    static std::string formatPeer(const sockaddr_in& addr);

    uint16_t port_;
    int backlog_;
    int server_fd_ = -1;
    std::atomic<int> active_connections_{0};

    CommandHandler& m_cmd_handler;   // borrowed, not owned
};