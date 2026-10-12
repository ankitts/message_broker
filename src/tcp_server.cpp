#include "broker/tcp_server.h"

#include <cerrno>
#include <cstring>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "broker/frame_buffer.h"
#include "broker/logger.h"

TcpServer::TcpServer(CommandHandler& cmd_handler, uint16_t port, int backlog)
    : port_(port), backlog_(backlog), m_cmd_handler(cmd_handler) {}

TcpServer::~TcpServer() {
    if (server_fd_ >= 0) {
        close(server_fd_);
    }
}

bool TcpServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        Logger::log(std::string("socket: ") + std::strerror(errno));
        return false;
    }

    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        Logger::log(std::string("setsockopt: ") + std::strerror(errno));
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    if (bind(server_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        Logger::log(std::string("bind: ") + std::strerror(errno));
        return false;
    }
    if (listen(server_fd_, backlog_) < 0) {
        Logger::log(std::string("listen: ") + std::strerror(errno));
        return false;
    }

    // Read the real port back (matters when the caller asked for port 0).
    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    if (getsockname(server_fd_, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0) {
        port_ = ntohs(bound.sin_port);
    }

    Logger::log("listening on :" + std::to_string(port_));
    return true;
}

void TcpServer::run() {
    while (true) {
        sockaddr_in client_address{};
        socklen_t client_len = sizeof(client_address);
        int client_fd = accept(server_fd_,
                               reinterpret_cast<sockaddr*>(&client_address),
                               &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            Logger::log(std::string("accept: ") + std::strerror(errno));
            continue;
        }

        std::string peer = formatPeer(client_address);
        std::thread(&TcpServer::handleClient, this, client_fd, std::move(peer)).detach();
    }
}

void TcpServer::handleClient(int client_fd, std::string peer) {
    int count = active_connections_.fetch_add(1) + 1;
    Logger::log("connected " + peer + " (" + std::to_string(count) + " active)");

    // Each connection has its own FrameBuffer: it turns the byte stream into
    // whole lines, however the bytes were split across reads.
    static const std::string kTooLong =
        "{\"type\":\"ERROR\",\"reason\":\"line too long\"}\n";

    FrameBuffer frames;
    char buffer[4096];
    bool open = true;
    while (open) {
        ssize_t n = read(client_fd, buffer, sizeof(buffer));
        if (n == 0) break;  // peer closed cleanly
        if (n < 0) {
            if (errno == EINTR) continue;
            Logger::log("read error from " + peer + ": " + std::strerror(errno));
            break;
        }

        bool within_limit = frames.append(buffer, static_cast<size_t>(n));

        // Answer every complete line, in order. No lock is held here: the
        // handler has already released its lock when it returns.
        while (auto frame = frames.nextFrame()) {
            std::string reply = m_cmd_handler.handleLine(*frame);
            reply += '\n';
            if (!sendAll(client_fd, reply.data(), reply.size())) {
                Logger::log("send error to " + peer + ": " + std::strerror(errno));
                open = false;
                break;
            }
        }

        // An unterminated line over the limit means the stream can't be
        // trusted any more: say so once, then hang up. (Complete lines before
        // it were already answered above.)
        if (open && !within_limit) {
            sendAll(client_fd, kTooLong.data(), kTooLong.size());
            Logger::log("line too long from " + peer + ", closing");
            open = false;
        }
    }

    close(client_fd);
    count = active_connections_.fetch_sub(1) - 1;
    Logger::log("disconnected " + peer + " (" + std::to_string(count) + " active)");
}

bool TcpServer::sendAll(int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

std::string TcpServer::formatPeer(const sockaddr_in& addr) {
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    return std::string(ip) + ":" + std::to_string(ntohs(addr.sin_port));
}