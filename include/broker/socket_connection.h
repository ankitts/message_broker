#pragma once

#include <atomic>
#include <mutex>
#include <string>

#include "broker/sink.h"

// A connected client socket. Owns the fd: the destructor closes it, and
// nothing else does. Held through std::shared_ptr so the fd stays open (and
// its number can't be reused by a new client) until the last thread that may
// still send to it has let go.
//
// send_mutex_ is a leaf lock: nothing else is acquired while holding it.
class SocketConnection : public Sink {
public:
    explicit SocketConnection(int fd) : fd_(fd) {}
    ~SocketConnection() override;

    SocketConnection(const SocketConnection&) = delete;
    SocketConnection& operator=(const SocketConnection&) = delete;

    // Holds send_mutex_ for the whole line. After the first failure the
    // connection is marked closed and later sends fail fast.
    bool send(const std::string& line) override;

    bool closed() const { return closed_.load(); }
    int fd() const { return fd_; }

private:
    const int fd_;
    std::mutex send_mutex_;
    std::atomic<bool> closed_{false};
};
