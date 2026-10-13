#pragma once

#include <string>

// Anything we can send one line to. Production uses SocketConnection; tests
// use a fake that records lines, so handler logic needs no sockets.
class Sink {
public:
    virtual ~Sink() = default;

    // Sends `line` followed by '\n'. Returns false if the peer is gone.
    // Thread-safe: concurrent calls never interleave partial lines.
    virtual bool send(const std::string& line) = 0;
};
