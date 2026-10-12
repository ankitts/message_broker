#pragma once

#include <cstddef>
#include <optional>
#include <string>

// Turns a TCP byte stream into complete newline-terminated frames.
// Knows nothing about sockets or JSON, so it can be unit tested directly.
//
// Empty lines are skipped (a bare "\n" or "\r\n" never produces a frame).
class FrameBuffer {
public:
    explicit FrameBuffer(size_t max_frame = 1 << 20);

    // Append bytes from a read(). Returns false if the unterminated data now
    // exceeds max_frame (client is misbehaving: caller should reply and close).
    bool append(const char* data, size_t len);

    // Pop one complete frame (no '\n', no trailing '\r'), or nullopt if no
    // complete frame is buffered yet. Call in a loop until it returns nullopt.
    std::optional<std::string> nextFrame();

private:
    std::string buf_;
    size_t max_frame_;
};