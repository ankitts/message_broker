#include "broker/frame_buffer.h"

FrameBuffer::FrameBuffer(size_t max_frame) : max_frame_(max_frame) {}

bool FrameBuffer::append(const char* data, size_t len) {
    buf_.append(data, len);

    // Only the unterminated tail counts toward the limit: complete frames
    // waiting to be drained are fine.
    size_t last_newline = buf_.rfind('\n');
    size_t tail = (last_newline == std::string::npos)
                      ? buf_.size()
                      : buf_.size() - last_newline - 1;
    return tail <= max_frame_;
}

std::optional<std::string> FrameBuffer::nextFrame() {
    while (true) {
        size_t pos = buf_.find('\n');
        if (pos == std::string::npos) return std::nullopt;

        std::string frame = buf_.substr(0, pos);
        buf_.erase(0, pos + 1);

        if (!frame.empty() && frame.back() == '\r') frame.pop_back();
        if (frame.empty()) continue;  // skip blank lines
        return frame;
    }
}