#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "broker/sink.h"

// Test double: records every line sent to it. Set `alive = false` to
// simulate a peer that has gone away.
class FakeSink : public Sink {
public:
    bool send(const std::string& line) override {
        std::lock_guard<std::mutex> lock(mutex);
        if (!alive) return false;
        lines.push_back(line);
        return true;
    }

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lock(mutex);
        return lines;
    }

    std::mutex mutex;
    std::vector<std::string> lines;
    bool alive = true;
};
