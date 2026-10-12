#pragma once

#include <iostream>
#include <mutex>
#include <string>

// Thread-safe line logger. Each call emits one whole line, so output from
// different threads never interleaves mid-line.
class Logger {
public:
    static void log(const std::string& line) {
        std::lock_guard<std::mutex> lock(mutex());
        std::cout << line << std::endl;
    }

private:
    static std::mutex& mutex() {
        static std::mutex m;
        return m;
    }
};