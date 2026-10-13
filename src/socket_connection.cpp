#include "broker/socket_connection.h"

#include <cerrno>

#include <sys/socket.h>
#include <unistd.h>

SocketConnection::~SocketConnection() {
    close(fd_);
}

bool SocketConnection::send(const std::string& line) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (closed_.load()) return false;

    std::string data = line;
    data += '\n';

    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            closed_.store(true);
            // Wakes the thread blocked in read() on this fd so it can clean
            // up. The fd itself stays open until the destructor.
            shutdown(fd_, SHUT_RDWR);
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}
