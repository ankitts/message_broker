#include "broker/queue.h"

#include <utility>

void Queue::push(Message msg) {
    m_ready.push_back(std::move(msg));
}

std::optional<Delivery> Queue::deliver_next() {
    if (m_ready.empty()) {
        return std::nullopt;
    }

    const uint64_t tag = ++m_last_tag;

    // Move the message into the unacked map: the broker keeps this copy
    // until the ack arrives.
    auto it = m_unacked.emplace(tag, std::move(m_ready.front())).first;
    m_ready.pop_front();

    // The consumer gets its own copy (the one deliberate copy per delivery).
    return Delivery{it->second, tag};
}

bool Queue::ack(uint64_t tag) {
    return m_unacked.erase(tag) > 0;
}

size_t Queue::readyCount() const {
    return m_ready.size();
}

size_t Queue::unackedCount() const {
    return m_unacked.size();
}