#include "broker/consumer_registry.h"

#include <algorithm>

bool ConsumerRegistry::addConsumer(const std::string& queue, const SinkPtr& sink) {
    Consumers& c = m_consumers[queue];
    if (std::find(c.sinks.begin(), c.sinks.end(), sink) != c.sinks.end()) return false;
    c.sinks.push_back(sink);
    return true;
}

ConsumerRegistry::SinkPtr ConsumerRegistry::pickConsumer(const std::string& queue) {
    auto it = m_consumers.find(queue);
    if (it == m_consumers.end() || it->second.sinks.empty()) return nullptr;

    Consumers& c = it->second;
    SinkPtr sink = c.sinks[c.next];
    c.next = (c.next + 1) % c.sinks.size();
    return sink;
}

void ConsumerRegistry::recordDelivery(const SinkPtr& sink, const std::string& queue,
                                      uint64_t tag) {
    m_held[sink.get()].insert({queue, tag});
}

bool ConsumerRegistry::confirmAck(const SinkPtr& sink, const std::string& queue,
                                  uint64_t tag) {
    auto it = m_held.find(sink.get());
    if (it == m_held.end()) return false;
    if (it->second.erase({queue, tag}) == 0) return false;
    if (it->second.empty()) m_held.erase(it);
    return true;
}

std::vector<std::string> ConsumerRegistry::queuesWithConsumers() const {
    std::vector<std::string> names;
    for (const auto& [name, c] : m_consumers) {
        if (!c.sinks.empty()) names.push_back(name);
    }
    return names;
}

std::vector<ConsumerRegistry::Held> ConsumerRegistry::removeConnection(const SinkPtr& sink) {
    for (auto it = m_consumers.begin(); it != m_consumers.end();) {
        Consumers& c = it->second;
        auto pos = std::find(c.sinks.begin(), c.sinks.end(), sink);
        if (pos != c.sinks.end()) {
            size_t idx = static_cast<size_t>(pos - c.sinks.begin());
            c.sinks.erase(pos);
            if (idx < c.next) --c.next;
            if (c.sinks.empty()) {
                it = m_consumers.erase(it);
                continue;
            }
            c.next %= c.sinks.size();
        }
        ++it;
    }

    std::vector<Held> held;
    auto it = m_held.find(sink.get());
    if (it != m_held.end()) {
        held.assign(it->second.begin(), it->second.end());
        m_held.erase(it);
    }
    return held;
}
