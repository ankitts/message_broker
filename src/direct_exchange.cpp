#include "broker/direct_exchange.h"
#include "broker/logger.h"

bool DirectExchange::bind(const std::string& routing_key, Queue* queue){
    // A null queue would crash on the first publish, so reject it up front.
    if (queue == nullptr) return false;

    // operator[] creates an empty set if this key is new. Take a REFERENCE
    // (auto&): plain `auto` would copy the set and the insert would be lost.
    auto& queues = m_bindings[routing_key];

    // insert().second is true only if the queue was not already present.
    if (!queues.insert(queue).second) {
        Logger::log("Queue is already bound to exchange.");
        return false;
    }
    return true;
}

std::vector<Queue*> DirectExchange::targets(const Message& msg){
    std::vector<Queue*> target_queues;

    // One lookup. find() (unlike operator[]) never inserts an empty entry
    // for unknown keys.
    auto it = m_bindings.find(msg.m_routing_key);
    if (it == m_bindings.end()) {
        Logger::log("dropped message: no binding for key '" + msg.m_routing_key + "'");
        return target_queues;
    }

    for (Queue* queue : it->second) {
        target_queues.push_back(queue);
    }
    return target_queues;
}