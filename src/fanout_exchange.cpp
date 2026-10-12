#include "broker/fanout_exchange.h"
#include "broker/logger.h"

bool FanoutExchange::bind(const std::string& /*routing_key*/, Queue* queue){
    // A null queue would crash on the first publish, so reject it up front.
    if (queue == nullptr) return false;

    // insert().second is true only if the queue was not already present.
    if (!m_queues.insert(queue).second) {
        Logger::log("Queue is already bound to exchange.");
        return false;
    }
    return true;
}

std::vector<Queue*> FanoutExchange::targets(const Message& /*msg*/){
    std::vector<Queue*> target_queues;

    // No key check: fanout delivers to everyone.
    for (Queue* queue : m_queues) {
        target_queues.push_back(queue);
    }
    return target_queues;
}