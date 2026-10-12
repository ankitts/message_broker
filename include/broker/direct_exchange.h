#pragma once

#include "broker/exchange.h"

// Routes a message to the queues bound under EXACTLY its routing key
// (case-sensitive, no wildcards). Unknown keys are dropped.
class DirectExchange: public Exchange{
    
    public:
        explicit DirectExchange(std::string name):
            Exchange(std::move(name), ExchangeType::Direct){}

        // Bind under routing_key. The same queue may be bound under several
        // keys; binding the same queue to the same key twice returns false.
        bool bind(const std::string& routing_key, Queue* queue) override;

    protected:
        // Queues bound under msg.m_routing_key (empty if none).
        std::vector<Queue*> targets(const Message& msg) override;

    private:
        // routing key -> queues bound under it. A set, so a queue can appear
        // only once per key and never gets a duplicate delivery.
        std::unordered_map<std::string, std::set<Queue*>> m_bindings;
};