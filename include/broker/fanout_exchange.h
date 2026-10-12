#pragma once

#include "broker/exchange.h"

// Routes every message to ALL bound queues, ignoring the routing key.
class FanoutExchange: public Exchange{
    
    public:
        explicit FanoutExchange(std::string name):
            Exchange(std::move(name), ExchangeType::Fanout){}

        // The routing key is ignored. Returns false if the queue is null or
        // already bound (under any key).
        bool bind(const std::string& routing_key, Queue* queue) override;

    protected:
        // Every bound queue, regardless of the message.
        std::vector<Queue*> targets(const Message& msg) override;

    private:
        // Bound queues; a set, so each queue receives at most one copy.
        std::set<Queue*> m_queues;
};