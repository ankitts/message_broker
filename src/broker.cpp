#include "broker/broker.h"
#include "broker/logger.h"
#include "broker/exchange_factory.h"

bool Broker::declareQueue(const std::string& queue_name) {
    if (m_queues.find(queue_name) != m_queues.end()) return true;

    m_queues.emplace(queue_name, std::make_unique<Queue>(queue_name));
    return true;
}

bool Broker::declareExchange(const std::string& exchange_name, ExchangeType exchange_type) {
    auto it = m_exchanges.find(exchange_name);
    if (it != m_exchanges.end()) {
        return it->second->get_exchange_type() == exchange_type;
    }

    auto exchange = makeExchange(exchange_name, exchange_type);
    if (!exchange) {                                       // e.g. Topic for now
        Logger::log("Exchange type not supported yet for '" + exchange_name + "'");
        return false;
    }
    m_exchanges.emplace(exchange_name, std::move(exchange));
    return true;
}

bool Broker::bind(const std::string& exchange_name,
                  const std::string& queue_name,
                  const std::string& routing_key) {
    auto exchange = m_exchanges.find(exchange_name);
    if (exchange == m_exchanges.end()) {
        Logger::log("Exchange: " + exchange_name + " is not declared.");
        return false;
    }

    auto queue = m_queues.find(queue_name);
    if (queue == m_queues.end()) {
        Logger::log("Queue: " + queue_name + " is not declared.");
        return false;
    }

    // The exchange gets a borrowed pointer; the broker keeps ownership.
    return exchange->second->bind(routing_key, queue->second.get());
}

std::optional<size_t> Broker::publish(const std::string& exchange_name, const Message& msg) {
    auto exchange = m_exchanges.find(exchange_name);
    if (exchange == m_exchanges.end()) {
        Logger::log("Exchange: " + exchange_name + " is not declared.");
        return std::nullopt;
    }

    return exchange->second->publish(msg);
}

Queue* Broker::getQueue(const std::string& queue_name) {
    auto it = m_queues.find(queue_name);
    return it == m_queues.end() ? nullptr : it->second.get();
}