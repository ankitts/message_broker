#include "broker/exchange.h"

#include "broker/logger.h"

size_t Exchange::publish(const Message& msg) {

    std::vector<Queue*> target_queues = targets(msg);

    size_t delivered = 0;
    for (Queue* queue : target_queues) {
        // push() takes its argument by value, so each queue gets its OWN
        // copy: they move and ack their messages independently.
        queue->push(msg);
        ++delivered;
    }
    return delivered;
}