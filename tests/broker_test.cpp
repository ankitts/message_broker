#include <cassert>
#include <iostream>
#include <string>

#include "broker/broker.h"

static Message make(int id, const std::string& key, const std::string& body = "body") {
    return Message(id, key, body);
}

int main() {
    {   // 1. declareQueue creates, and re-declaring is idempotent
        Broker b;
        assert(b.getQueue("q") == nullptr);
        assert(b.declareQueue("q"));
        Queue* first = b.getQueue("q");
        assert(first != nullptr);
        assert(first->name() == "q");
        assert(b.declareQueue("q"));
        assert(b.getQueue("q") == first);        // same object, not replaced
    }
    {   // 2. re-declaring does not wipe a queue's contents
        Broker b;
        b.declareQueue("q");
        b.declareExchange("ex", ExchangeType::Direct);
        b.bind("ex", "q", "k");
        b.publish("ex", make(0, "k"));
        assert(b.getQueue("q")->readyCount() == 1);
        assert(b.declareQueue("q"));
        assert(b.getQueue("q")->readyCount() == 1);
    }
    {   // 3. declareExchange: same type ok, different type rejected
        Broker b;
        assert(b.declareExchange("ex", ExchangeType::Direct));
        assert(b.declareExchange("ex", ExchangeType::Direct));
        assert(!b.declareExchange("ex", ExchangeType::Fanout));
        assert(b.declareExchange("ex", ExchangeType::Direct));   // original intact
    }
    {   // 4. a rejected re-declare does not replace the exchange or its bindings
        Broker b;
        b.declareQueue("q");
        b.declareExchange("ex", ExchangeType::Direct);
        b.bind("ex", "q", "k");
        assert(!b.declareExchange("ex", ExchangeType::Fanout));
        assert(b.publish("ex", make(0, "k")) == std::optional<size_t>(1));
    }
    {   // 5. bind fails for unknown exchange / unknown queue / both
        Broker b;
        assert(!b.bind("ex", "q", "k"));
        b.declareExchange("ex", ExchangeType::Direct);
        assert(!b.bind("ex", "q", "k"));          // queue missing
        b.declareQueue("q");
        assert(!b.bind("nope", "q", "k"));        // exchange missing
        assert(b.bind("ex", "q", "k"));           // finally fine
    }
    {   // 6. a failed bind creates nothing as a side effect
        Broker b;
        b.declareExchange("ex", ExchangeType::Direct);
        assert(!b.bind("ex", "ghost", "k"));
        assert(b.getQueue("ghost") == nullptr);
        assert(b.publish("ex", make(0, "k")) == std::optional<size_t>(0));
    }
    {   // 7. publish: unknown exchange (nullopt) is distinct from dropped (0)
        Broker b;
        assert(b.publish("nope", make(0, "k")) == std::nullopt);
        b.declareExchange("ex", ExchangeType::Direct);
        auto r = b.publish("ex", make(0, "k"));
        assert(r.has_value());
        assert(*r == 0);
    }
    {   // 8. full path: declare, bind, publish, consume
        Broker b;
        b.declareExchange("events", ExchangeType::Direct);
        b.declareQueue("orders");
        assert(b.bind("events", "orders", "order.created"));
        assert(b.publish("events", make(42, "order.created", "hello")) ==
               std::optional<size_t>(1));
        auto d = b.getQueue("orders")->deliver_next();
        assert(d);
        assert(d->message.m_id == 42);
        assert(d->message.m_routing_key == "order.created");
        assert(d->message.m_body == "hello");
    }
    {   // 9. only queues bound under the published key receive it
        Broker b;
        b.declareExchange("ex", ExchangeType::Direct);
        b.declareQueue("a"); b.declareQueue("b");
        b.bind("ex", "a", "x");
        b.bind("ex", "b", "y");
        assert(b.publish("ex", make(0, "x")) == std::optional<size_t>(1));
        assert(b.getQueue("a")->readyCount() == 1);
        assert(b.getQueue("b")->readyCount() == 0);
    }
    {   // 10. two queues on one exchange and key both receive a copy
        Broker b;
        b.declareExchange("ex", ExchangeType::Direct);
        b.declareQueue("a"); b.declareQueue("b");
        b.bind("ex", "a", "k");
        b.bind("ex", "b", "k");
        assert(b.publish("ex", make(0, "k")) == std::optional<size_t>(2));
        assert(b.getQueue("a")->readyCount() == 1);
        assert(b.getQueue("b")->readyCount() == 1);
    }
    {   // 11. one queue shared by two exchanges
        Broker b;
        b.declareExchange("e1", ExchangeType::Direct);
        b.declareExchange("e2", ExchangeType::Direct);
        b.declareQueue("q");
        b.bind("e1", "q", "k");
        b.bind("e2", "q", "k");
        b.publish("e1", make(1, "k"));
        b.publish("e2", make(2, "k"));
        assert(b.getQueue("q")->readyCount() == 2);
    }
    {   // 12. bindings are per exchange: e1's binding does not leak into e2
        Broker b;
        b.declareExchange("e1", ExchangeType::Direct);
        b.declareExchange("e2", ExchangeType::Direct);
        b.declareQueue("q");
        b.bind("e1", "q", "k");
        assert(b.publish("e2", make(0, "k")) == std::optional<size_t>(0));
        assert(b.getQueue("q")->readyCount() == 0);
    }
    {   // 13. duplicate bind through the broker: rejected, no double delivery
        Broker b;
        b.declareExchange("ex", ExchangeType::Direct);
        b.declareQueue("q");
        assert(b.bind("ex", "q", "k"));
        assert(!b.bind("ex", "q", "k"));
        assert(b.publish("ex", make(0, "k")) == std::optional<size_t>(1));
        assert(b.getQueue("q")->readyCount() == 1);
    }
    {   // 14. names are case-sensitive and independent
        Broker b;
        b.declareQueue("q");
        b.declareQueue("Q");
        assert(b.getQueue("q") != b.getQueue("Q"));
        b.declareExchange("ex", ExchangeType::Direct);
        b.declareExchange("EX", ExchangeType::Fanout);   // different name: no conflict
    }
    {   // 15. many objects: ownership/cleanup (leaks show up under ASan)
        Broker b;
        b.declareExchange("ex", ExchangeType::Direct);
        for (int i = 0; i < 200; ++i) {
            std::string q = "q" + std::to_string(i);
            b.declareQueue(q);
            b.bind("ex", q, "k");
        }
        assert(b.publish("ex", make(0, "k")) == std::optional<size_t>(200));
    }
    {   // 16. fanout exchange through the broker ignores the routing key
        Broker b;
        assert(b.declareExchange("fx", ExchangeType::Fanout));
        b.declareQueue("a"); b.declareQueue("b");
        assert(b.bind("fx", "a", "x"));
        assert(b.bind("fx", "b", "y"));
        assert(b.publish("fx", make(0, "other")) == std::optional<size_t>(2));
        assert(b.getQueue("a")->readyCount() == 1);
        assert(b.getQueue("b")->readyCount() == 1);
    }
    {   // 17. Topic is not supported yet: declare fails and creates nothing
        Broker b;
        assert(!b.declareExchange("t", ExchangeType::Topic));
        assert(b.publish("t", make(0, "k")) == std::nullopt);
    }
    std::cout << "all broker tests passed\n";
}