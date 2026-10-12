#include <cassert>
#include <iostream>
#include <string>

#include "broker/direct_exchange.h"
#include "broker/exchange_factory.h"
#include "broker/fanout_exchange.h"
#include "broker/queue.h"

static Message make(int id, const std::string& key, const std::string& body = "body") {
    return Message(id, key, body);
}

int main() {
    {   // 1. nothing bound: message is dropped
        DirectExchange ex("ex");
        assert(ex.publish(make(0, "a")) == 0);
    }
    {   // 2. bind A under "a", publish "a": A receives it
        DirectExchange ex("ex"); Queue a("a");
        assert(ex.bind("a", &a));
        assert(ex.publish(make(0, "a")) == 1);
        assert(a.readyCount() == 1);
    }
    {   // 3. unbound key is dropped and leaves A untouched
        DirectExchange ex("ex"); Queue a("a");
        ex.bind("a", &a);
        assert(ex.publish(make(0, "b")) == 0);
        assert(a.readyCount() == 0);
    }
    {   // 4. matching is exact: case, prefix, suffix, empty key
        DirectExchange ex("ex"); Queue a("a");
        ex.bind("a", &a);
        for (const char* k : {"A", "a.b", "aa", " a", ""}) {
            assert(ex.publish(make(0, k)) == 0);
        }
        assert(a.readyCount() == 0);
    }
    {   // 5. two queues under the same key: both receive it
        DirectExchange ex("ex"); Queue a("a"), b("b");
        ex.bind("k", &a);
        ex.bind("k", &b);
        assert(ex.publish(make(0, "k")) == 2);
        assert(a.readyCount() == 1);
        assert(b.readyCount() == 1);
    }
    {   // 6. different keys route to different queues
        DirectExchange ex("ex"); Queue a("a"), b("b");
        ex.bind("x", &a);
        ex.bind("y", &b);
        assert(ex.publish(make(0, "x")) == 1);
        assert(a.readyCount() == 1);
        assert(b.readyCount() == 0);
        assert(ex.publish(make(1, "y")) == 1);
        assert(a.readyCount() == 1);
        assert(b.readyCount() == 1);
    }
    {   // 7. duplicate bind: no double delivery
        DirectExchange ex("ex"); Queue a("a");
        assert(ex.bind("a", &a));
        assert(!ex.bind("a", &a));          // already bound
        assert(ex.publish(make(0, "a")) == 1);
        assert(a.readyCount() == 1);
    }
    {   // 8. each queue has its own copy
        DirectExchange ex("ex"); Queue a("a"), b("b");
        ex.bind("k", &a);
        ex.bind("k", &b);
        ex.publish(make(5, "k", "payload"));
        auto da = a.deliver_next();
        assert(da);
        assert(a.ack(da->tag));              // finish with A's copy...
        assert(a.unackedCount() == 0);
        assert(b.readyCount() == 1);         // ...B's is still waiting
        auto db = b.deliver_next();
        assert(db->message.m_body == "payload");
    }
    {   // 9. content is delivered intact
        DirectExchange ex("ex"); Queue a("a");
        ex.bind("order.created", &a);
        ex.publish(make(42, "order.created", "hello world"));
        auto d = a.deliver_next();
        assert(d->message.m_id == 42);
        assert(d->message.m_routing_key == "order.created");
        assert(d->message.m_body == "hello world");
    }
    {   // 10. null queue is rejected and does not poison the key
        DirectExchange ex("ex"); Queue a("a");
        assert(!ex.bind("a", nullptr));
        assert(ex.publish(make(0, "a")) == 0);
        assert(ex.bind("a", &a));
        assert(ex.publish(make(1, "a")) == 1);
    }
    {   // 11. same queue under several keys receives each message once
        DirectExchange ex("ex"); Queue a("a");
        ex.bind("x", &a);
        ex.bind("y", &a);
        assert(ex.publish(make(0, "x")) == 1);
        assert(ex.publish(make(1, "y")) == 1);
        assert(a.readyCount() == 2);
    }
    {   // 12. FIFO order is preserved through the exchange
        DirectExchange ex("ex"); Queue a("a");
        ex.bind("k", &a);
        for (int i = 0; i < 5; ++i) ex.publish(make(i, "k"));
        for (int i = 0; i < 5; ++i) assert(a.deliver_next()->message.m_id == static_cast<uint64_t>(i));
    }
    {   // 13. fanout: every bound queue gets the message, key is ignored
        FanoutExchange ex("fx"); Queue a("a"), b("b");
        assert(ex.bind("", &a));
        assert(ex.bind("whatever", &b));
        assert(ex.publish(make(0, "anything")) == 2);
        assert(a.readyCount() == 1);
        assert(b.readyCount() == 1);
    }
    {   // 14. fanout: nothing bound drops, duplicate and null binds rejected
        FanoutExchange ex("fx"); Queue a("a");
        assert(ex.publish(make(0, "k")) == 0);
        assert(!ex.bind("", nullptr));
        assert(ex.bind("x", &a));
        assert(!ex.bind("y", &a));           // same queue, any key: duplicate
        assert(ex.publish(make(1, "k")) == 1);
        assert(a.readyCount() == 1);
    }
    {   // 15. factory builds the right type and name
        auto d = makeExchange("d", ExchangeType::Direct);
        auto f = makeExchange("f", ExchangeType::Fanout);
        assert(d && d->get_exchange_type() == ExchangeType::Direct);
        assert(d->name() == "d");
        assert(f && f->get_exchange_type() == ExchangeType::Fanout);
        assert(f->name() == "f");
        assert(dynamic_cast<DirectExchange*>(d.get()));
        assert(dynamic_cast<FanoutExchange*>(f.get()));
    }
    {   // 16. factory: Topic is not implemented yet
        assert(makeExchange("t", ExchangeType::Topic) == nullptr);
    }
    {   // 17. factory-made exchanges route through the base interface
        auto ex = makeExchange("d", ExchangeType::Direct); Queue a("a");
        assert(ex->bind("k", &a));
        assert(ex->publish(make(0, "k")) == 1);
    }
    std::cout << "all exchange tests passed\n";
}