#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <type_traits>

#include "broker/queue.h"

static Message make(int id, const std::string& body = "body") {
    return Message(id, "key", body);
}

int main() {
    {   // 1. new queue is empty
        Queue q("q");
        assert(q.readyCount() == 0);
        assert(q.unackedCount() == 0);
        assert(!q.deliver_next());
    }
    {   // 2. push increases ready only
        Queue q("q");
        q.push(make(0)); q.push(make(1)); q.push(make(2));
        assert(q.readyCount() == 3);
        assert(q.unackedCount() == 0);
    }
    {   // 3. FIFO order, and delivery moves ready -> unacked
        Queue q("q");
        q.push(make(0)); q.push(make(1)); q.push(make(2));
        for (int expected = 0; expected < 3; ++expected) {
            auto d = q.deliver_next();
            assert(d);
            assert(d->message.m_id == static_cast<uint64_t>(expected));
            assert(q.readyCount() == static_cast<size_t>(2 - expected));
            assert(q.unackedCount() == static_cast<size_t>(expected + 1));
        }
        assert(!q.deliver_next());
    }
    {   // 4. ack out of order empties the queue
        Queue q("q");
        q.push(make(0)); q.push(make(1)); q.push(make(2));
        auto a = q.deliver_next();
        auto b = q.deliver_next();
        auto c = q.deliver_next();
        assert(q.ack(b->tag));
        assert(q.unackedCount() == 2);
        assert(q.ack(c->tag));
        assert(q.ack(a->tag));
        assert(q.unackedCount() == 0);
        assert(q.readyCount() == 0);
    }
    {   // 5. unknown tag fails and changes nothing
        Queue q("q");
        q.push(make(0));
        auto d = q.deliver_next();
        assert(!q.ack(9999));
        assert(!q.ack(0));
        assert(q.unackedCount() == 1);
        assert(q.ack(d->tag));
    }
    {   // 6. double ack: second fails
        Queue q("q");
        q.push(make(0));
        auto d = q.deliver_next();
        assert(q.ack(d->tag));
        assert(!q.ack(d->tag));
        assert(q.unackedCount() == 0);
    }
    {   // 7. tags are unique
        Queue q("q");
        std::set<uint64_t> tags;
        for (int i = 0; i < 100; ++i) q.push(make(i));
        for (int i = 0; i < 100; ++i) tags.insert(q.deliver_next()->tag);
        assert(tags.size() == 100);
    }
    {   // 8. interleaved push / deliver / ack
        Queue q("q");
        q.push(make(0));
        auto a = q.deliver_next();
        assert(a->message.m_id == 0);
        q.push(make(1));
        assert(q.readyCount() == 1 && q.unackedCount() == 1);
        auto b = q.deliver_next();
        assert(b->message.m_id == 1);
        assert(q.ack(a->tag));
        assert(q.readyCount() == 0 && q.unackedCount() == 1);
        assert(q.ack(b->tag));
        assert(q.readyCount() == 0 && q.unackedCount() == 0);
    }
    {   // 9. message contents survive the trip intact
        Queue q("q");
        std::string big(100000, 'x');
        q.push(Message(7, "order.created", big));
        auto d = q.deliver_next();
        assert(d->message.m_id == 7);
        assert(d->message.m_routing_key == "order.created");
        assert(d->message.m_body == big);
    }
    {   // 10. ack after more pushes still targets the right message
        Queue q("q");
        q.push(make(0));
        auto a = q.deliver_next();
        q.push(make(1));
        auto b = q.deliver_next();
        assert(q.ack(b->tag));        // ack the newer one first
        assert(q.unackedCount() == 1);
        assert(!q.ack(b->tag));
        assert(q.ack(a->tag));
    }
    {   // 11. Message is really movable (guards against re-adding a copy ctor)
        static_assert(std::is_nothrow_move_constructible<Message>::value,
                      "Message must be movable");
    }
    std::cout << "all queue tests passed\n";
}