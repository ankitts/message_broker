#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>

#include "broker/consumer_registry.h"
#include "fake_sink.h"

static std::shared_ptr<Sink> newSink() { return std::make_shared<FakeSink>(); }

int main() {
    {   // 1. add, and a duplicate add is refused
        ConsumerRegistry r;
        auto a = newSink();
        assert(r.addConsumer("q", a));
        assert(!r.addConsumer("q", a));
        assert(r.addConsumer("other", a));      // same sink, different queue is fine
    }
    {   // 2. pick on a queue nobody consumes
        ConsumerRegistry r;
        assert(r.pickConsumer("q") == nullptr);
    }
    {   // 3. round-robin across 3 sinks
        ConsumerRegistry r;
        auto a = newSink(), b = newSink(), c = newSink();
        r.addConsumer("q", a); r.addConsumer("q", b); r.addConsumer("q", c);
        for (int round = 0; round < 3; ++round) {
            assert(r.pickConsumer("q") == a);
            assert(r.pickConsumer("q") == b);
            assert(r.pickConsumer("q") == c);
        }
    }
    {   // 4. round-robin is per queue
        ConsumerRegistry r;
        auto a = newSink(), b = newSink();
        r.addConsumer("x", a); r.addConsumer("x", b);
        r.addConsumer("y", b);
        assert(r.pickConsumer("x") == a);
        assert(r.pickConsumer("y") == b);
        assert(r.pickConsumer("x") == b);
    }
    {   // 5. confirmAck: own tag, someone else's tag, twice, wrong queue
        ConsumerRegistry r;
        auto a = newSink(), b = newSink();
        r.addConsumer("q", a); r.addConsumer("q", b);
        r.recordDelivery(a, "q", 1);
        r.recordDelivery(b, "q", 2);

        assert(!r.confirmAck(a, "q", 2));       // b's tag
        assert(!r.confirmAck(a, "other", 1));   // right tag, wrong queue
        assert(!r.confirmAck(a, "q", 99));      // never delivered
        assert(r.confirmAck(a, "q", 1));
        assert(!r.confirmAck(a, "q", 1));       // twice
        assert(r.confirmAck(b, "q", 2));
    }
    {   // 6. same tag on two queues is two different deliveries
        ConsumerRegistry r;
        auto a = newSink();
        r.addConsumer("q1", a); r.addConsumer("q2", a);
        r.recordDelivery(a, "q1", 1);
        r.recordDelivery(a, "q2", 1);
        assert(r.confirmAck(a, "q1", 1));
        assert(r.confirmAck(a, "q2", 1));
    }
    {   // 7. removeConnection returns held tags and drops the sink everywhere
        ConsumerRegistry r;
        auto a = newSink(), b = newSink();
        r.addConsumer("q1", a); r.addConsumer("q2", a); r.addConsumer("q1", b);
        r.recordDelivery(a, "q1", 1);
        r.recordDelivery(a, "q2", 5);
        r.recordDelivery(b, "q1", 2);
        assert(r.confirmAck(a, "q1", 1) == true);
        r.recordDelivery(a, "q1", 3);

        auto held = r.removeConnection(a);
        std::sort(held.begin(), held.end());
        assert((held == std::vector<ConsumerRegistry::Held>{{"q1", 3}, {"q2", 5}}));

        assert(r.pickConsumer("q1") == b);
        assert(r.pickConsumer("q1") == b);
        assert(r.pickConsumer("q2") == nullptr);
        assert(!r.confirmAck(a, "q1", 3));      // gone
        assert(r.confirmAck(b, "q1", 2));       // b untouched
        assert(r.removeConnection(a).empty());  // removing twice is harmless
    }
    {   // 8. queuesWithConsumers
        ConsumerRegistry r;
        assert(r.queuesWithConsumers().empty());
        auto a = newSink();
        r.addConsumer("q1", a); r.addConsumer("q2", a);
        auto names = r.queuesWithConsumers();
        std::sort(names.begin(), names.end());
        assert((names == std::vector<std::string>{"q1", "q2"}));
        r.removeConnection(a);
        assert(r.queuesWithConsumers().empty());
    }
    {   // 9. removing a sink keeps round-robin fair for the rest
        ConsumerRegistry r;
        auto a = newSink(), b = newSink(), c = newSink();
        r.addConsumer("q", a); r.addConsumer("q", b); r.addConsumer("q", c);
        assert(r.pickConsumer("q") == a);       // next is b
        r.removeConnection(a);
        assert(r.pickConsumer("q") == b);
        assert(r.pickConsumer("q") == c);
        assert(r.pickConsumer("q") == b);
    }
    std::cout << "all registry tests passed\n";
    return 0;
}
