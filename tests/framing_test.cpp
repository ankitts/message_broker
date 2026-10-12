#include <cassert>
#include <iostream>
#include <string>
#include "broker/frame_buffer.h"

static bool feed(FrameBuffer& fb, const std::string& s) {
    return fb.append(s.data(), s.size());
}

int main() {
    {   // 1. two frames in one append
        FrameBuffer fb;
        assert(feed(fb, "aaa\nbbb\n"));
        assert(fb.nextFrame() == "aaa");
        assert(fb.nextFrame() == "bbb");
        assert(!fb.nextFrame());
    }
    {   // 2. one frame split across three appends
        FrameBuffer fb;
        feed(fb, "he"); assert(!fb.nextFrame());
        feed(fb, "llo"); assert(!fb.nextFrame());
        feed(fb, "\n");
        assert(fb.nextFrame() == "hello");
        assert(!fb.nextFrame());
    }
    {   // 3. \r\n stripped
        FrameBuffer fb;
        feed(fb, "abc\r\n");
        assert(fb.nextFrame() == "abc");
    }
    {   // 4. blank lines skipped
        FrameBuffer fb;
        feed(fb, "\n\r\nx\n\n");
        assert(fb.nextFrame() == "x");
        assert(!fb.nextFrame());
    }
    {   // 5. overflow: no newline, over limit, across many small appends
        FrameBuffer fb(10);
        assert(feed(fb, "12345"));
        assert(feed(fb, "12345"));
        assert(!feed(fb, "1"));
    }
    {   // 5b. many complete frames totalling > limit are fine
        FrameBuffer fb(10);
        assert(feed(fb, "aaaaaa\nbbbbbb\ncccccc\n"));
        assert(fb.nextFrame() == "aaaaaa");
        assert(fb.nextFrame() == "bbbbbb");
        assert(fb.nextFrame() == "cccccc");
    }
    {   // 6. embedded NUL preserved
        FrameBuffer fb;
        std::string s("a\0b\n", 4);
        feed(fb, s);
        assert(fb.nextFrame() == std::string("a\0b", 3));
    }
    {   // 7. frame ends, partial next frame stays buffered
        FrameBuffer fb;
        feed(fb, "one\ntw");
        assert(fb.nextFrame() == "one");
        assert(!fb.nextFrame());
        feed(fb, "o\n");
        assert(fb.nextFrame() == "two");
    }
    std::cout << "all framing tests passed\n";
}