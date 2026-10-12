CXX      ?= clang++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -g
CPPFLAGS += -Iinclude
BUILD    := build

# Everything except main.cpp is the library, so tests link against it too.
LIB_SRCS := $(filter-out src/main.cpp,$(wildcard src/*.cpp))
LIB_OBJS := $(LIB_SRCS:src/%.cpp=$(BUILD)/obj/%.o)
TESTS    := $(patsubst tests/%.cpp,$(BUILD)/%,$(wildcard tests/*_test.cpp))

.PHONY: all test clean
all: $(BUILD)/server

$(BUILD)/server: $(BUILD)/obj/main.o $(LIB_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(BUILD)/%_test: tests/%_test.cpp $(LIB_OBJS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^

$(BUILD)/obj/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -c -o $@ $<

test: $(TESTS)
	@for t in $(TESTS); do ./$$t > /dev/null || exit 1; echo "ok  $$t"; done

clean:
	rm -rf $(BUILD)/obj $(BUILD)/server $(TESTS)

-include $(wildcard $(BUILD)/obj/*.d)
