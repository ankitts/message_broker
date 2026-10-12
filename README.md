# Message broker

A small RabbitMQ-style message broker in C++17. Clients connect over TCP and send
newline-delimited JSON commands; the server replies with one JSON line per command.

- **Queues** store messages in FIFO order.
- **Exchanges** route published messages to queues: `direct` (exact routing key) and `fanout` (all bound queues). `topic` is reserved but not implemented yet.

Status: declare, bind and publish work over the network. Consumers and acks (3.2) and persistence are not built yet.

## Requirements

- A C++17 compiler (`clang++` on macOS)
- [nlohmann/json](https://github.com/nlohmann/json): `brew install nlohmann-json`
- `make`; Python 3 for the sample client

## Build

```sh
CPPFLAGS=-I/opt/homebrew/include make
```

The `CPPFLAGS` tells the compiler where Homebrew installed nlohmann/json. The server binary is `build/server`.

## Run

```sh
./build/server
```

It listens on port 8080 (set in `src/main.cpp`).

Try it from another terminal:

```sh
python3 tools/client.py
```

or by hand:

```sh
nc 127.0.0.1 8080
```

## Protocol

One JSON object per line. Every command gets exactly one reply line.

| Command | Example |
|---|---|
| Declare a queue | `{"cmd":"DECLARE_QUEUE","queue":"orders"}` |
| Declare an exchange | `{"cmd":"DECLARE_EXCHANGE","exchange":"shop","type":"direct"}` |
| Bind a queue | `{"cmd":"BIND","exchange":"shop","queue":"orders","routing_key":"new"}` |
| Publish | `{"cmd":"PUBLISH","exchange":"shop","routing_key":"new","body":"hello"}` |

Replies:

- Success: `{"type":"OK"}`. A publish also returns `"routed"` (how many queues got it) and `"id"`.
- Failure: `{"type":"ERROR","reason":"..."}`. The connection stays open.
- A line over 1 MiB gets an ERROR and the connection is closed.

## Test

```sh
CPPFLAGS=-I/opt/homebrew/include make test
```

Builds and runs every `tests/*_test.cpp`; each prints `ok` when it passes.

To check for memory errors or data races, compile the tests by hand with `-fsanitize=address,undefined` or `-fsanitize=thread`.

## Project layout

| Path | Contents |
|---|---|
| `include/broker/`, `src/` | The broker library and server |
| `tests/` | Test programs (plain `assert`) |
| `tools/client.py` | Sample client |

Clean build output with `make clean`.
