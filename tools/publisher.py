"""Publish messages to the broker.

Usage:
  python3 tools/publisher.py hello                     # one message
  python3 tools/publisher.py -n 5 "order"              # five messages ("order 1".."order 5")
  python3 tools/publisher.py -e logs -t fanout -k "" hi
  echo "from stdin" | python3 tools/publisher.py -     # one message per stdin line

Declares the exchange (and the queue + binding unless --no-bind) first, so a
message published before any consumer exists waits in the queue.
"""
import argparse
import json
import socket
import sys


def call(sock, reader, command):
    sock.sendall((json.dumps(command) + "\n").encode())
    reply = json.loads(reader.readline())
    print(f"> {json.dumps(command)}\n< {json.dumps(reply)}")
    return reply


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("body", help="message body, or '-' to read one message per stdin line")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("-e", "--exchange", default="shop")
    p.add_argument("-t", "--type", default="direct", choices=["direct", "fanout"])
    p.add_argument("-k", "--routing-key", default="new")
    p.add_argument("-q", "--queue", default="orders", help="queue to declare and bind")
    p.add_argument("--no-bind", action="store_true", help="only declare the exchange")
    p.add_argument("-n", "--count", type=int, default=1, help="repeat the body N times")
    args = p.parse_args()

    if args.body == "-":
        bodies = [line.rstrip("\n") for line in sys.stdin]
    elif args.count > 1:
        bodies = [f"{args.body} {i}" for i in range(1, args.count + 1)]
    else:
        bodies = [args.body]

    with socket.create_connection((args.host, args.port)) as sock:
        reader = sock.makefile("r", encoding="utf-8", newline="\n")
        call(sock, reader, {"cmd": "DECLARE_EXCHANGE", "exchange": args.exchange, "type": args.type})
        if not args.no_bind:
            call(sock, reader, {"cmd": "DECLARE_QUEUE", "queue": args.queue})
            # A repeated BIND is an ERROR by design; harmless here.
            call(sock, reader, {"cmd": "BIND", "exchange": args.exchange,
                                "queue": args.queue, "routing_key": args.routing_key})
        for body in bodies:
            call(sock, reader, {"cmd": "PUBLISH", "exchange": args.exchange,
                                "routing_key": args.routing_key, "body": body})


if __name__ == "__main__":
    main()
