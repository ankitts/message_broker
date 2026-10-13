"""Consume queues from the broker and print every line the server sends.

Usage:
  python3 tools/consumer.py                    # consume "orders", ack each message
  python3 tools/consumer.py -q a -q b          # consume several queues on one connection
  python3 tools/consumer.py --no-ack           # leave messages unacked

Declares the queue (and the exchange + binding unless --no-bind) first.
Stop with Ctrl-C.
"""
import argparse
import json
import socket


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("-q", "--queue", action="append", help="queue to consume (repeatable; default: orders)")
    p.add_argument("-e", "--exchange", default="shop")
    p.add_argument("-t", "--type", default="direct", choices=["direct", "fanout"])
    p.add_argument("-k", "--routing-key", default="new")
    p.add_argument("--no-bind", action="store_true", help="don't declare the exchange or bind")
    p.add_argument("--no-ack", action="store_true", help="don't ACK deliveries")
    args = p.parse_args()
    queues = args.queue or ["orders"]

    commands = []
    if not args.no_bind:
        commands.append({"cmd": "DECLARE_EXCHANGE", "exchange": args.exchange, "type": args.type})
    for q in queues:
        commands.append({"cmd": "DECLARE_QUEUE", "queue": q})
        if not args.no_bind:
            commands.append({"cmd": "BIND", "exchange": args.exchange, "queue": q,
                             "routing_key": args.routing_key})
    commands += [{"cmd": "CONSUME", "queue": q} for q in queues]

    with socket.create_connection((args.host, args.port)) as sock:
        reader = sock.makefile("r", encoding="utf-8", newline="\n")

        def send(command):
            sock.sendall((json.dumps(command) + "\n").encode())
            print(f"> {json.dumps(command)}")

        # Replies and DELIVER lines share the socket, so send everything and
        # then treat each incoming line the same way.
        try:
            for command in commands:
                send(command)
            for line in reader:
                print(f"< {line.rstrip()}")
                msg = json.loads(line)
                if msg.get("type") == "DELIVER" and not args.no_ack:
                    send({"cmd": "ACK", "queue": msg["queue"], "tag": msg["tag"]})
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
