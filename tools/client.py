"""Tiny manual client: sends a few broker commands and prints each reply.

Usage: python3 tools/client.py [host] [port]
"""
import json
import socket
import sys

COMMANDS = [
    {"cmd": "DECLARE_EXCHANGE", "exchange": "shop", "type": "direct"},
    {"cmd": "DECLARE_QUEUE", "queue": "orders"},
    {"cmd": "BIND", "exchange": "shop", "queue": "orders", "routing_key": "new"},
    {"cmd": "PUBLISH", "exchange": "shop", "routing_key": "new", "body": "hello"},
]


def main(host: str, port: int) -> None:
    with socket.create_connection((host, port)) as sock:
        reader = sock.makefile("r", encoding="utf-8", newline="\n")
        for command in COMMANDS:
            line = json.dumps(command)
            sock.sendall((line + "\n").encode())
            print(f"> {line}")
            print(f"< {reader.readline().rstrip()}")


if __name__ == "__main__":
    main(
        sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1",
        int(sys.argv[2]) if len(sys.argv) > 2 else 8080,
    )
