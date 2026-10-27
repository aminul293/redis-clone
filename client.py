"""Tiny RESP client plus an interactive demo.

Run the server first:  python server.py --port 6390
Then run this demo:    python client.py
"""

import socket
import sys


class Client:
    def __init__(self, host="127.0.0.1", port=6390):
        self.sock = socket.create_connection((host, port))
        self.buf = self.sock.makefile("rb")

    def _read_line(self):
        return self.buf.readline().rstrip(b"\r\n")

    def _read_reply(self):
        prefix = self.buf.read(1)
        if prefix == b"+":
            return self._read_line().decode()
        if prefix == b"-":
            raise RuntimeError(self._read_line().decode())
        if prefix == b":":
            return int(self._read_line())
        if prefix == b"$":
            n = int(self._read_line())
            if n == -1:
                return None
            return self.buf.read(n + 2)[:n].decode()
        if prefix == b"*":
            n = int(self._read_line())
            if n == -1:
                return None
            return [self._read_reply() for _ in range(n)]
        raise RuntimeError(f"bad reply prefix {prefix!r}")

    def execute(self, *args):
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = str(a).encode()
            parts.append(b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n")
        self.sock.sendall(b"".join(parts))
        return self._read_reply()

    def __getattr__(self, name):
        return lambda *args: self.execute(name.upper(), *args)


def demo(port=6390):
    c = Client(port=port)
    steps = [
        ("PING",),
        ("SET", "greeting", "hello"),
        ("GET", "greeting"),
        ("SET", "session", "abc123", "PX", 400),
        ("GET", "session"),
        ("INCR", "visits"),
        ("INCR", "visits"),
        ("INCRBY", "visits", 10),
        ("LPUSH", "queue", "job1"),
        ("RPUSH", "queue", "job2", "job3"),
        ("LRANGE", "queue", 0, -1),
        ("LPOP", "queue"),
        ("HSET", "user:1", "name", "Aminul", "lang", "Python"),
        ("HGETALL", "user:1"),
        ("EXISTS", "greeting", "missing"),
        ("DEL", "greeting"),
        ("GET", "greeting"),
        ("TYPE", "queue"),
        ("KEYS", "*"),
    ]
    for step in steps:
        print(f"> {' '.join(map(str, step))}")
        print(f"  {c.execute(*step)!r}")
    import time
    time.sleep(0.45)
    print("> GET session   (after 450ms, it expired)")
    print(f"  {c.execute('GET', 'session')!r}")
    print("demo complete")


if __name__ == "__main__":
    demo(int(sys.argv[1]) if len(sys.argv) > 1 else 6390)
