"""A small Redis-compatible server speaking RESP over TCP.

Supports strings with expiry, counters, lists, hashes, keyspace
commands, and append-only file persistence. Single file on purpose:
the point is to show the whole design fits in your head.
"""

import argparse
import os
import socket
import socketserver
import threading
import time

CRLF = b"\r\n"


class RespError(Exception):
    pass


def encode_simple(s):
    return b"+" + s.encode() + CRLF


def encode_error(s):
    return b"-" + s.encode() + CRLF


def encode_int(n):
    return b":" + str(n).encode() + CRLF


def encode_bulk(s):
    if s is None:
        return b"$-1" + CRLF
    b = s.encode() if isinstance(s, str) else s
    return b"$" + str(len(b)).encode() + CRLF + b + CRLF


def encode_array(items):
    if items is None:
        return b"*-1" + CRLF
    out = b"*" + str(len(items)).encode() + CRLF
    for it in items:
        out += encode_bulk(it)
    return out


class RespReader:
    """Incremental RESP parser over a buffered socket file."""

    def __init__(self, rfile):
        self.rfile = rfile

    def _line(self):
        line = self.rfile.readline()
        if not line:
            raise ConnectionError("client closed connection")
        if not line.endswith(CRLF):
            raise RespError("protocol error: expected CRLF")
        return line[:-2]

    def read_command(self):
        """Return a list of str arguments, or None on clean disconnect."""
        first = self.rfile.read(1)
        if not first:
            return None
        if first != b"*":
            raise RespError("protocol error: expected '*'")
        count = int(self._line())
        args = []
        for _ in range(count):
            marker = self.rfile.read(1)
            if marker != b"$":
                raise RespError("protocol error: expected '$'")
            length = int(self._line())
            data = self.rfile.read(length + 2)[:length]
            args.append(data.decode())
        return args


class Store:
    """In-memory data store: dict of key -> (type, value, expire_at)."""

    def __init__(self):
        self.data = {}
        self.lock = threading.RLock()

    def _entry(self, key):
        entry = self.data.get(key)
        if entry is None:
            return None
        if entry[2] is not None and entry[2] <= time.time():
            del self.data[key]
            return None
        return entry

    def get_typed(self, key, want):
        entry = self._entry(key)
        if entry is None:
            return None
        if entry[0] != want:
            raise RespError("WRONGTYPE operation against a key holding the wrong kind of value")
        return entry[1]

    # -- strings -----------------------------------------------------
    def cmd_set(self, key, value, ex=None, px=None):
        expire = None
        if ex is not None:
            expire = time.time() + int(ex)
        elif px is not None:
            expire = time.time() + int(px) / 1000.0
        with self.lock:
            self.data[key] = ("string", value, expire)
        return "OK"

    def cmd_get(self, key):
        with self.lock:
            return self.get_typed(key, "string")

    def cmd_del(self, *keys):
        n = 0
        with self.lock:
            for k in keys:
                if self._entry(k) is not None:
                    del self.data[k]
                    n += 1
        return n

    def cmd_exists(self, *keys):
        with self.lock:
            return sum(1 for k in keys if self._entry(k) is not None)

    def cmd_incr(self, key, by=1):
        with self.lock:
            cur = self.get_typed(key, "string")
            if cur is None:
                val = 0
                expire = None
            else:
                try:
                    val = int(cur)
                except ValueError:
                    raise RespError("ERR value is not an integer or out of range")
                expire = self.data[key][2]
            val += by
            self.data[key] = ("string", str(val), expire)
            return val

    def cmd_expire(self, key, seconds):
        with self.lock:
            entry = self._entry(key)
            if entry is None:
                return 0
            self.data[key] = (entry[0], entry[1], time.time() + int(seconds))
            return 1

    def cmd_ttl(self, key):
        with self.lock:
            entry = self._entry(key)
            if entry is None:
                return -2
            if entry[2] is None:
                return -1
            return max(0, int(entry[2] - time.time()))

    # -- lists --------------------------------------------------------
    def _list(self, key, create=False):
        lst = self.get_typed(key, "list")
        if lst is None and create:
            lst = []
            self.data[key] = ("list", lst, None)
        return lst

    def cmd_lpush(self, key, *values):
        with self.lock:
            lst = self._list(key, create=True)
            for v in values:
                lst.insert(0, v)
            return len(lst)

    def cmd_rpush(self, key, *values):
        with self.lock:
            lst = self._list(key, create=True)
            lst.extend(values)
            return len(lst)

    def cmd_lpop(self, key):
        with self.lock:
            lst = self._list(key)
            if not lst:
                return None
            val = lst.pop(0)
            if not lst:
                del self.data[key]
            return val

    def cmd_rpop(self, key):
        with self.lock:
            lst = self._list(key)
            if not lst:
                return None
            val = lst.pop()
            if not lst:
                del self.data[key]
            return val

    def cmd_llen(self, key):
        with self.lock:
            lst = self._list(key)
            return 0 if lst is None else len(lst)

    def cmd_lrange(self, key, start, stop):
        with self.lock:
            lst = self._list(key)
            if lst is None:
                return []
            n = len(lst)
            start, stop = int(start), int(stop)
            if start < 0:
                start = max(0, n + start)
            if stop < 0:
                stop = n + stop
            stop = min(stop, n - 1)
            if start > stop or start >= n:
                return []
            return lst[start:stop + 1]

    # -- hashes -------------------------------------------------------
    def _hash(self, key, create=False):
        h = self.get_typed(key, "hash")
        if h is None and create:
            h = {}
            self.data[key] = ("hash", h, None)
        return h

    def cmd_hset(self, key, *pairs):
        with self.lock:
            h = self._hash(key, create=True)
            added = 0
            for i in range(0, len(pairs), 2):
                if pairs[i] not in h:
                    added += 1
                h[pairs[i]] = pairs[i + 1]
            return added

    def cmd_hget(self, key, field):
        with self.lock:
            h = self._hash(key)
            return None if h is None else h.get(field)

    def cmd_hgetall(self, key):
        with self.lock:
            h = self._hash(key)
            if h is None:
                return []
            out = []
            for k, v in h.items():
                out.extend([k, v])
            return out

    def cmd_hdel(self, key, *fields):
        with self.lock:
            h = self._hash(key)
            if h is None:
                return 0
            n = 0
            for f in fields:
                if f in h:
                    del h[f]
                    n += 1
            if not h:
                del self.data[key]
            return n


WRITE_COMMANDS = {
    "SET", "DEL", "INCR", "INCRBY", "EXPIRE", "LPUSH", "RPUSH",
    "LPOP", "RPOP", "HSET", "HDEL",
}


class CommandExecutor:
    def __init__(self, store, aof_path=None):
        self.store = store
        self.aof_path = aof_path
        self._aof_lock = threading.Lock()

    def _log_aof(self, args):
        if not self.aof_path:
            return
        with self._aof_lock:
            with open(self.aof_path, "ab") as fh:
                fh.write(encode_array(args))

    def replay_aof(self):
        if not self.aof_path or not os.path.exists(self.aof_path):
            return 0
        count = 0
        with open(self.aof_path, "rb") as fh:
            reader = RespReader(fh)
            while True:
                try:
                    args = reader.read_command()
                except (ConnectionError, RespError, ValueError):
                    break
                if args is None:
                    break
                self.execute(args, replay=True)
                count += 1
        return count

    def execute(self, args, replay=False):
        if not args:
            return encode_error("ERR empty command")
        cmd = args[0].upper()
        a = args[1:]
        s = self.store
        try:
            if cmd == "PING":
                result = encode_simple(a[0] if a else "PONG")
            elif cmd == "ECHO":
                result = encode_bulk(a[0])
            elif cmd == "SET":
                if len(a) < 2:
                    raise RespError("ERR wrong number of arguments for 'set'")
                ex = px = None
                rest = [x.upper() for x in a[2:]]
                for i, tok in enumerate(rest):
                    if tok == "EX":
                        ex = a[2 + i + 1]
                    elif tok == "PX":
                        px = a[2 + i + 1]
                result = encode_simple(s.cmd_set(a[0], a[1], ex, px))
            elif cmd == "GET":
                result = encode_bulk(s.cmd_get(a[0]))
            elif cmd == "DEL":
                result = encode_int(s.cmd_del(*a))
            elif cmd == "EXISTS":
                result = encode_int(s.cmd_exists(*a))
            elif cmd == "INCR":
                result = encode_int(s.cmd_incr(a[0]))
            elif cmd == "INCRBY":
                result = encode_int(s.cmd_incr(a[0], int(a[1])))
            elif cmd == "EXPIRE":
                result = encode_int(s.cmd_expire(a[0], a[1]))
            elif cmd == "TTL":
                result = encode_int(s.cmd_ttl(a[0]))
            elif cmd == "LPUSH":
                result = encode_int(s.cmd_lpush(a[0], *a[1:]))
            elif cmd == "RPUSH":
                result = encode_int(s.cmd_rpush(a[0], *a[1:]))
            elif cmd == "LPOP":
                result = encode_bulk(s.cmd_lpop(a[0]))
            elif cmd == "RPOP":
                result = encode_bulk(s.cmd_rpop(a[0]))
            elif cmd == "LLEN":
                result = encode_int(s.cmd_llen(a[0]))
            elif cmd == "LRANGE":
                result = encode_array(s.cmd_lrange(a[0], a[1], a[2]))
            elif cmd == "HSET":
                if len(a) < 3 or len(a) % 2 == 0:
                    raise RespError("ERR wrong number of arguments for 'hset'")
                result = encode_int(s.cmd_hset(a[0], *a[1:]))
            elif cmd == "HGET":
                result = encode_bulk(s.cmd_hget(a[0], a[1]))
            elif cmd == "HGETALL":
                result = encode_array(s.cmd_hgetall(a[0]))
            elif cmd == "HDEL":
                result = encode_int(s.cmd_hdel(a[0], *a[1:]))
            elif cmd == "TYPE":
                with s.lock:
                    entry = s._entry(a[0])
                result = encode_simple(entry[0] if entry else "none")
            elif cmd == "KEYS":
                with s.lock:
                    keys = [k for k in list(s.data) if s._entry(k) is not None]
                result = encode_array(keys)
            elif cmd == "FLUSHALL":
                with s.lock:
                    s.data.clear()
                result = encode_simple("OK")
            else:
                return encode_error(f"ERR unknown command '{args[0]}'")
        except IndexError:
            return encode_error(f"ERR wrong number of arguments for '{cmd.lower()}'")
        except RespError as exc:
            return encode_error(str(exc))
        except ValueError:
            return encode_error("ERR value is not an integer or out of range")

        if cmd in WRITE_COMMANDS and not replay:
            self._log_aof(args)
        return result


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        reader = RespReader(self.request.makefile("rb"))
        while True:
            try:
                args = reader.read_command()
            except (ConnectionError, RespError, ValueError):
                return
            if args is None:
                return
            self.request.sendall(self.server.executor.execute(args))


class RedisServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, executor):
        super().__init__(address, Handler)
        self.executor = executor


def main():
    parser = argparse.ArgumentParser(description="redis-clone server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6390)
    parser.add_argument("--aof", default=None, help="append-only file path")
    ns = parser.parse_args()

    executor = CommandExecutor(Store(), aof_path=ns.aof)
    replayed = executor.replay_aof()
    if replayed:
        print(f"replayed {replayed} commands from {ns.aof}")
    server = RedisServer((ns.host, ns.port), executor)
    print(f"redis-clone listening on {ns.host}:{ns.port} (pid {os.getpid()})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("shutting down")


if __name__ == "__main__":
    main()
