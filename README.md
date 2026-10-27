# redis-clone

A small Redis-compatible server written in pure Python. It speaks the
real RESP protocol over TCP, so anything that can talk RESP can talk to
it. I built it to understand what Redis actually does under the hood:
parsing a wire protocol, managing shared state across connections, and
surviving a restart with an append-only file.

## Run it

```bash
python server.py --port 6390 --aof data.aof
```

In another terminal:

```bash
python client.py        # scripted demo against the running server
python -m pytest        # test suite
```

Or with redis-cli if you have it installed:

```bash
redis-cli -p 6390 SET name aminul
redis-cli -p 6390 GET name
```

## Example session

```text
> SET greeting hello
  'OK'
> INCR visits
  1
> LPUSH queue job1
  1
> HSET user:1 name Aminul lang Python
  2
> SET session abc123 PX 400
  'OK'
> GET session   (after 450ms, it expired)
  None
```

## What it supports

- Strings: SET (with EX/PX expiry), GET, INCR, INCRBY
- Keys: DEL, EXISTS, EXPIRE, TTL, TYPE, KEYS, FLUSHALL
- Lists: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE
- Hashes: HSET, HGET, HGETALL, HDEL
- PING and ECHO
- Persistence: every write command is appended to an AOF and replayed
  on startup

## How it works

Three pieces, all in server.py:

1. A RESP reader that parses commands incrementally from the socket.
   RESP frames arrays of bulk strings, so the parser reads a count,
   then that many length-prefixed strings.
2. A store: one dict mapping keys to (type, value, expiry). Every
   command goes through the same lock, which keeps concurrent clients
   from tearing a list or hash apart. Expiry is lazy: a key is checked
   and removed the next time anything touches it.
3. A command executor that validates types (GET on a list returns the
   same WRONGTYPE error real Redis gives) and appends each write
   command to the AOF as it executes. Real Redis can fsync on every
   write; this one just appends to the file buffer, which is a
   deliberate simplification.

## What it demonstrates

Wire protocols, socket servers, concurrency control, lazy expiry, and
write-ahead style persistence. The same ideas show up in every real
system that caches, queues, or counts things.

## Honest scope

No replication, no clustering, no pub/sub, no sorted sets, no eviction
policies, and no fsync tuning on the AOF. It is a learning build, sized
to be read in one sitting, not a production cache.
