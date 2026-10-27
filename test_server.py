import time

import pytest

from server import CommandExecutor, Store


@pytest.fixture()
def ex():
    return CommandExecutor(Store())


def parse(resp):
    """Decode the small subset of RESP the tests need."""
    if resp.startswith(b"+"):
        return resp[1:-2].decode()
    if resp.startswith(b"-"):
        return ("error", resp[1:-2].decode())
    if resp.startswith(b":"):
        return int(resp[1:-2])
    if resp.startswith(b"$-1"):
        return None
    if resp.startswith(b"$"):
        n = int(resp[1:resp.index(b"\r\n")])
        start = resp.index(b"\r\n") + 2
        return resp[start:start + n].decode()
    if resp.startswith(b"*"):
        return resp  # arrays checked loosely via membership below
    raise AssertionError(resp)


def test_set_get_and_delete(ex):
    assert parse(ex.execute(["SET", "k", "v"])) == "OK"
    assert parse(ex.execute(["GET", "k"])) == "v"
    assert parse(ex.execute(["EXISTS", "k", "nope"])) == 1
    assert parse(ex.execute(["DEL", "k"])) == 1
    assert parse(ex.execute(["GET", "k"])) is None


def test_incr(ex):
    assert parse(ex.execute(["INCR", "n"])) == 1
    assert parse(ex.execute(["INCR", "n"])) == 2
    assert parse(ex.execute(["INCRBY", "n", "8"])) == 10
    ex.execute(["SET", "s", "abc"])
    kind, msg = parse(ex.execute(["INCR", "s"]))
    assert kind == "error"


def test_expiry(ex):
    ex.execute(["SET", "temp", "x", "PX", "60"])
    assert parse(ex.execute(["GET", "temp"])) == "x"
    time.sleep(0.09)
    assert parse(ex.execute(["GET", "temp"])) is None
    assert parse(ex.execute(["TTL", "temp"])) == -2


def test_lists(ex):
    assert parse(ex.execute(["RPUSH", "q", "a", "b"])) == 2
    assert parse(ex.execute(["LPUSH", "q", "z"])) == 3
    assert parse(ex.execute(["LPOP", "q"])) == "z"
    assert parse(ex.execute(["RPOP", "q"])) == "b"
    assert parse(ex.execute(["LLEN", "q"])) == 1
    assert parse(ex.execute(["LPOP", "missing"])) is None


def test_hashes(ex):
    assert parse(ex.execute(["HSET", "h", "a", "1", "b", "2"])) == 2
    assert parse(ex.execute(["HSET", "h", "a", "9"])) == 0
    assert parse(ex.execute(["HGET", "h", "a"])) == "9"
    assert parse(ex.execute(["HGET", "h", "nope"])) is None
    assert parse(ex.execute(["HDEL", "h", "a"])) == 1


def test_wrongtype_and_unknown(ex):
    ex.execute(["RPUSH", "q", "a"])
    kind, msg = parse(ex.execute(["GET", "q"]))
    assert kind == "error" and "WRONGTYPE" in msg
    kind, msg = parse(ex.execute(["NOSUCH", "cmd"]))
    assert kind == "error" and "unknown command" in msg


def test_aof_replay(tmp_path):
    aof = tmp_path / "data.aof"
    ex1 = CommandExecutor(Store(), aof_path=str(aof))
    ex1.execute(["SET", "user", "aminul"])
    ex1.execute(["INCR", "logins"])
    ex1.execute(["RPUSH", "jobs", "a", "b"])
    ex2 = CommandExecutor(Store(), aof_path=str(aof))
    assert ex2.replay_aof() == 3
    assert parse(ex2.execute(["GET", "user"])) == "aminul"
    assert parse(ex2.execute(["GET", "logins"])) == "1"
