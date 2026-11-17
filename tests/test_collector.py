"""collector.py message handling, without a network: a fake WebSocket records what is sent."""
import asyncio
import json
import os
import signal
import time
from datetime import datetime, timezone

import pytest
import websockets
import zstandard

import collector as C


def dumps(o):
    """Compact JSON, the way Kalshi sends it."""
    return json.dumps(o, separators=(",", ":"))


class FakeWS:
    def __init__(self):
        self.sent = []
        self.closes = 0

    closed = property(lambda self: self.closes > 0)

    async def send(self, m):
        self.sent.append(json.loads(m))

    async def close(self):
        self.closes += 1


class ClosedWS(FakeWS):
    """A socket that went away: every send fails the way websockets does."""
    async def send(self, m):
        raise websockets.ConnectionClosedOK(None, None)


class ScriptedWS(FakeWS):
    """recv() plays back messages, then either drops the connection or stays silent."""
    def __init__(self, script, then):
        super().__init__()
        self.script, self.then = list(script), then

    async def recv(self, decode=None):
        await asyncio.sleep(0.01)
        if self.closed:
            raise websockets.ConnectionClosedOK(None, None)
        if self.script:
            return self.script.pop(0)
        if self.then == "drop":
            raise websockets.ConnectionClosedError(None, None)
        await asyncio.Event().wait()


def up(conn, sid=1):
    """The state of a connection that has connected and subscribed everything."""
    conn.ws, conn.ob_sid, conn.subscribed, conn.sub_pos = FakeWS(), sid, True, len(conn.chunks)
    return conn.ws


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def error(code, text, **envelope):
    return dumps({"type": "error", **envelope, "msg": {"code": code, "msg": text}})


@pytest.fixture
def col(tmp_path):
    c = C.Collector()
    c.writer.close()
    c.writer = C.HourlyWriter(tmp_path)
    return c


def records(col, tmp_path):
    col.writer.close()
    out = []
    for f in sorted((tmp_path / "closed").rglob("*.zst")):
        out += zstandard.ZstdDecompressor().stream_reader(open(f, "rb")).read().decode().splitlines()
    return [json.loads(l) for l in out]


def run(coro):
    return asyncio.run(coro)


def delta(seq, ticker="A", **extra):
    m = {"market_ticker": ticker, "market_id": "01a1d72f-993b-7a23-931a-537798ad51e2", "clock": seq,
         "price_dollars": "0.0700", "delta_fp": "-1.00", "side": "no", "ts": "2026-11-13T23:46:26.1Z",
         "ts_ms": 1794613586000, **extra}
    return dumps({"type": "orderbook_delta", "sid": 1, "seq": seq, "msg": m}).encode() + b"\n"


def actions(ws):
    return [m["params"].get("action") or m["cmd"] for m in ws.sent]


# ---------------------------------------------------------------- sequence numbers and storage
def test_merged_subscribes_cause_no_false_gaps(col, tmp_path):
    """Kalshi folds every subscribe on a connection into one sid and confirms extra ones with
    an `ok` that uses up a sequence number. That must not count as a gap."""
    async def go():
        for t in ["M0", "M1", "M2"]:
            col._assign(t)
        c = col.conns[0]
        c.ws = FakeWS()
        await c.subscribe_all()
        assert not c.ready()                # sent, but Kalshi has not confirmed the subscription yet
        await c.handle(dumps({"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 5}}))
        await c.handle(dumps({"type": "orderbook_snapshot", "sid": 5, "seq": 1, "msg": {}}))
        await c.handle(dumps({"id": 2, "type": "ok", "sid": 5, "seq": 2, "msg": {"market_tickers": ["M0", "M1"]}}))
        await c.handle(dumps({"type": "orderbook_snapshot", "sid": 5, "seq": 3, "msg": {}}))
        return c
    c = run(go())
    assert c.ob_sid == 5 and col.gaps == 0 and c.ready()


def test_real_gap_requests_fresh_snapshots(col):
    async def go():
        col._assign("A")
        c = col.conns[0]
        c.ws, c.ob_sid = FakeWS(), 1
        await c.handle(delta(1))
        await c.handle(delta(2))
        await c.handle(delta(5))
        await asyncio.sleep(0.05)
        return c
    c = run(go())
    assert col.gaps == 1
    snaps = [m for m in c.ws.sent if m["params"].get("action") == "get_snapshot"]
    assert snaps and snaps[0]["params"]["sids"] == [1] and "A" in snaps[0]["params"]["market_tickers"]


def test_a_gap_during_a_resnapshot_starts_another_pass(col):
    """Snapshots requested before the second gap can't repair what it lost."""
    async def go():
        col._assign("A")
        c = col.conns[0]
        up(c)
        await c.handle(delta(1))
        await c.handle(delta(3))            # gap: a pass starts
        await asyncio.sleep(0.05)           # it has asked for A's snapshot and is pausing
        await c.handle(delta(7))            # second gap while it runs
        await asyncio.sleep(0.5)
        return [m for m in c.ws.sent if m["params"].get("action") == "get_snapshot"]
    assert len(run(go())) == 2 and col.gaps == 2


def test_stored_records_are_one_per_line_and_slimmed(col, tmp_path):
    async def go():
        col._assign("A")
        c = col.conns[0]
        c.ws, c.ob_sid = FakeWS(), 1
        await c.handle(delta(1))
        await c.handle(dumps({"type": "ok", "id": 2, "sid": 1, "seq": 2,
                                   "msg": {"market_tickers": [f"T{i}" for i in range(26000)]}}) + "\n")
        await c.handle(dumps({"type": "orderbook_snapshot", "sid": 1, "seq": 3,
                                   "msg": {"market_ticker": "A", "market_id": "x", "clock": 3}}).encode() + b"\n")
    run(go())
    recs = records(col, tmp_path)
    assert len(recs) == 3 and col.gaps == 0
    d, ok, snap = (r["msg"] for r in recs)
    assert "market_id" not in d["msg"] and "ts" not in d["msg"] and d["msg"]["ts_ms"] == 1794613586000
    assert ok["msg"] == {"market_tickers_count": 26000}
    assert snap["msg"]["market_id"] == "x"          # snapshots keep the ticker -> id mapping


def test_ts_is_kept_when_there_is_no_ts_ms_to_replace_it(col, tmp_path):
    """Kalshi's spec marks both optional; dropping `ts` then would lose the only exchange time."""
    m = {"market_ticker": "A", "market_id": "01a1d72f-993b-7a23-931a-537798ad51e2", "clock": 1,
         "price_dollars": "0.0700", "delta_fp": "-1.00", "side": "no", "ts": "2026-11-13T23:46:26.1Z"}
    raw = dumps({"type": "orderbook_delta", "sid": 1, "seq": 1, "msg": m, "sending_ts_ms": 1794613586002})

    async def go():
        await col.conns[0].handle(raw.encode() + b"\n")
    run(go())
    stored = records(col, tmp_path)[0]["msg"]
    assert stored["msg"]["ts"] == "2026-11-13T23:46:26.1Z" and "market_id" not in stored["msg"]
    assert stored["sending_ts_ms"] == 1794613586002


# ---------------------------------------------------------------- adding and dropping markets
def test_adds_are_one_command_per_connection_and_determined_markets_send_nothing(col):
    """Every add or delete command is answered with the subscription's whole ticker list
    (~800 KB). One command per ticker, thousands at a time, overflowed Kalshi's buffers."""
    async def go():
        for t in ("A", "B"):
            col._assign(t)
        for c in col.conns:
            up(c)
        n = await col.add_markets([f"NEW{i}" for i in range(9)] + ["A"])      # A is already recorded
        col.on_lifecycle({"market_ticker": "A", "event_type": "determined"})
        col.on_lifecycle({"market_ticker": "A", "event_type": "settled"})
        return n, [m for c in col.conns for m in c.ws.sent]
    n, sent = run(go())
    assert n == 9
    assert [m["params"]["action"] for m in sent] == ["add_markets", "add_markets"]   # nothing for the removal
    assert sorted(t for m in sent for t in m["params"]["market_tickers"]) == sorted(f"NEW{i}" for i in range(9))
    assert "A" not in col.where and all("A" not in ch for c in col.conns for ch in c.chunks)


def test_large_adds_are_split_into_chunk_sized_commands(col, monkeypatch):
    monkeypatch.setattr(C, "CHUNK", 3)

    async def go():
        for c in col.conns:
            up(c)
        await col.add_markets([f"N{i}" for i in range(14)])                    # 7 per connection
        return [[len(m["params"]["market_tickers"]) for m in c.ws.sent] for c in col.conns]
    assert run(go()) == [[3, 3, 1], [3, 3, 1]]


def test_markets_added_while_subscribing_are_subscribed_exactly_once(col, monkeypatch):
    monkeypatch.setattr(C, "CHUNK", 2)

    async def go():
        for t in "ABCDEF":
            col._assign(t)                   # connection 0 gets A, C, E in chunks [A, C] and [E]
        c = col.conns[0]
        c.ws = FakeWS()
        task = asyncio.create_task(c.subscribe_all())
        await asyncio.sleep(0.05)            # the first chunk has been sent, the second not yet
        col.remove_market("A")               # makes room in the chunk that was already sent
        await col.add_markets(["LATE1"])     # goes into that chunk: needs a command of its own
        await col.add_markets(["LATE2"])     # goes into the chunk still to be sent: no command
        await task
        return c
    c = run(go())
    subs = [m["params"]["market_tickers"] for m in c.ws.sent if "market_tickers" in m["params"]]
    assert subs == [["A", "C"], ["LATE1"], ["E", "LATE2"]] and c.subscribed
    assert actions(c.ws) == ["subscribe"] * 5                                   # lifecycle, trade, 3 x orderbook


def test_a_closed_socket_does_not_break_adds_on_other_connections(col):
    async def go():
        for c in col.conns:
            up(c)
        col.conns[0].ws = ClosedWS()         # closed, but its reader has not noticed yet
        n = await col.add_markets(["N1", "N2", "N3", "N4"])
        return n
    assert run(go()) == 4
    assert col.conns[1].ws.sent[0]["params"]["market_tickers"] == ["N2", "N4"]
    assert not col.conns[0].pending                      # no reply can come for what was never sent
    assert {"N1", "N3"} <= set(col.where)                # still in its chunks: subscribed on reconnect


def lifecycle(ticker, event_type, open_ts=None):
    """Shaped like real market_lifecycle_v2 messages (Kalshi sends `created`, not `activated`)."""
    m = {"market_ticker": ticker, "event_type": event_type, "exchange_index": 0}
    if open_ts is not None:
        m["open_ts"], m["close_ts"] = open_ts, open_ts + 900
    return dumps({"type": "market_lifecycle_v2", "sid": 2, "seq": 1, "msg": m}).encode() + b"\n"


def test_created_markets_are_added_when_they_open(col, monkeypatch):
    monkeypatch.setattr(C, "OPEN_LAG_SECS", 0)
    looked_up = []

    async def fake_get_markets(params):
        looked_up.append(params["tickers"])
        return {t: "" for t in params["tickers"].split(",")}
    monkeypatch.setattr(col, "get_markets", fake_get_markets)

    async def go():
        for c in col.conns:
            up(c)
        loop_task = asyncio.create_task(col.new_market_loop())
        now = time.time()
        c0 = col.conns[0]
        await c0.handle(lifecycle("OPEN-NOW", "created", open_ts=int(now) - 5))     # already open
        await c0.handle(lifecycle("OPENS-SOON", "created", open_ts=now + 0.3))      # opens shortly
        await c0.handle(lifecycle("OPENS-SOON", "created", open_ts=now + 0.3))      # duplicate message
        await c0.handle(lifecycle("CANCELLED", "created", open_ts=now + 0.3))       # settled before open
        await c0.handle(lifecycle("CANCELLED", "settled"))
        await c0.handle(lifecycle("TOMORROW", "created", open_ts=now + 86400))      # opens much later
        assert set(col.opening) == {"OPENS-SOON", "TOMORROW"}
        assert col.new_tickers.qsize() == 1     # only OPEN-NOW is queued before OPENS-SOON opens
        await asyncio.sleep(0.5)                # OPENS-SOON has opened: now queued too
        assert set(col.opening) == {"TOMORROW"}
        await asyncio.sleep(2.5)                # past the loop's 2 s batching window
        loop_task.cancel()
        col.unschedule("TOMORROW")
        return [m for c in col.conns for m in c.ws.sent]
    sent = run(go())
    added = [t for m in sent if m["params"].get("action") == "add_markets" for t in m["params"]["market_tickers"]]
    assert sorted(added) == ["OPEN-NOW", "OPENS-SOON"]
    assert "CANCELLED" not in col.where and "TOMORROW" not in col.where
    assert not any("CANCELLED" in b or "TOMORROW" in b for b in looked_up)


def test_activated_without_open_ts_is_added_immediately(col, monkeypatch):
    async def fake_get_markets(params):
        return {t: "" for t in params["tickers"].split(",")}
    monkeypatch.setattr(col, "get_markets", fake_get_markets)

    async def go():
        for c in col.conns:
            up(c)
        loop_task = asyncio.create_task(col.new_market_loop())
        await col.conns[0].handle(lifecycle("RESUMED", "activated"))
        await asyncio.sleep(2.5)
        loop_task.cancel()
    run(go())
    assert "RESUMED" in col.where and not col.opening


def test_rescan_restores_the_opening_schedule_and_adds_open_markets(col, monkeypatch):
    """`created` messages from before a restart are gone; the rescan lists what has not opened yet."""
    now = time.time()
    listing = {"unopened": {"LATER": iso(now + 3600), "MOVED": iso(now + 7200), "NO-TIME": ""},
               "open": {"OPEN-A": iso(now - 60), "KNOWN": iso(now - 60)}}
    asked = []

    async def fake_get_markets(params):
        asked.append(params["status"])
        return listing[params["status"]]
    monkeypatch.setattr(col, "get_markets", fake_get_markets)

    async def go():
        col._assign("KNOWN")
        for c in col.conns:
            up(c)
        col.on_lifecycle({"market_ticker": "MOVED", "event_type": "created", "open_ts": int(now) + 60})
        n = await col.scan()
        when = {t: v[1] for t, v in col.opening.items()}
        for t in list(col.opening):
            col.unschedule(t)
        return n, when, [m for c in col.conns for m in c.ws.sent]
    n, when, sent = run(go())
    assert asked == ["unopened", "open"]                 # in this order: see scan()
    assert n == 1 and [m["params"]["market_tickers"] for m in sent] == [["OPEN-A"]]
    assert set(when) == {"LATER", "MOVED"} and abs(when["MOVED"] - (now + 7200)) < 2   # moved to its new time


def test_new_market_rest_does_not_list_yet_is_looked_up_again(col, monkeypatch):
    monkeypatch.setattr(C, "LOOKUP_RETRY_SECS", (0.05, 0.05))
    calls = []

    async def fake_get_markets(params):
        calls.append(params["tickers"])
        return {} if len(calls) == 1 else {t: "" for t in params["tickers"].split(",")}
    monkeypatch.setattr(col, "get_markets", fake_get_markets)

    async def go():
        for c in col.conns:
            up(c)
        await col.add_new(["FRESH"])
        assert "FRESH" not in col.where and col.new_tickers.empty()
        await asyncio.sleep(0.1)                         # it has been queued again
        await col.add_new([col.new_tickers.get_nowait()])
    run(go())
    assert "FRESH" in col.where and not col.lookup_tries and len(calls) == 2


def test_market_whose_opening_was_moved_back_is_rescheduled_not_subscribed(col, monkeypatch):
    later = time.time() + 3600

    async def fake_get_markets(params):
        return {t: iso(later) for t in params["tickers"].split(",")}
    monkeypatch.setattr(col, "get_markets", fake_get_markets)

    async def go():
        for c in col.conns:
            up(c)
        await col.add_new(["POSTPONED"])
        when = col.opening["POSTPONED"][1]
        col.unschedule("POSTPONED")
        return when, [m for c in col.conns for m in c.ws.sent]
    when, sent = run(go())
    assert abs(when - later) < 2 and sent == [] and "POSTPONED" not in col.where


# ---------------------------------------------------------------- errors from Kalshi
def test_buffer_overflow_forces_reconnect(col):
    async def go():
        c = col.conns[0]
        c.ws = FakeWS()
        await c.handle(dumps({"id": 3, "type": "error", "msg": {"code": 25, "msg": "overflow"}}))
        return c.ws.closed
    assert run(go())


@pytest.mark.parametrize("message", [
    error(10, "Channel error", sid=1, seq=9),            # a terminal error on a running subscription
    dumps({"type": "unsubscribed", "sid": 1, "seq": 9}),  # what Kalshi sends after one
    error(7, "Unknown subscription ID", id=4),            # a command for a subscription that is gone
])
def test_a_subscription_ended_by_kalshi_forces_a_reconnect(col, message):
    """The connection stays open in these cases, so nothing else would notice."""
    async def go():
        c = col.conns[0]
        ws = up(c)
        await c.handle(message)
        for cid in (5, 6):                                # replies to commands that were still in flight
            await c.handle(error(7, "Unknown subscription ID", id=cid))
        return ws.closes
    assert run(go()) == 1


def test_a_rate_limited_add_is_sent_again(col, monkeypatch):
    monkeypatch.setattr(C, "RETRY_SECS", 0.01)

    async def go():
        for c in col.conns:
            up(c)
        await col.add_markets(["NEW1", "NEW2"])
        c = col.conns[0]
        # sid and seq as well: a reply to a command may name the subscription without ending it
        await c.handle(error(27, "Too many requests", id=c.ws.sent[-1]["id"], sid=1, seq=1))
        await asyncio.sleep(0.1)
        return c.ws
    ws = run(go())
    first, again = ws.sent
    assert first["params"] == again["params"] and first["params"]["market_tickers"] == ["NEW1"]
    assert first["id"] != again["id"] and "NEW1" in col.where and not ws.closed


@pytest.mark.parametrize("code, sends", [(27, C.MAX_TRIES), (11, 1)])
def test_markets_kalshi_will_not_add_are_forgotten_for_the_rescan(col, monkeypatch, code, sends):
    """Kept in `where`, the rescan would skip them and they would never be recorded."""
    monkeypatch.setattr(C, "RETRY_SECS", 0.001)

    async def go():
        for c in col.conns:
            up(c)
        await col.add_markets(["REFUSED", "FINE"])
        c = col.conns[0]
        for _ in range(sends):
            await c.handle(error(code, "no", id=c.ws.sent[-1]["id"]))
            await asyncio.sleep(0.05)
        return len(c.ws.sent)
    assert run(go()) == sends
    assert "REFUSED" not in col.where and "FINE" in col.where


def test_a_channel_that_cannot_be_subscribed_makes_the_connection_count_as_down(col):
    async def go():
        col._assign("A")
        c = col.conns[0]
        c.ws = FakeWS()
        await c.subscribe_all()
        await c.handle(dumps({"id": 3, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 5}}))
        assert c.ready()
        trade = next(m for m in c.ws.sent if m["params"].get("channels") == ["trade"])
        await c.handle(error(8, "Unknown channel name", id=trade["id"]))
        return c.ready(), c.ws.closed
    assert run(go()) == (False, False)                   # the orderbook data keeps flowing; the healthcheck alerts


# ---------------------------------------------------------------- the connection loop and the process
def test_run_subscribes_everything_again_after_kalshi_ends_the_subscription(col, tmp_path, monkeypatch):
    subscribed = dumps({"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 1}})
    scripts = [([subscribed, dumps({"type": "unsubscribed", "sid": 1, "seq": 1})], "drop"), ([subscribed], "hang")]
    sockets = []

    class connect:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            sockets.append(ScriptedWS(*scripts[len(sockets)]))
            return sockets[-1]

        async def __aexit__(self, *exc):
            return False
    monkeypatch.setattr(C.websockets, "connect", connect)
    monkeypatch.setattr(C.random, "random", lambda: 0.0)

    async def go():
        for t in ("A", "B", "C"):
            col._assign(t)
        c = col.conns[1]                                  # holds B; connection 0 would add channel subscribes
        task = asyncio.create_task(c.run())
        for _ in range(100):
            await asyncio.sleep(0.05)
            if len(sockets) == 2 and c.ready():
                break
        ready = c.ready()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return ready
    assert run(go())
    assert sockets[0].closed and [m["params"]["market_tickers"] for m in sockets[1].sent] == [["B"]]
    events = [r["msg"]["type"] for r in records(col, tmp_path) if r["conn"] == -1]
    assert events == ["collector_connect", "collector_disconnect", "collector_connect"]


class FakeResponse:
    def __init__(self, status, body):
        self.status, self.body = status, body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        assert self.status == 200

    async def read(self):
        return json.dumps(self.body).encode()


def test_get_markets_follows_cursors_and_waits_out_rate_limits(col):
    pages = [(200, {"markets": [{"ticker": "A", "open_time": "2026-11-17T06:50:00Z"}], "cursor": "c1"}),
             (429, {"error": {"code": "too_many_requests"}}),
             (200, {"markets": [{"ticker": "B"}], "cursor": ""})]
    calls = []

    class Session:
        def get(self, url, params=None, headers=None):
            calls.append(dict(params))
            return FakeResponse(*pages[len(calls) - 1])
    col.session = Session()
    assert run(col.get_markets({"status": "open"})) == {"A": "2026-11-17T06:50:00Z", "B": ""}
    assert calls[0] == {"status": "open", "limit": "1000", "mve_filter": "exclude"}
    assert calls[1] == calls[2] == dict(calls[0], cursor="c1")       # the rate-limited page is asked for again
    assert C.parse_ts("2026-11-17T06:50:00Z") == 1794898200 and C.parse_ts("") is None
    assert C.parse_ts("2026-11-17T05:41:24.71598Z") == pytest.approx(1794894084.71598)


def stub_out_the_network(col, monkeypatch):
    async def no_markets(params):
        return {}

    async def forever(self):
        await asyncio.Event().wait()
    monkeypatch.setattr(col, "get_markets", no_markets)
    monkeypatch.setattr(C.Conn, "run", forever)


def test_collector_exits_with_an_error_when_one_of_its_tasks_dies(col, monkeypatch):
    """Otherwise it would keep running without that loop or connection and systemd would never restart it."""
    stub_out_the_network(col, monkeypatch)

    async def broken():
        raise OSError("No space left on device")
    monkeypatch.setattr(col, "housekeeping_loop", broken)
    assert run(col.main()) == 1


def test_collector_stops_cleanly_on_sigterm(col, monkeypatch):
    stub_out_the_network(col, monkeypatch)

    async def go():
        asyncio.get_running_loop().call_later(0.2, os.kill, os.getpid(), signal.SIGTERM)
        return await col.main()
    assert run(go()) == 0


def test_writer_rotates_hourly_and_recovers_crash_files(tmp_path, monkeypatch):
    clock = [1794650000.0]
    monkeypatch.setattr(C.time, "time", lambda: clock[0])
    w = C.HourlyWriter(tmp_path)
    w.write(b'{"a":1}\n')
    clock[0] += 3600
    w.write(b'{"a":2}\n')                            # next hour: first file is closed
    assert len(list((tmp_path / "closed").rglob("*.zst"))) == 1
    w.zw.flush(zstandard.FLUSH_BLOCK)
    w.fh.flush()                                      # simulate a crash: file left in open/
    crashed = w.path
    w2 = C.HourlyWriter(tmp_path)                     # restart recovers it
    assert not crashed.exists() and len(list((tmp_path / "closed").rglob("*.zst"))) == 2
    w2.close()
