"""collector.py message handling, without a network: a fake WebSocket records what is sent."""
import asyncio
import json

import pytest
import zstandard

import collector as C


def dumps(o):
    """Compact JSON, the way Kalshi sends it."""
    return json.dumps(o, separators=(",", ":"))


class FakeWS:
    def __init__(self):
        self.sent = []
        self.closed = False

    async def send(self, m):
        self.sent.append(json.loads(m))

    async def close(self):
        self.closed = True


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


def test_merged_subscribes_cause_no_false_gaps(col, tmp_path):
    """Kalshi folds every subscribe on a connection into one sid and confirms extra ones with
    an `ok` that uses up a sequence number. That must not count as a gap."""
    async def go():
        for t in ["M0", "M1", "M2"]:
            col._assign(t)
        c = col.conns[0]
        c.ws = FakeWS()
        await c.subscribe_all()
        await c.handle(dumps({"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 5}}))
        await c.handle(dumps({"type": "orderbook_snapshot", "sid": 5, "seq": 1, "msg": {}}))
        await c.handle(dumps({"id": 2, "type": "ok", "sid": 5, "seq": 2, "msg": {"market_tickers": ["M0", "M1"]}}))
        await c.handle(dumps({"type": "orderbook_snapshot", "sid": 5, "seq": 3, "msg": {}}))
        return c
    c = run(go())
    assert c.ob_sid == 5 and col.gaps == 0


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


def test_new_and_settled_markets_update_the_subscription(col, monkeypatch):
    async def go():
        col._assign("A")
        c = col.conns[0]
        c.ws, c.ob_sid = FakeWS(), 1
        for other in col.conns[1:]:
            other.ws, other.ob_sid = FakeWS(), 2
        await col.add_market("NEW")
        await col.remove_market("A")
        return [m for conn in col.conns if conn.ws for m in conn.ws.sent]
    sent = run(go())
    actions = {(m["params"]["action"], tuple(m["params"]["market_tickers"])) for m in sent}
    assert ("add_markets", ("NEW",)) in actions and ("delete_markets", ("A",)) in actions
    assert "A" not in col.where and "NEW" in col.where


def test_buffer_overflow_forces_reconnect(col):
    async def go():
        c = col.conns[0]
        c.ws = FakeWS()
        await c.handle(dumps({"id": 3, "type": "error", "msg": {"code": 25, "msg": "overflow"}}))
        return c.ws.closed
    assert run(go())


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
