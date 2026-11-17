#!/usr/bin/env python3
"""
Kalshi market-data collector.

- Lists every open market over REST, shards the tickers across several
  WebSocket connections, and subscribes to `orderbook_delta` for all of them.
- Subscribes to `trade` and `market_lifecycle_v2` (all markets) on connection 0,
  adds newly activated markets live and drops determined/settled ones.
- Writes every raw message, wrapped with a local receive timestamp, to an
  hourly zstd-compressed JSONL file. Finished hours are moved to
  data/closed/date=YYYY-MM-DD/ for the rclone upload timer.
- Detects per-subscription `seq` gaps, logs them into the data stream, and
  requests fresh snapshots for the affected connection.

Config comes from environment variables (see collector.env.example).
"""
import asyncio
import base64
import logging
import os
import random
import re
import signal
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import orjson
import websockets
import zstandard as zstd

try:
    import uvloop  # faster event loop; optional
except ImportError:
    uvloop = None
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding

# ---------------------------------------------------------------- config
def env(name: str, default: str = "") -> str:
    """Env var with any trailing ' # comment' removed (systemd keeps them)."""
    return os.environ.get(name, default).split(" #")[0].split("\t#")[0].strip()


KEY_ID = env("KALSHI_KEY_ID")
KEY_PATH = env("KALSHI_KEY_PATH", "kalshi.key")
WS_URL = env("KALSHI_WS_URL", "wss://external-api-ws.kalshi.com/trade-api/ws/v2")
REST_BASE = env("KALSHI_REST_BASE", "https://external-api.kalshi.com")
DATA_DIR = Path(env("DATA_DIR", "data"))
N_CONN = int(env("N_CONN", "4"))              # WebSocket connections per process
CHUNK = int(env("CHUNK", "500"))              # tickers per orderbook subscription
INCLUDE_MVE = env("INCLUDE_MVE", "0") == "1"  # include multivariate combo markets
REFRESH_SECS = int(env("REFRESH_SECS", "1800"))  # full market re-scan (safety net)
ZSTD_LEVEL = int(env("ZSTD_LEVEL", "1"))
# Run N_SHARDS processes (one per core); each records the markets whose
# crc32(ticker) % N_SHARDS == SHARD. Shard 0 also records the trade channel.
SHARD = int(env("SHARD", "0"))
N_SHARDS = int(env("N_SHARDS", "1"))
HC_URL = env("HEALTHCHECK_URL").replace("{shard}", str(SHARD))  # e.g. https://hc-ping.com/<key>/kalshi-s{shard}?create=1
if not KEY_ID:
    raise SystemExit("KALSHI_KEY_ID is not set (check collector.env)")


def mine(ticker: str) -> bool:
    return zlib.crc32(ticker.encode()) % N_SHARDS == SHARD

WS_PATH = "/trade-api/ws/v2"
MARKETS_PATH = "/trade-api/v2/markets"
# Fast path: orderbook messages arrive as {"type":"orderbook_...","sid":N,"seq":N,...}.
# Pull sid/seq from the head without parsing the whole message; anything that
# doesn't match falls back to a full parse.
OB_HEAD = re.compile(rb'\{"type":"orderbook_(delta|snapshot)","sid":(\d+),"seq":(\d+)[,}]')
# Redundant fields dropped from deltas before writing: market_id (a random UUID
# that duplicates market_ticker and barely compresses) and the deprecated
# string `ts` (duplicates ts_ms). Snapshots are kept intact, so they still
# record the ticker -> market_id mapping.
DELTA_STRIP = re.compile(rb',"(?:market_id":"[0-9a-f-]{36}|ts":"[^"]*)"')

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s %(levelname)s [s{SHARD}] %(message)s")
log = logging.getLogger("collector")


# ---------------------------------------------------------------- auth
def _load_key(path):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


KEY = _load_key(KEY_PATH)


def sign_headers(method: str, path: str) -> dict:
    """Kalshi signature: timestamp_ms + METHOD + path (no query string)."""
    ts = str(int(time.time() * 1000))
    msg = (ts + method + path.split("?")[0]).encode()
    if isinstance(KEY, ed25519.Ed25519PrivateKey):
        sig = KEY.sign(msg)
    else:  # RSA
        sig = KEY.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


# ---------------------------------------------------------------- output
class HourlyWriter:
    """Streams lines into data/open/kalshi_<YYYYmmddTHH>_<start>.jsonl.zst and,
    when the UTC hour changes (or on shutdown), moves the finished file to
    data/closed/date=YYYY-MM-DD/."""

    def __init__(self, root: Path):
        self.open_dir = root / "open"
        self.closed_dir = root / "closed"
        self.open_dir.mkdir(parents=True, exist_ok=True)
        self.closed_dir.mkdir(parents=True, exist_ok=True)
        self.cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
        self.hour = None
        self.fh = self.zw = self.path = None
        # Files left in open/ by a crash: still readable up to the truncation point.
        for p in self.open_dir.glob(f"*_s{SHARD}_*.jsonl.zst"):
            self._move_to_closed(p)

    def _move_to_closed(self, p: Path):
        stamp = p.name.split("_")[1]  # YYYYmmddTHH
        dest = self.closed_dir / f"date={stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}" / p.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        p.rename(dest)
        log.info("closed %s", dest)

    def rotate_if_needed(self):
        hour = int(time.time() // 3600)
        if hour == self.hour:
            return
        self.close()
        self.hour = hour
        key = datetime.fromtimestamp(hour * 3600, tz=timezone.utc).strftime("%Y%m%dT%H")
        self.path = self.open_dir / f"kalshi_{key}_s{SHARD}_{int(time.time())}.jsonl.zst"
        self.fh = open(self.path, "wb")
        self.zw = self.cctx.stream_writer(self.fh)

    def write(self, line: bytes):
        self.rotate_if_needed()
        self.zw.write(line)

    def close(self):
        if self.zw is None:
            return
        self.zw.close()  # ends the zstd frame
        if not self.fh.closed:
            self.fh.close()
        self._move_to_closed(self.path)
        self.zw = self.fh = self.path = None


# ---------------------------------------------------------------- connections
class Conn:
    """One WebSocket connection. Kalshi merges every orderbook_delta subscribe
    on a connection into a single subscription (one sid), confirming the extra
    ones with an "ok" message that consumes a seq number. Tickers are still kept
    in CHUNK-sized lists, but only to keep each command message small."""

    def __init__(self, idx: int, coll: "Collector"):
        self.idx = idx
        self.c = coll
        self.chunks: list[list[str]] = []
        self.ob_sid: int | None = None             # the orderbook_delta subscription
        self.pending: dict[int, int | None] = {}   # command id -> chunk index
        self.last_seq: dict[int, int] = {}
        self.resnap_task: asyncio.Task | None = None
        self.ws = None
        self.next_id = 1

    def total(self) -> int:
        return sum(len(ch) for ch in self.chunks)

    async def send(self, cmd: str, params: dict, chunk_idx: int | None = None):
        if self.ws is None:
            return
        cid = self.next_id
        self.next_id += 1
        self.pending[cid] = chunk_idx
        await self.ws.send(orjson.dumps({"id": cid, "cmd": cmd, "params": params}).decode())

    async def subscribe_tickers(self, tickers: list[str], chunk_idx: int | None = None):
        if tickers:
            await self.send("subscribe", {"channels": ["orderbook_delta"],
                                          "market_tickers": list(tickers)}, chunk_idx)

    async def subscribe_all(self):
        if self.idx == 0:
            await self.send("subscribe", {"channels": ["market_lifecycle_v2"]})
            if SHARD == 0:
                await self.send("subscribe", {"channels": ["trade"]})
        for i in range(len(self.chunks)):
            await self.subscribe_tickers(self.chunks[i], i)
            await asyncio.sleep(0.2)  # stay well under the command rate limit

    async def retry_subscribe(self, i: int, delay: float):
        await asyncio.sleep(delay)
        if self.ws is not None and i < len(self.chunks):
            await self.subscribe_tickers(self.chunks[i], i)

    async def resnapshot(self):
        """Fresh snapshots for every market on this connection, in chunks."""
        for ch in list(self.chunks):
            if self.ws is None or self.ob_sid is None:
                return
            if ch:
                await self.send("update_subscription", {"sids": [self.ob_sid], "action": "get_snapshot",
                                                        "market_tickers": list(ch)})
                await asyncio.sleep(0.2)

    async def run(self):
        backoff = 1.0
        while True:
            sub_task = None
            try:
                async with websockets.connect(
                    WS_URL,
                    additional_headers=sign_headers("GET", WS_PATH),
                    max_size=None, ping_interval=20, ping_timeout=30, close_timeout=5,
                    compression=None,  # no permessage-deflate: inbound bandwidth is free, CPU isn't
                ) as ws:
                    self.ws = ws
                    self.ob_sid = None
                    self.last_seq.clear()
                    self.pending.clear()
                    log.info("conn %d connected (%d markets in %d chunks)",
                             self.idx, self.total(), len(self.chunks))
                    self.c.event({"type": "collector_connect", "conn": self.idx})
                    sub_task = asyncio.create_task(self.subscribe_all())
                    backoff = 1.0
                    while True:
                        # decode=False: raw bytes, skipping a UTF-8 decode/encode round trip
                        await self.handle(await ws.recv(decode=False))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # network errors, handshake failures, closes
                log.warning("conn %d dropped: %r", self.idx, e)
            finally:
                self.ws = None
                for task in (sub_task, self.resnap_task):
                    if task:
                        task.cancel()
                self.resnap_task = None
            self.c.event({"type": "collector_disconnect", "conn": self.idx})
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)

    def check_seq(self, sid: int, seq: int):
        """Every message carrying sid+seq counts, including "ok" responses."""
        last = self.last_seq.get(sid)
        if last is not None and seq != last + 1:
            self.c.gaps += 1
            self.c.event({"type": "collector_gap", "conn": self.idx, "sid": sid,
                          "expected": last + 1, "got": seq})
            if sid == self.ob_sid and (self.resnap_task is None or self.resnap_task.done()):
                self.resnap_task = asyncio.create_task(self.resnapshot())
        self.last_seq[sid] = seq

    async def handle(self, raw):
        recv_ns = time.time_ns()
        data = raw.encode() if isinstance(raw, str) else raw
        data = data.rstrip()  # Kalshi messages end with a newline; keep one record per line
        self.c.msgs += 1

        head = OB_HEAD.match(data)
        if head:  # the vast majority of traffic
            if head[1] == b"delta":
                data = DELTA_STRIP.sub(b"", data)
            self.c.writer.write(b'{"recv_ns":%d,"conn":%d,"msg":%s}\n' % (recv_ns, self.idx, data))
            self.check_seq(int(head[2]), int(head[3]))
            return

        m = orjson.loads(data)
        t = m.get("type")
        body = m.get("msg") or {}
        if t == "ok" and isinstance(body, dict) and "market_tickers" in body:
            # Kalshi echoes every ticker in the subscription (~800 KB). Store only the count.
            slim = {k: v for k, v in body.items() if k != "market_tickers"}
            slim["market_tickers_count"] = len(body["market_tickers"])
            data = orjson.dumps(dict(m, msg=slim))
        self.c.writer.write(b'{"recv_ns":%d,"conn":%d,"msg":%s}\n' % (recv_ns, self.idx, data))
        if not isinstance(body, dict):
            body = {}
        sid, seq = m.get("sid"), m.get("seq")
        if sid is not None and seq is not None:
            self.check_seq(sid, seq)

        if t == "subscribed":
            self.pending.pop(m.get("id"), None)
            if body.get("channel") == "orderbook_delta":
                self.ob_sid = body.get("sid")
        elif t in ("ok", "unsubscribed"):
            self.pending.pop(m.get("id"), None)
        elif t == "error":
            ci = self.pending.pop(m.get("id"), None)
            code = body.get("code")
            log.error("conn %d error: %s", self.idx, body)
            if code == 27 and ci is not None:      # command rate limit
                asyncio.create_task(self.retry_subscribe(ci, 2.0))
            elif code == 26:
                log.error("per-subscription market limit hit: raise N_CONN")
            elif code == 25:                        # buffer overflow: reconnect + resnapshot
                await self.ws.close()
        elif t == "market_lifecycle_v2":
            await self.c.on_lifecycle(body)


# ---------------------------------------------------------------- collector
class Collector:
    def __init__(self):
        self.writer = HourlyWriter(DATA_DIR)
        self.conns = [Conn(i, self) for i in range(N_CONN)]
        self.where: dict[str, tuple[int, int]] = {}  # ticker -> (conn, chunk)
        self.new_tickers: asyncio.Queue[str] = asyncio.Queue()
        self.msgs = 0
        self.gaps = 0
        self.session: aiohttp.ClientSession | None = None

    def event(self, ev: dict):
        """Collector-generated records go in the same stream with conn = -1."""
        self.writer.write(b'{"recv_ns":%d,"conn":-1,"msg":%s}\n'
                          % (time.time_ns(), orjson.dumps(ev)))

    # -- REST
    async def get_markets(self, params: dict) -> list[str]:
        tickers, cursor = [], ""
        while True:
            q = dict(params, limit="1000")
            if cursor:
                q["cursor"] = cursor
            if not INCLUDE_MVE:
                q["mve_filter"] = "exclude"
            async with self.session.get(REST_BASE + MARKETS_PATH, params=q,
                                        headers=sign_headers("GET", MARKETS_PATH)) as r:
                if r.status == 429:
                    await asyncio.sleep(2)
                    continue
                r.raise_for_status()
                data = await r.json()
            tickers += [mk["ticker"] for mk in data.get("markets", [])]
            cursor = data.get("cursor") or ""
            if not cursor:
                return tickers
            await asyncio.sleep(0.1)

    # -- sharding
    def _assign(self, t: str):
        conn = min(self.conns, key=Conn.total)
        for i, ch in enumerate(conn.chunks):
            if len(ch) < CHUNK:
                break
        else:
            conn.chunks.append([])
            i = len(conn.chunks) - 1
        conn.chunks[i].append(t)
        self.where[t] = (conn.idx, i)
        return conn, i

    async def add_market(self, t: str):
        if t in self.where:
            return
        conn, i = self._assign(t)
        if conn.ws is None:
            return  # picked up by subscribe_all on (re)connect
        if conn.ob_sid is not None:
            await conn.send("update_subscription", {"sids": [conn.ob_sid], "action": "add_markets",
                                                    "market_tickers": [t]}, i)
        else:
            # Still subscribing: a plain subscribe gets merged into the subscription.
            await conn.subscribe_tickers([t], i)

    async def remove_market(self, t: str):
        loc = self.where.pop(t, None)
        if loc is None:
            return
        conn = self.conns[loc[0]]
        conn.chunks[loc[1]].remove(t)
        if conn.ob_sid is not None and conn.ws is not None:
            await conn.send("update_subscription", {"sids": [conn.ob_sid], "action": "delete_markets",
                                                    "market_tickers": [t]}, loc[1])

    # -- lifecycle
    async def on_lifecycle(self, msg: dict):
        t, et = msg.get("market_ticker"), msg.get("event_type")
        if not t:
            return
        if et == "activated":
            if mine(t):
                await self.new_tickers.put(t)
        elif et in ("determined", "settled"):
            await self.remove_market(t)

    async def new_market_loop(self):
        """Batch newly activated tickers; check them over REST (applies the
        combo filter) and subscribe within a few seconds of opening."""
        while True:
            batch = [await self.new_tickers.get()]
            await asyncio.sleep(2)
            while not self.new_tickers.empty() and len(batch) < 100:
                batch.append(self.new_tickers.get_nowait())
            try:
                ok = await self.get_markets({"tickers": ",".join(batch)})
                for t in ok:
                    await self.add_market(t)
                if ok:
                    log.info("added %d new markets", len(ok))
            except Exception as e:
                log.warning("new-market lookup failed: %r", e)

    async def refresh_loop(self):
        while True:
            await asyncio.sleep(REFRESH_SECS)
            try:
                missing = [t for t in await self.get_markets({"status": "open"})
                           if mine(t) and t not in self.where]
                for t in missing:
                    await self.add_market(t)
                if missing:
                    log.info("refresh added %d markets", len(missing))
            except Exception as e:
                log.warning("refresh failed: %r", e)

    async def housekeeping_loop(self):
        last_msgs, last_hc = 0, 0.0
        while True:
            await asyncio.sleep(5)
            self.writer.rotate_if_needed()
            now = time.time()
            if now - last_hc >= 60:
                rate = (self.msgs - last_msgs) / max(now - last_hc, 1) if last_hc else 0
                up = sum(c.ws is not None for c in self.conns)
                log.info("msgs=%d (%.0f/s) gaps=%d markets=%d conns_up=%d/%d",
                         self.msgs, rate, self.gaps, len(self.where), up, len(self.conns))
                if HC_URL and up == len(self.conns) and self.msgs > last_msgs:
                    try:
                        async with self.session.get(HC_URL, timeout=aiohttp.ClientTimeout(total=10)):
                            pass
                    except Exception:
                        pass
                last_msgs, last_hc = self.msgs, now

    async def main(self):
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, stop.set)

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as self.session:
            tickers = [t for t in await self.get_markets({"status": "open"}) if mine(t)]
            for t in tickers:
                self._assign(t)
            log.info("shard %d/%d: bootstrapped %d open markets across %d connections",
                     SHARD, N_SHARDS, len(tickers), N_CONN)

            tasks = [asyncio.create_task(c.run()) for c in self.conns]
            tasks += [asyncio.create_task(f()) for f in
                      (self.new_market_loop, self.refresh_loop, self.housekeeping_loop)]
            await stop.wait()
            log.info("shutting down")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.writer.close()


if __name__ == "__main__":
    if uvloop is not None:
        uvloop.run(Collector().main())
    else:
        asyncio.run(Collector().main())
