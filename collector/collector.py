#!/usr/bin/env python3
"""
Kalshi market-data collector.

- Lists every open market over REST, shards the tickers across several
  WebSocket connections, and subscribes to `orderbook_delta` for all of them.
- Subscribes to `trade` and `market_lifecycle_v2` (all markets) on connection 0,
  adds newly created markets the moment they open and drops determined/settled ones.
- Writes every raw message, wrapped with a local receive timestamp, to an
  hourly zstd-compressed JSONL file. Finished hours are moved to
  data/closed/date=YYYY-MM-DD/ for the rclone upload timer.
- Detects per-subscription `seq` gaps, logs them into the data stream, and
  requests fresh snapshots for the affected connection.
- Reconnects (which subscribes everything again) whenever Kalshi ends a
  subscription, and exits if one of its own tasks dies, so systemd restarts it.

Config comes from environment variables (see collector.env.example).
"""
import asyncio
import base64
import logging
import os
import random
import re
import signal
import sys
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
# A market announced by a `created` lifecycle message is subscribed this many seconds
# after its open_ts (whether subscribing before open is accepted is unverified).
OPEN_LAG_SECS = float(env("OPEN_LAG_SECS", "1"))
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
# record the ticker -> market_id mapping. Kalshi's spec marks both `ts` and
# `ts_ms` optional, so `ts` is only dropped from a delta that carries `ts_ms`.
DELTA_STRIP = re.compile(rb',"(?:market_id":"[0-9a-f-]{36}|ts":"[^"]*)"')
DELTA_STRIP_ID = re.compile(rb',"market_id":"[0-9a-f-]{36}"')
# Error codes that mean "not applied, send it again": 10 channel error (answering a
# subscribe), 18 command timeout, 27 too many requests. Sent again after RETRY_SECS,
# doubling each time, MAX_TRIES sends in all.
RETRY_CODES = (10, 18, 27)
RETRY_SECS = 2.0
MAX_TRIES = 5
# A new market that REST does not list yet is looked up again after these many seconds
# (Kalshi: a market may not be queryable right after its `created` event).
LOOKUP_RETRY_SECS = (5, 20, 60)
FIRST_REFRESH_SECS = 20   # the first rescan after start-up; then every REFRESH_SECS
STABLE_SECS = 30          # a connection that lasted this long resets the reconnect backoff

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s %(levelname)s [s{SHARD}] %(message)s")
log = logging.getLogger("collector")


def parse_ts(s: str) -> float | None:
    """A REST timestamp such as '2026-11-17T06:50:00Z' as unix seconds; None if absent or unreadable."""
    try:
        dt = datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


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
    """Streams lines into data/open/kalshi_<YYYYmmddTHH>_<start>_s<shard>.jsonl.zst and,
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
        # command id -> (cmd, params, times sent before): what an `error` reply refers to
        self.pending: dict[int, tuple[str, dict, int]] = {}
        self.last_seq: dict[int, int] = {}
        self.resnap_task: asyncio.Task | None = None
        self.resnap_again = False                  # a gap arrived that a running pass may predate
        self.retries: set[asyncio.Task] = set()    # commands waiting to be sent again
        self.ws = None
        self.next_id = 1
        self.sub_pos = 0          # chunks subscribe_all has sent on this connection so far
        self.subscribed = False   # subscribe_all has sent every chunk
        self.closing = False      # reconnect() was called on this connection
        self.degraded = False     # the trade or lifecycle channel could not be subscribed

    def total(self) -> int:
        return sum(len(ch) for ch in self.chunks)

    def ready(self) -> bool:
        """Connected, with everything subscribed. This is what `conns_up` counts, and so
        what the deploy script and the healthcheck ping wait for."""
        return (self.ws is not None and self.subscribed and not self.degraded
                and (self.ob_sid is not None or not self.total()))

    async def send(self, cmd: str, params: dict, tries: int = 0, remember: dict | None = None) -> bool:
        """Send one command. False if it could not be sent because the connection is down
        or closing: everything in `chunks` is subscribed again once it is back."""
        ws = self.ws
        if ws is None:
            return False
        cid = self.next_id
        self.next_id += 1
        self.pending[cid] = (cmd, params if remember is None else remember, tries)
        try:
            await ws.send(orjson.dumps({"id": cid, "cmd": cmd, "params": params}).decode())
        except websockets.ConnectionClosed:
            self.pending.pop(cid, None)
            return False
        return True

    async def subscribe_tickers(self, tickers: list[str]):
        if tickers:
            await self.send("subscribe", {"channels": ["orderbook_delta"], "market_tickers": list(tickers)})

    async def subscribe_all(self):
        try:
            if self.idx == 0:
                await self.send("subscribe", {"channels": ["market_lifecycle_v2"]})
                if SHARD == 0:
                    await self.send("subscribe", {"channels": ["trade"]})
            while self.sub_pos < len(self.chunks):  # len() each time: chunks can be appended meanwhile
                chunk = self.chunks[self.sub_pos]
                self.sub_pos += 1                   # from here on, additions to it go through add_tickers()
                if chunk:
                    await self.subscribe_tickers(chunk)
                    await asyncio.sleep(0.2)        # stay well under the command rate limit
            self.subscribed = True
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("conn %d: subscribing failed; reconnecting", self.idx)
            await self.reconnect()

    async def add_tickers(self, tickers: list[str]):
        """Subscribe tickers that are already in `chunks`, CHUNK per command."""
        for k in range(0, len(tickers), CHUNK):
            if k:
                await asyncio.sleep(0.2)
            part = tickers[k:k + CHUNK]
            if self.ob_sid is not None:
                await self.send("update_subscription", {"sids": [self.ob_sid], "action": "add_markets",
                                                        "market_tickers": part})
            else:
                # Still subscribing: a plain subscribe gets merged into the subscription.
                await self.subscribe_tickers(part)

    async def resend(self, cmd: str, params: dict, tries: int):
        """Send a command again after Kalshi answered it with one of RETRY_CODES."""
        ws = self.ws
        await asyncio.sleep(min(RETRY_SECS * 2 ** tries, 30) + random.random() * RETRY_SECS / 2)
        if ws is None or self.ws is not ws:
            return  # reconnected meanwhile, which subscribed everything again
        if "market_tickers" in params:  # leave out markets that were determined meanwhile
            here = [t for t in params["market_tickers"] if self.c.where.get(t, (-1,))[0] == self.idx]
            if not here:
                return
            params = dict(params, market_tickers=here)
        await self.send(cmd, params, tries + 1)

    async def reconnect(self):
        """Close the socket: run() then connects again and subscribes everything from scratch."""
        ws = self.ws
        if ws is not None and not self.closing:
            self.closing = True
            await ws.close()

    async def resnapshot(self):
        """Fresh snapshots for every market on this connection, in chunks. A gap that
        arrives while a pass is running starts another pass afterwards: the chunks
        already requested got their snapshots before that gap."""
        while self.resnap_again:
            self.resnap_again = False
            for ch in list(self.chunks):
                if self.ws is None or self.ob_sid is None:
                    return
                if ch:
                    await self.send("update_subscription", {"sids": [self.ob_sid], "action": "get_snapshot",
                                                            "market_tickers": list(ch)},
                                    remember={"action": "get_snapshot"})
                    await asyncio.sleep(0.2)

    async def run(self):
        backoff = 1.0
        while True:
            sub_task = up_since = None
            try:
                async with websockets.connect(
                    WS_URL,
                    additional_headers=sign_headers("GET", WS_PATH),
                    max_size=None, ping_interval=20, ping_timeout=30, close_timeout=5,
                    compression=None,  # no permessage-deflate: inbound bandwidth is free, CPU isn't
                ) as ws:
                    self.ws = ws
                    self.ob_sid = None
                    self.sub_pos = 0
                    self.subscribed = self.closing = self.degraded = self.resnap_again = False
                    self.last_seq.clear()
                    self.pending.clear()
                    up_since = time.monotonic()
                    log.info("conn %d connected (%d markets in %d chunks)",
                             self.idx, self.total(), len(self.chunks))
                    self.c.event({"type": "collector_connect", "conn": self.idx})
                    sub_task = asyncio.create_task(self.subscribe_all())
                    while True:
                        # decode=False: raw bytes, skipping a UTF-8 decode/encode round trip
                        await self.handle(await ws.recv(decode=False))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # network errors, handshake failures, closes
                log.warning("conn %d dropped: %r", self.idx, e)
            finally:
                self.ws = None
                self.subscribed = False
                for task in (sub_task, self.resnap_task, *self.retries):
                    if task:
                        task.cancel()
                self.resnap_task = None
            self.c.event({"type": "collector_disconnect", "conn": self.idx})
            # Back to the shortest wait only after a connection that held for a while: one
            # that is accepted and dropped again right away must not be retried in a tight loop.
            if up_since is not None and time.monotonic() - up_since >= STABLE_SECS:
                backoff = 1.0
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)

    def check_seq(self, sid: int, seq: int):
        """Every message carrying sid+seq counts, including "ok" responses."""
        last = self.last_seq.get(sid)
        if last is not None and seq != last + 1:
            self.c.gaps += 1
            self.c.event({"type": "collector_gap", "conn": self.idx, "sid": sid,
                          "expected": last + 1, "got": seq})
            if sid == self.ob_sid:
                self.resnap_again = True
                if self.resnap_task is None or self.resnap_task.done():
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
                data = (DELTA_STRIP if b'"ts_ms":' in data else DELTA_STRIP_ID).sub(b"", data)
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
        elif t == "ok":
            self.pending.pop(m.get("id"), None)
        elif t == "unsubscribed":
            # We never unsubscribe, so Kalshi ended this subscription (it follows a terminal
            # error: 25 buffer overflow, 10 channel error). The connection stays open and
            # would look healthy while that subscription's data has silently stopped.
            if not self.closing:
                log.error("conn %d: subscription %s was ended by Kalshi; reconnecting", self.idx, sid)
            await self.reconnect()
        elif t == "error":
            await self.on_error(m, body)
        elif t == "market_lifecycle_v2":
            self.c.on_lifecycle(body)

    async def on_error(self, m: dict, body: dict):
        sent = self.pending.pop(m.get("id"), None)
        if self.closing:
            return  # replies to commands that were in flight when the subscription ended
        cmd, params, tries = sent or ("", {}, 0)
        code = body.get("code")
        log.error("conn %d error: %s%s", self.idx, body,
                  f" (answering {cmd} {params.get('action') or params.get('channels')})" if sent else "")
        ended = code == 25 or (m.get("sid") is not None and m.get("id") is None)
        if ended or code == 7 or params.get("action") == "get_snapshot":
            # Kalshi ended a subscription (25 buffer overflow, 10 channel error: sent with
            # the subscription's sid and no command id), no longer knows our sid (7), or
            # could not resynchronise after a gap. Only subscribing again brings the data back.
            await self.reconnect()
        elif sent and code in RETRY_CODES and tries + 1 < MAX_TRIES:
            task = asyncio.create_task(self.resend(cmd, params, tries))
            self.retries.add(task)
            task.add_done_callback(self.retries.discard)
        elif "market_tickers" in params:
            # These markets are not being recorded. Forget them, so the next rescan adds them again.
            self.c.forget(self.idx, params["market_tickers"])
        elif cmd == "subscribe":
            # No trade / lifecycle feed on this connection. It stops counting as up, so the
            # healthcheck alerts (and a deploy that caused it is rolled back).
            self.degraded = True


# ---------------------------------------------------------------- collector
class Collector:
    def __init__(self):
        self.writer = HourlyWriter(DATA_DIR)
        self.conns = [Conn(i, self) for i in range(N_CONN)]
        self.where: dict[str, tuple[int, int]] = {}  # ticker -> (conn, chunk)
        self.new_tickers: asyncio.Queue[str] = asyncio.Queue()
        # created, not open yet: ticker -> (timer that queues it when it opens, its open time)
        self.opening: dict[str, tuple[asyncio.TimerHandle, float]] = {}
        self.lookup_tries: dict[str, int] = {}  # new tickers REST did not list yet -> lookups so far
        self.rest_slots = asyncio.Semaphore(4)   # concurrent new-market lookups
        self.msgs = 0
        self.gaps = 0
        self.session: aiohttp.ClientSession | None = None

    def event(self, ev: dict):
        """Collector-generated records go in the same stream with conn = -1."""
        self.writer.write(b'{"recv_ns":%d,"conn":-1,"msg":%s}\n'
                          % (time.time_ns(), orjson.dumps(ev)))

    # -- REST
    async def get_markets(self, params: dict) -> dict[str, str]:
        """Every market matching params, all pages, as {ticker: open_time}."""
        found, cursor = {}, ""
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
                data = orjson.loads(await r.read())  # ~2 MB a page: orjson stalls the event loop least
            for mk in data.get("markets", []):
                found[mk["ticker"]] = mk.get("open_time") or ""
            cursor = data.get("cursor") or ""
            if not cursor:
                return found
            await asyncio.sleep(0.1)

    async def lookup(self, tickers: list[str]) -> dict[str, str]:
        async with self.rest_slots:
            return await self.get_markets({"tickers": ",".join(tickers)})

    async def scan(self) -> int:
        """One pass over REST; returns how many markets it added. Markets that have not
        opened yet are scheduled for their open time, which is also what restores the
        schedule after a restart. Open markets that are not being recorded are added.
        Unopened first: a market that opens between the two listings is in one of them."""
        for t, open_time in (await self.get_markets({"status": "unopened"})).items():
            open_ts = parse_ts(open_time)
            if open_ts is not None and mine(t):
                self.schedule(t, open_ts)
        missing = [t for t in await self.get_markets({"status": "open"})
                   if mine(t) and t not in self.where]
        return await self.add_markets(missing)

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

    async def add_markets(self, tickers) -> int:
        """Start recording tickers; returns how many were new. Each connection gets its
        share in one command, not one command per ticker: Kalshi answers every add with
        the subscription's whole ticker list (~800 KB), and a rescan that added 1,500
        markets one by one made it drop the subscriptions with buffer overflows."""
        share: dict[int, list[str]] = {}
        n = 0
        for t in tickers:
            if t in self.where:
                continue
            conn, i = self._assign(t)
            n += 1
            # Otherwise subscribe_all includes it: on (re)connect, or when it reaches that chunk.
            if conn.ws is not None and (conn.subscribed or i < conn.sub_pos):
                share.setdefault(conn.idx, []).append(t)
        results = await asyncio.gather(*(self.conns[i].add_tickers(ts) for i, ts in share.items()),
                                       return_exceptions=True)
        for (i, ts), res in zip(share.items(), results):
            if isinstance(res, Exception):
                log.error("conn %d: adding %d markets failed: %r", i, len(ts), res)
                self.forget(i, ts)
        return n

    def remove_market(self, t: str):
        """Stop tracking a determined market. Nothing is sent: Kalshi takes a determined
        market out of the subscription itself (after a last, empty snapshot), so a
        delete_markets command changed nothing and only cost another ~800 KB reply."""
        loc = self.where.pop(t, None)
        if loc is not None:
            self.conns[loc[0]].chunks[loc[1]].remove(t)

    def forget(self, conn_idx: int, tickers):
        """Tickers a connection failed to subscribe: drop them, so a rescan tries again."""
        for t in tickers:
            if self.where.get(t, (-1,))[0] == conn_idx:
                self.remove_market(t)

    # -- lifecycle
    def schedule(self, t: str, open_ts: float):
        """Queue market t for subscribing just after it opens (right away if it already has)."""
        if t in self.where:
            return
        old = self.opening.get(t)
        if old is not None:
            if old[1] == open_ts:
                return
            self.unschedule(t)  # its opening time changed
        delay = open_ts + OPEN_LAG_SECS - time.time()
        if delay > 0:
            self.opening[t] = (asyncio.get_running_loop().call_later(delay, self._opened, t), open_ts)
        else:
            self.new_tickers.put_nowait(t)

    def unschedule(self, t: str):
        old = self.opening.pop(t, None)
        if old is not None:
            old[0].cancel()

    def _opened(self, t: str):
        self.opening.pop(t, None)
        self.new_tickers.put_nowait(t)

    def on_lifecycle(self, msg: dict):
        """New markets arrive as `created`, usually hours before they open (open_ts), and
        nothing more is sent when they open, so the add is scheduled for just after open_ts.
        `activated` means a paused or closed market trades again: add it now if it is not
        being recorded."""
        t, et = msg.get("market_ticker"), msg.get("event_type")
        if not t or not mine(t):
            return
        if et == "created" and isinstance(msg.get("open_ts"), (int, float)):
            self.schedule(t, msg["open_ts"])
        elif et in ("created", "activated"):
            if t not in self.where:
                self.unschedule(t)
                self.new_tickers.put_nowait(t)
        elif et in ("determined", "settled"):
            self.unschedule(t)
            self.remove_market(t)

    def lookup_later(self, t: str):
        n = self.lookup_tries.get(t, 0)
        if n < len(LOOKUP_RETRY_SECS):
            self.lookup_tries[t] = n + 1
            asyncio.get_running_loop().call_later(LOOKUP_RETRY_SECS[n], self.new_tickers.put_nowait, t)
        else:
            self.lookup_tries.pop(t, None)  # give up: a rescan adds it once REST lists it as open

    async def add_new(self, tickers: list[str]):
        """Check new tickers over REST, 100 per request (it leaves out combo markets and
        gives their current open time), then subscribe them."""
        todo = [t for t in tickers if t not in self.where]
        pages = [todo[k:k + 100] for k in range(0, len(todo), 100)]
        found: dict[str, str] = {}
        for res in await asyncio.gather(*map(self.lookup, pages), return_exceptions=True):
            if isinstance(res, Exception):
                log.warning("new-market lookup failed: %r", res)
            else:
                found.update(res)
        now, add = time.time(), []
        for t in todo:
            if t not in found:
                self.lookup_later(t)  # lookup failed, or REST does not list it yet
                continue
            self.lookup_tries.pop(t, None)
            open_ts = parse_ts(found[t])
            if open_ts is not None and open_ts > now:
                self.schedule(t, open_ts)  # its opening was moved back
            else:
                add.append(t)
        n = await self.add_markets(add)
        if n:
            log.info("added %d new markets", n)

    async def new_market_loop(self):
        """Markets that just opened: collect them for 2 s, so that the thousands opening at
        the top of an hour become a few commands, and subscribe them."""
        while True:
            batch = {await self.new_tickers.get(): None}
            await asyncio.sleep(2)
            while not self.new_tickers.empty():
                batch[self.new_tickers.get_nowait()] = None
            try:
                await self.add_new(list(batch))
            except Exception:  # the rescan picks these markets up
                log.exception("adding new markets failed")

    async def refresh_loop(self):
        # The first pass comes right after start-up: it restores the opening schedule and
        # catches markets that opened or were created while the collector was starting.
        delay = min(REFRESH_SECS, FIRST_REFRESH_SECS)
        while True:
            await asyncio.sleep(delay)
            delay = REFRESH_SECS
            try:
                n = await self.scan()
                if n:
                    log.info("refresh added %d markets", n)
            except Exception as e:
                log.warning("refresh failed: %r", e)

    async def housekeeping_loop(self):
        last_msgs, last_hc, last_up = 0, 0.0, 0
        while True:
            await asyncio.sleep(5)
            self.writer.rotate_if_needed()
            now = time.time()
            up = sum(c.ready() for c in self.conns)
            if up != last_up:
                # Logged as soon as it changes: deploy.sh waits for conns_up=N/N after a restart.
                log.info("conns_up=%d/%d", up, len(self.conns))
                last_up = up
            if now - last_hc >= 60:
                rate = (self.msgs - last_msgs) / max(now - last_hc, 1) if last_hc else 0
                log.info("msgs=%d (%.0f/s) gaps=%d markets=%d conns_up=%d/%d",
                         self.msgs, rate, self.gaps, len(self.where), up, len(self.conns))
                if HC_URL and up == len(self.conns) and self.msgs > last_msgs:
                    try:
                        async with self.session.get(HC_URL, timeout=aiohttp.ClientTimeout(total=10)):
                            pass
                    except Exception:
                        pass
                last_msgs, last_hc = self.msgs, now

    async def main(self) -> int:
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

            tasks = [asyncio.create_task(c.run(), name=f"conn {c.idx}") for c in self.conns]
            tasks += [asyncio.create_task(f(), name=f.__name__) for f in
                      (self.new_market_loop, self.refresh_loop, self.housekeeping_loop)]
            stopping = asyncio.create_task(stop.wait())
            done, _ = await asyncio.wait([stopping, *tasks], return_when=asyncio.FIRST_COMPLETED)
            # None of these tasks is meant to end. If one did, exit with an error: systemd
            # restarts the collector, which beats running on without a connection or a loop.
            died = [t for t in done if t is not stopping]
            for t in died:
                log.critical("%s ended unexpectedly: %r", t.get_name(),
                             "cancelled" if t.cancelled() else t.exception())
            log.info("shutting down")
            stopping.cancel()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.writer.close()
        return 1 if died else 0


if __name__ == "__main__":
    sys.exit((uvloop.run if uvloop is not None else asyncio.run)(Collector().main()))
