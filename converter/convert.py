#!/usr/bin/env python3
"""
Convert raw Kalshi capture files (.jsonl.zst from collector.py) to Parquet.

Works one UTC hour at a time:
  1. list raw files under --raw, group them by hour
  2. skip hours that are too recent, or already converted from the same inputs
  3. download the hour, load it with DuckDB, write four Parquet files:
       deltas     orderbook_delta rows, sorted by (market_ticker, recv_ns, seq)
       snapshots  orderbook_snapshot rows, one row per snapshot, levels as lists
       trades     public trades
       events     everything else (lifecycle, subscribe acks, errors, and the
                  collector's own connect/disconnect/gap records) as raw JSON
  4. verify (row counts add up, required fields present, fixed-point exact)
  5. upload the Parquet, then a manifest that marks the hour as done

Prices are integers in 1/10000 dollar (price_e4: 2000 = $0.20) and sizes are
integers in 1/100 contract (delta_e2 / size_e2 / count_e2: 14800 = 148.00).

--raw and --out are either local directories or rclone paths (b2:bucket/raw).
Raw files are never modified or deleted by this script.
"""
import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

VERSION = 1
RAW_RE = re.compile(r"^kalshi_(\d{8})T(\d{2})_(?:s(\d+)_)?(\d+)\.jsonl\.zst$")
TABLES = ("deltas", "snapshots", "trades", "events")

# Fields this script carries into typed columns. Anything else Kalshi adds to
# these message types is reported as an unknown key (and listed in the manifest).
KNOWN_KEYS = {
    "orderbook_delta": {"market_ticker", "market_id", "clock", "price_dollars", "delta_fp", "side",
                        "client_order_id", "subaccount", "ts", "ts_ms"},
    "orderbook_snapshot": {"market_ticker", "market_id", "clock", "yes_dollars_fp", "no_dollars_fp"},
    "trade": {"trade_id", "market_ticker", "yes_price_dollars", "no_price_dollars", "count_fp",
              "taker_side", "taker_outcome_side", "taker_book_side", "is_block_trade", "ts", "ts_ms"},
}

ENVELOPE = json.dumps({
    "type": "VARCHAR", "sid": "INTEGER", "seq": "BIGINT", "sending_ts_ms": "BIGINT",
    "msg": {
        "market_ticker": "VARCHAR", "market_id": "VARCHAR", "clock": "BIGINT",
        "price_dollars": "VARCHAR", "delta_fp": "VARCHAR", "side": "VARCHAR",
        "client_order_id": "VARCHAR", "subaccount": "INTEGER",
        "ts": "VARCHAR", "ts_ms": "BIGINT",
        "yes_dollars_fp": [["VARCHAR"]], "no_dollars_fp": [["VARCHAR"]],
        "trade_id": "VARCHAR", "yes_price_dollars": "VARCHAR", "no_price_dollars": "VARCHAR",
        "count_fp": "VARCHAR", "taker_side": "VARCHAR", "taker_outcome_side": "VARCHAR",
        "taker_book_side": "VARCHAR", "is_block_trade": "BOOLEAN",
    },
})

log = logging.getLogger("convert")


class CheckFailed(Exception):
    pass


# ---------------------------------------------------------------- storage
def is_local(root: str) -> bool:
    if re.match(r"^[A-Za-z]:[\\/]", root):   # Windows drive path
        return True
    return ":" not in root                    # rclone remotes look like name:path


class Store:
    """A directory tree, either local or behind rclone."""

    def __init__(self, root: str):
        self.local = is_local(root)
        self.root = str(Path(root).expanduser()) if self.local else root.rstrip("/")

    def _rclone(self, *args: str, ok_codes=(0,)) -> tuple[int, str]:
        cmd = ["rclone", *args]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        except FileNotFoundError:
            raise SystemExit("rclone is not installed or not on PATH") from None
        if p.returncode not in ok_codes:
            raise RuntimeError(f"{' '.join(cmd)} failed ({p.returncode}): {p.stderr.strip()[-2000:]}")
        return p.returncode, p.stdout

    stall_seconds = 600   # kill a transfer whose byte count hasn't moved for this long
    attempts = 3

    def _transfer(self, *args: str):
        """rclone copy with progress in the log, and a watchdog: a transfer that stops
        moving bytes is killed and retried (rclone itself skips files already copied)."""
        cmd = ["rclone", *args, "--stats", "30s", "--stats-one-line", "--stats-log-level", "NOTICE",
               "--timeout", "2m", "--contimeout", "60s", "--low-level-retries", "10"]
        for attempt in range(1, self.attempts + 1):
            try:
                p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                     text=True, encoding="utf-8", errors="replace")
            except FileNotFoundError:
                raise SystemExit("rclone is not installed or not on PATH") from None
            state = {"bytes": None, "moved": time.time(), "last_log": 0.0, "tail": []}

            def read():
                for line in p.stderr:
                    line = line.strip()
                    state["tail"] = (state["tail"] + [line])[-20:]
                    m = re.search(r"Transferred:\s*([^/,]+)/", line) or re.search(r"\s([\d.]+ ?\w?i?B) /", line)
                    if m:
                        if m.group(1) != state["bytes"]:
                            state["bytes"], state["moved"] = m.group(1), time.time()
                        if time.time() - state["last_log"] >= 60:
                            state["last_log"] = time.time()
                            log.info("    rclone: %s", line.split("NOTICE:")[-1].strip()[:160])

            reader = threading.Thread(target=read, daemon=True)
            reader.start()
            stalled = False
            while p.poll() is None:
                time.sleep(5)
                # Only once rclone's progress lines are being parsed: never kill a transfer blind.
                if state["bytes"] is not None and time.time() - state["moved"] > self.stall_seconds:
                    stalled = True
                    p.kill()
                    p.wait()
                    break
            reader.join(timeout=10)
            if stalled:
                log.warning("rclone made no progress for %.0f min (attempt %d/%d); restarting it",
                            self.stall_seconds / 60, attempt, self.attempts)
                continue
            if p.returncode == 0:
                return
            log.warning("rclone exited %d (attempt %d/%d): %s", p.returncode, attempt, self.attempts,
                        " | ".join(state["tail"][-3:])[-500:])
        raise RuntimeError(f"rclone copy failed after {self.attempts} attempts: {' '.join(args[:3])}")

    def _join(self, sub: str) -> str:
        return f"{self.root}/{sub}" if sub else self.root

    def ls(self, sub: str = "") -> list[dict]:
        """Files under sub as [{'path': 'relative/to/sub', 'size': n}]; [] if sub is missing."""
        if self.local:
            base = Path(self._join(sub))
            if not base.is_dir():
                return []
            return [{"path": p.relative_to(base).as_posix(), "size": p.stat().st_size}
                    for p in base.rglob("*") if p.is_file()]
        # exit code 3 = directory not found
        code, out = self._rclone("lsjson", "-R", "--files-only", "--no-modtime", "--no-mimetype",
                                 self._join(sub), ok_codes=(0, 3))
        if code == 3:
            return []
        return [{"path": e["Path"], "size": e["Size"]} for e in json.loads(out or "[]")]

    def download(self, sub: str, rels: list[str], dest: Path):
        dest.mkdir(parents=True, exist_ok=True)
        if self.local:
            for r in rels:
                (dest / r).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(Path(self._join(sub)) / r, dest / r)
            return
        lst = dest / ".files-from.txt"
        lst.write_bytes(("\n".join(rels) + "\n").encode())  # bytes: no CRLF on Windows
        self._transfer("copy", self._join(sub), str(dest), "--files-from-raw", str(lst),
                       "--transfers", "4", "--checksum")
        lst.unlink()

    def upload_tree(self, src: Path, sub: str = ""):
        if self.local:
            shutil.copytree(src, Path(self._join(sub)), dirs_exist_ok=True)
        else:
            self._transfer("copy", str(src), self._join(sub), "--transfers", "4", "--checksum")

    def delete(self, rel: str):
        if self.local:
            Path(self._join(rel)).unlink(missing_ok=True)
        else:
            self._rclone("deletefile", self._join(rel))

    def mirror_down(self, sub: str, dest: Path):
        """Make dest an exact copy of sub (small files only: used for manifests)."""
        if self.local:
            if dest.exists():
                shutil.rmtree(dest)
            src = Path(self._join(sub))
            if src.is_dir():
                shutil.copytree(src, dest)
            else:
                dest.mkdir(parents=True)
            return
        dest.mkdir(parents=True, exist_ok=True)
        code, _ = self._rclone("sync", self._join(sub), str(dest), "--checksum", ok_codes=(0, 3))
        if code == 3:  # nothing converted yet on the remote
            shutil.rmtree(dest)
            dest.mkdir(parents=True)


# ---------------------------------------------------------------- helpers
def q(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


RECORD_START = b'\n{"recv_ns":'


def repair_zst(src: Path, dst: Path) -> bool:
    """Rewrite a .zst that may be cut off (collector crash): keep every complete
    record that can still be decompressed. Returns True if anything was dropped.
    A record may span two lines (Kalshi messages end with a newline), so the
    cut is made where the next record starts, not at the last newline."""
    import zstandard as zstd
    damaged = False
    tail = b""
    dobj = zstd.ZstdDecompressor().decompressobj()
    with open(src, "rb") as fi, open(dst, "wb") as fo:
        writer = zstd.ZstdCompressor(level=1).stream_writer(fo)
        while not dobj.eof:
            block = fi.read(1 << 20)
            if not block:
                break
            try:
                chunk = dobj.decompress(block)
            except zstd.ZstdError:
                damaged = True
                break
            buf = tail + chunk
            cut = buf.rfind(RECORD_START) + 1   # 0 when no boundary yet
            writer.write(buf[:cut])
            tail = buf[cut:]
        if tail.strip():
            try:
                json.loads(tail)                 # last record: keep only if complete
                writer.write(tail if tail.endswith(b"\n") else tail + b"\n")
            except ValueError:
                damaged = True
        writer.close()
    # Cut-off files end without the zstd end-of-frame marker, or mid-record.
    return damaged or not dobj.eof


# Decimal strings are parsed as DECIMAL(38,8): 30 digits before the point, so a
# huge order size (e.g. 111,111,111.00 contracts) can't overflow the arithmetic.
def e4(expr: str) -> str:   # dollars string -> integer 1/10000 dollar
    return f"CAST(CAST({expr} AS DECIMAL(38,8)) * 10000 AS SMALLINT)"


def e2(expr: str) -> str:   # contracts string -> integer 1/100 contract
    return f"CAST(CAST({expr} AS DECIMAL(38,8)) * 100 AS BIGINT)"


def inexact(expr: str, scale: int) -> str:
    """True when the string is not a number, or has more decimals than the integer column keeps."""
    return (f"coalesce((TRY_CAST({expr} AS DECIMAL(38,8)) * {scale}) % 1 <> 0, true)")


# ---------------------------------------------------------------- one hour
def load_stage(con, files: list[Path]):
    flist = "[" + ",".join(q(f.as_posix()) for f in files) + "]"
    con.execute(f"""
        CREATE OR REPLACE TABLE stage AS
        SELECT recv_ns, conn,
               coalesce(CAST(nullif(regexp_extract(filename, '_s(\\d+)_\\d+\\.jsonl', 1), '') AS TINYINT), 0) AS shard,
               t.type AS type, t.sid AS sid, t.seq AS seq, t.sending_ts_ms AS sending_ts_ms, t.msg AS m,
               CASE WHEN t.type IN ('orderbook_delta', 'orderbook_snapshot', 'trade') THEN NULL
                    -- Kalshi's "ok" to a subscription change echoes every ticker in the
                    -- subscription (~800 KB with 26k markets). Keep only the count: the
                    -- list is redundant, and hundreds of them an hour exhaust DuckDB's memory.
                    WHEN t.type = 'ok' AND json_exists(msg, '$.msg.market_tickers') THEN
                         json_merge_patch(msg, json_object('msg', json_object(
                             'market_tickers', NULL,
                             'market_tickers_count', json_array_length(msg, '$.msg.market_tickers'))))::VARCHAR
                    ELSE CAST(msg AS VARCHAR) END AS raw,
               CASE WHEN recv_ns % 499 = 0 AND t.type IN ('orderbook_delta', 'orderbook_snapshot', 'trade')
                    THEN json_keys(msg, '$.msg') END AS sample_keys
        FROM (
            SELECT recv_ns, conn, msg, filename, json_transform(msg, {q(ENVELOPE)}) AS t
            FROM read_json({flist}, format = 'unstructured', filename = true,
                           maximum_object_size = 16777216,
                           columns = {{'recv_ns': 'BIGINT', 'conn': 'TINYINT', 'msg': 'JSON'}})
        )
    """)


def run_checks(con) -> dict:
    """Raise CheckFailed if anything would be lost or mangled; return stats."""
    problems = []

    def bad(label: str, sql: str):
        n = con.execute(sql).fetchone()[0]
        if n:
            problems.append(f"{label}: {n} rows")

    bad("line without recv_ns/conn/type",
        "SELECT count(*) FROM stage WHERE recv_ns IS NULL OR conn IS NULL OR type IS NULL")
    bad("delta missing a required field", """
        SELECT count(*) FROM stage WHERE type = 'orderbook_delta' AND (
            sid IS NULL OR seq IS NULL OR m.market_ticker IS NULL OR m.clock IS NULL
            OR m.price_dollars IS NULL OR m.delta_fp IS NULL OR m.side NOT IN ('yes', 'no')
            OR m.side IS NULL)""")
    bad("delta price/size not exact at 4/2 decimals", f"""
        SELECT count(*) FROM stage WHERE type = 'orderbook_delta' AND (
            {inexact('m.price_dollars', 10000)} OR {inexact('m.delta_fp', 100)}
            OR TRY_CAST(m.price_dollars AS DECIMAL(38,8)) NOT BETWEEN 0 AND 3)""")
    bad("delta with an unreadable ts string", """
        SELECT count(*) FROM stage WHERE type = 'orderbook_delta' AND m.ts_ms IS NULL
            AND m.ts IS NOT NULL AND TRY_CAST(m.ts AS TIMESTAMPTZ) IS NULL""")
    bad("snapshot missing a required field", """
        SELECT count(*) FROM stage WHERE type = 'orderbook_snapshot' AND (
            sid IS NULL OR seq IS NULL OR m.market_ticker IS NULL OR m.clock IS NULL)""")
    bad("snapshot level malformed or not exact", f"""
        SELECT count(*) FROM stage WHERE type = 'orderbook_snapshot' AND len(list_filter(
            list_concat(coalesce(m.yes_dollars_fp, []), coalesce(m.no_dollars_fp, [])),
            lambda x: len(x) <> 2 OR x[1] IS NULL OR x[2] IS NULL
                      OR {inexact('x[1]', 10000)} OR {inexact('x[2]', 100)}
                      OR TRY_CAST(x[1] AS DECIMAL(38,8)) NOT BETWEEN 0 AND 3)) > 0""")
    bad("trade missing a required field", """
        SELECT count(*) FROM stage WHERE type = 'trade' AND (
            m.trade_id IS NULL OR m.market_ticker IS NULL OR m.yes_price_dollars IS NULL
            OR m.no_price_dollars IS NULL OR m.count_fp IS NULL)""")
    bad("trade price/size not exact at 4/2 decimals or price out of range", f"""
        SELECT count(*) FROM stage WHERE type = 'trade' AND (
            {inexact('m.yes_price_dollars', 10000)} OR {inexact('m.no_price_dollars', 10000)}
            OR {inexact('m.count_fp', 100)}
            OR TRY_CAST(m.yes_price_dollars AS DECIMAL(38,8)) NOT BETWEEN 0 AND 3
            OR TRY_CAST(m.no_price_dollars AS DECIMAL(38,8)) NOT BETWEEN 0 AND 3)""")
    if problems:
        raise CheckFailed("; ".join(problems))

    unknown = {}
    for typ, key in con.execute("""
            SELECT DISTINCT type, unnest(sample_keys) FROM stage WHERE sample_keys IS NOT NULL""").fetchall():
        if key not in KNOWN_KEYS[typ]:
            unknown.setdefault(typ, []).append(key)
    no_ts = con.execute("""SELECT count(*) FROM stage
                           WHERE type = 'orderbook_delta' AND m.ts_ms IS NULL AND m.ts IS NULL""").fetchone()[0]
    return {"unknown_keys": unknown, "deltas_without_exchange_ts": no_ts}


def write_tables(con, out_dir: Path, date: str, stamp: str, zstd_level: int, rows_per_file: int) -> dict:
    """Write the four tables; return {"rows": counts, "files": relative paths written}.

    A big table is split into several files per hour so each sort fits in memory:
    part b holds the markets with hash(market_ticker) % parts == b, sorted by
    (market_ticker, recv_ns, seq). A market's rows for the hour are always in
    exactly one file."""
    opts = (f"(FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL {zstd_level}, "
            f"ROW_GROUP_SIZE 200000, PARQUET_VERSION v2)")
    # table -> (columns, stage filter, sort order, split by market?)
    specs = {
        "deltas": (f"""recv_ns, shard, conn, sid, seq,
                   m.market_ticker AS market_ticker, m.clock AS clock, m.side AS side,
                   {e4('m.price_dollars')} AS price_e4, {e2('m.delta_fp')} AS delta_e2,
                   coalesce(m.ts_ms, epoch_ms(TRY_CAST(m.ts AS TIMESTAMPTZ))) AS ts_ms,
                   sending_ts_ms, m.client_order_id AS client_order_id, m.subaccount AS subaccount""",
                   "type = 'orderbook_delta'", "market_ticker, recv_ns, seq", True),
        "snapshots": (f"""recv_ns, shard, conn, sid, seq,
                   m.market_ticker AS market_ticker, m.market_id AS market_id, m.clock AS clock,
                   coalesce(list_transform(m.yes_dollars_fp, lambda x: {e4('x[1]')}), CAST([] AS SMALLINT[])) AS yes_price_e4,
                   coalesce(list_transform(m.yes_dollars_fp, lambda x: {e2('x[2]')}), CAST([] AS BIGINT[])) AS yes_size_e2,
                   coalesce(list_transform(m.no_dollars_fp, lambda x: {e4('x[1]')}), CAST([] AS SMALLINT[])) AS no_price_e4,
                   coalesce(list_transform(m.no_dollars_fp, lambda x: {e2('x[2]')}), CAST([] AS BIGINT[])) AS no_size_e2,
                   sending_ts_ms""",
                   "type = 'orderbook_snapshot'", "market_ticker, recv_ns, seq", True),
        "trades": (f"""recv_ns, shard, conn, sid, seq,
                   m.trade_id AS trade_id, m.market_ticker AS market_ticker,
                   {e4('m.yes_price_dollars')} AS yes_price_e4, {e4('m.no_price_dollars')} AS no_price_e4,
                   {e2('m.count_fp')} AS count_e2,
                   m.taker_side AS taker_side, m.taker_outcome_side AS taker_outcome_side,
                   m.taker_book_side AS taker_book_side, m.is_block_trade AS is_block_trade,
                   coalesce(m.ts_ms, TRY_CAST(m.ts AS BIGINT) * 1000) AS ts_ms, sending_ts_ms""",
                   "type = 'trade'", "market_ticker, recv_ns, seq", True),
        "events": ("recv_ns, shard, conn, type, sid, seq, raw",
                   "type NOT IN ('orderbook_delta', 'orderbook_snapshot', 'trade')",
                   "recv_ns, shard, conn", False),
    }
    wanted = dict(con.execute("""
        SELECT CASE type WHEN 'orderbook_delta' THEN 'deltas' WHEN 'orderbook_snapshot' THEN 'snapshots'
                         WHEN 'trade' THEN 'trades' ELSE 'events' END AS t, count(*)
        FROM stage GROUP BY t""").fetchall())
    rows, files = {}, []
    for table in TABLES:
        n = wanted.get(table, 0)
        rows[table] = n
        if n == 0:
            continue
        cols, where, order, splittable = specs[table]
        parts = -(-n // rows_per_file) if splittable else 1
        t = time.time()
        log.info("%s:   writing %s (%d rows, %d file%s)", stamp, table, n, parts, "" if parts == 1 else "s")
        written = 0
        for b in range(parts):
            name = f"{table}_{stamp}.parquet" if parts == 1 else f"{table}_{stamp}_p{b:02d}.parquet"
            rel = f"{table}/date={date}/{name}"
            path = out_dir / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            cond = where if parts == 1 else f"{where} AND hash(m.market_ticker) % {parts} = {b}"
            con.execute(f"COPY (SELECT {cols} FROM stage WHERE {cond} ORDER BY {order}) "
                        f"TO {q(path.as_posix())} {opts}")
            written += con.execute(f"SELECT count(*) FROM read_parquet({q(path.as_posix())})").fetchone()[0]
            files.append(rel)
        mb = sum((out_dir / f).stat().st_size for f in files if f.startswith(table + "/")) / 1e6
        log.info("%s:   %s done in %.0fs (%.0f MB)", stamp, table, time.time() - t, mb)
        if written != n:
            raise CheckFailed(f"{table}: wrote {written} rows, expected {n}")
    total = con.execute("SELECT count(*) FROM stage").fetchone()[0]
    if sum(rows.values()) != total:
        raise CheckFailed(f"row counts {rows} do not add up to {total} input lines")
    return {"rows": rows, "files": files}


def convert_hour(stamp: str, inputs: list[dict], raw: Store, out: Store, work: Path, args) -> dict:
    t0 = time.time()
    date = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
    hour_dir = work / f"hour_{stamp}"
    if hour_dir.exists():
        shutil.rmtree(hour_dir)
    in_dir, out_dir = hour_dir / "in", hour_dir / "out"
    gb = sum(f["size"] for f in inputs) / 1e9
    log.info("%s: downloading %d files (%.2f GB)", stamp, len(inputs), gb)
    raw.download("", [f["path"] for f in inputs], in_dir)
    log.info("%s: downloaded in %.0fs; loading", stamp, time.time() - t0)
    files = [in_dir / f["path"] for f in inputs]
    for f, meta in zip(files, inputs):
        if f.stat().st_size != meta["size"]:
            raise RuntimeError(f"{f.name}: downloaded {f.stat().st_size} bytes, listing says {meta['size']}")

    con = duckdb.connect(str(hour_dir / "stage.duckdb"))
    try:
        con.execute(f"SET threads = {args.threads}")
        con.execute(f"SET memory_limit = {q(args.memory_limit)}")
        con.execute(f"SET temp_directory = {q((hour_dir / 'tmp').as_posix())}")
        con.execute("SET preserve_insertion_order = false")

        repaired = []
        try:
            load_stage(con, files)
        except duckdb.OutOfMemoryException:
            raise  # not a damaged file: retrying through the repair path would not help
        except duckdb.Error as e:
            # Most likely a file cut off by a collector crash. Salvage every complete record.
            log.warning("%s: direct read failed (%s); rewriting inputs and retrying",
                        stamp, str(e).splitlines()[0][:200])
            fix_dir = hour_dir / "fixed"
            fix_dir.mkdir()
            fixed = []
            for f in files:
                dst = fix_dir / f.name
                if repair_zst(f, dst):
                    repaired.append(f.name)
                    log.warning("%s: %s was truncated; kept all complete records", stamp, f.name)
                fixed.append(dst)
            load_stage(con, fixed)

        n = con.execute("SELECT count(*) FROM stage").fetchone()[0]
        log.info("%s: loaded %d records at %.0fs; verifying", stamp, n, time.time() - t0)
        if n == 0:
            raise CheckFailed("no records could be read from the raw files")
        stats = run_checks(con)
        log.info("%s: verified at %.0fs; writing Parquet", stamp, time.time() - t0)
        written = write_tables(con, out_dir, date, stamp, args.zstd_level, args.rows_per_file)
    finally:
        # Also when the hour fails: the exception keeps this connection alive while main()
        # cleans up, and Windows cannot delete a database file that is still open, so the
        # failed hour's scratch files (tens of GB) would stay behind.
        con.close()
    rows = written["rows"]
    log.info("%s: wrote Parquet at %.0fs; uploading", stamp, time.time() - t0)

    bytes_out = sum(p.stat().st_size for p in out_dir.rglob("*.parquet"))
    manifest = {
        "version": VERSION, "hour": stamp,
        "converted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "inputs": [{"path": f["path"], "size": f["size"]} for f in inputs],
        "outputs": written["files"],
        "bytes_in": sum(f["size"] for f in inputs), "bytes_out": bytes_out,
        "lines": sum(rows.values()), "rows": rows,
        "repaired_inputs": repaired, **stats,
        "seconds": round(time.time() - t0, 1),
    }
    # An hour that is being converted again: take its old manifest down before touching its
    # files. A run that dies part-way through the upload then leaves the hour "not done"
    # (redone next time), not an old done-marker sitting on a mix of old and new files.
    if any(f["path"] == f"{stamp}.json" for f in out.ls(f"_manifests/date={date}")):
        out.delete(f"_manifests/date={date}/{stamp}.json")
        log.info("%s: removed the previous manifest before replacing the hour's files", stamp)
    out.upload_tree(out_dir, "")
    # A reconverted hour may have been split differently before: remove its old files.
    new = set(written["files"])
    for table in TABLES:
        for f in out.ls(f"{table}/date={date}"):
            rel = f"{table}/date={date}/{f['path']}"
            if f["path"].startswith(f"{table}_{stamp}") and rel not in new:
                out.delete(rel)
                log.info("%s: removed stale %s", stamp, rel)
    # The manifest goes up last: its presence means the hour's Parquet is complete.
    mdir = hour_dir / "manifest" / "_manifests" / f"date={date}"
    mdir.mkdir(parents=True)
    (mdir / f"{stamp}.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    out.upload_tree(hour_dir / "manifest", "")
    if not args.keep_work:
        shutil.rmtree(hour_dir)
    return manifest


# ---------------------------------------------------------------- planning
def plan(raw: Store, out: Store, work: Path, args) -> list[tuple[str, list[dict]]]:
    hours: dict[str, list[dict]] = {}
    skipped = 0
    for f in raw.ls(""):
        m = RAW_RE.match(f["path"].rsplit("/", 1)[-1])
        if not m:
            skipped += 1
            continue
        hours.setdefault(f"{m[1]}T{m[2]}", []).append(f)
    if skipped:
        log.info("ignored %d files under --raw that are not collector output", skipped)

    mdir = work / "_manifests"
    out.mirror_down("_manifests", mdir)
    done = {}
    for p in mdir.rglob("*.json"):
        try:
            done[p.stem] = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            log.warning("unreadable manifest %s: will reconvert", p.name)

    now = datetime.now(timezone.utc)
    todo = []
    for stamp in sorted(hours):
        date = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
        if (args.start and date < args.start) or (args.end and date > args.end):
            continue
        start = datetime.strptime(stamp, "%Y%m%dT%H").replace(tzinfo=timezone.utc)
        if now < start + timedelta(hours=1, minutes=args.settle_minutes):
            continue  # hour may not be fully uploaded yet
        inputs = sorted(hours[stamp], key=lambda f: f["path"])
        man = done.get(stamp)
        if man:
            had = {(f["path"], f["size"]) for f in man.get("inputs", [])}
            if not args.force and all((f["path"], f["size"]) in had for f in inputs):
                continue  # nothing new for this hour (raw files expiring later is fine)
            gone = {f["path"] for f in man.get("inputs", [])} - {f["path"] for f in inputs}
            if gone:
                # Converting again from what is left would replace complete Parquet with less.
                log.warning("%s: %d of the raw files it was converted from are no longer in --raw; "
                            "leaving its Parquet as it is", stamp, len(gone))
                continue
            if not args.force:
                log.info("%s: raw inputs changed since last conversion; redoing", stamp)
        todo.append((stamp, inputs))
    return todo


def lock_work_dir(work: Path):
    """One converter per work directory: a second one would delete the first one's scratch
    files. Returns the lock file's descriptor (the lock lasts until the process ends), or
    None if another run holds it."""
    fd = os.open(work / "convert.lock", os.O_RDWR | os.O_CREAT)
    try:
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"\n")          # Windows locks a byte range: make sure byte 0 exists
        os.lseek(fd, 0, os.SEEK_SET)
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", required=True, help="raw capture root: directory or rclone path, e.g. b2:bucket/raw")
    ap.add_argument("--out", required=True, help="Parquet root: directory or rclone path, e.g. b2:bucket/parquet")
    ap.add_argument("--work", default="work", help="local scratch directory (needs ~10x one raw hour)")
    ap.add_argument("--start", help="first UTC date to convert, YYYY-MM-DD")
    ap.add_argument("--end", help="last UTC date to convert, YYYY-MM-DD")
    ap.add_argument("--max-hours", type=int, default=0, help="stop after this many hours (0 = no limit)")
    ap.add_argument("--settle-minutes", type=int, default=45,
                    help="wait this long after an hour ends before converting it")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--memory-limit", default="8GB")
    ap.add_argument("--zstd-level", type=int, default=9)
    ap.add_argument("--rows-per-file", type=int, default=15_000_000,
                    help="split a table into several files per hour above this many rows "
                         "(lower it if DuckDB runs out of memory)")
    ap.add_argument("--stall-minutes", type=float, default=10,
                    help="restart an rclone transfer that has moved no data for this long")
    ap.add_argument("--force", action="store_true", help="reconvert hours that already have a manifest")
    ap.add_argument("--keep-work", action="store_true", help="keep each hour's scratch files")
    ap.add_argument("--dry-run", action="store_true", help="list the hours that would be converted")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    Store.stall_seconds = args.stall_minutes * 60
    raw, out = Store(args.raw), Store(args.out)
    work = Path(args.work).expanduser().resolve()
    work.mkdir(parents=True, exist_ok=True)
    # e.g. the nightly task starting while a long backfill is still running
    lock = None if args.dry_run else lock_work_dir(work)
    if lock is None and not args.dry_run:
        log.error("another convert.py is already running in %s; not starting a second one", work)
        return 3

    todo = plan(raw, out, work, args)
    if args.max_hours:
        todo = todo[:args.max_hours]
    gb = sum(f["size"] for _, fs in todo for f in fs) / 1e9
    log.info("%d hours to convert (%.1f GB raw)", len(todo), gb)
    if args.dry_run:
        for stamp, inputs in todo:
            print(f"{stamp}  {len(inputs)} files  {sum(f['size'] for f in inputs) / 1e9:6.2f} GB")
        return 0

    failed, streak = [], 0
    for stamp, inputs in todo:
        try:
            man = convert_hour(stamp, inputs, raw, out, work, args)
            streak = 0
        except Exception as e:  # one bad hour must not stop an unattended backlog
            failed.append(stamp)
            streak += 1
            kind = "verification failed" if isinstance(e, CheckFailed) else f"{type(e).__name__}"
            log.error("%s: %s, hour NOT marked done (retried next run): %s",
                      stamp, kind, str(e).splitlines()[0][:500] if str(e) else "")
            if isinstance(e, CheckFailed) or args.keep_work:
                log.error("scratch files kept in %s", work / f"hour_{stamp}")
            else:
                shutil.rmtree(work / f"hour_{stamp}", ignore_errors=True)  # raw data is safe in --raw
            if streak >= 3:
                log.error("3 hours in a row failed: stopping (likely network, credentials or disk)")
                break
            continue
        note = ""
        if man["unknown_keys"]:
            note += f"  UNKNOWN FIELDS (not converted): {man['unknown_keys']}"
        if man["deltas_without_exchange_ts"]:
            note += f"  deltas without exchange timestamp: {man['deltas_without_exchange_ts']}"
        if man["repaired_inputs"]:
            note += f"  truncated inputs: {man['repaired_inputs']}"
        log.info("%s: %d records, %.2f GB -> %.2f GB (%.1fx) in %.0fs%s", stamp, man["lines"],
                 man["bytes_in"] / 1e9, man["bytes_out"] / 1e9,
                 man["bytes_in"] / max(man["bytes_out"], 1), man["seconds"], note)
    if failed:
        log.error("done with %d failed hour(s): %s", len(failed), ", ".join(failed))
        return 2
    log.info("done: all hours converted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
