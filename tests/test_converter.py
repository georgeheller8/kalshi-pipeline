"""convert.py end to end: output must match an independent parse of the raw records."""
import json
from pathlib import Path

import pyarrow.parquet as pq
import zstandard

from conftest import run_convert
from helpers.gen import gen_hour
from helpers.ref import compare


def write_hour(raw, stamp, records, shard=0):
    d = Path(raw) / f"date={stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
    d.mkdir(parents=True, exist_ok=True)
    body = b"".join(b'{"recv_ns":%d,"conn":0,"msg":%s}\n' % (1794500000000000000 + i, json.dumps(r).encode())
                    for i, r in enumerate(records))
    p = d / f"kalshi_{stamp}_s{shard}_1794500000.jsonl.zst"
    p.write_bytes(zstandard.ZstdCompressor().compress(body))
    return p


def delta(size="1.00", price="0.4200", seq=1):
    return {"type": "orderbook_delta", "sid": 1, "seq": seq, "msg": {
        "market_ticker": "KXT-1", "clock": seq, "price_dollars": price, "delta_fp": size,
        "side": "yes", "ts_ms": 1794500000000}}


def test_round_trip_local(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "parquet"
    lines = []
    for stamp, shard, start, n, seed, strip, legacy, nl in [
        ("20261114T12", 0, 1794650000, 20000, 1, False, False, 1.0),
        ("20261114T12", 1, 1794650000, 20000, 2, True, False, 0.0),
        ("20261114T12", 1, 1794651800, 5000, 3, True, False, 0.5),   # restart fragment
        ("20261114T11", 0, 1794646400, 3000, 4, False, True, 1.0),    # pre-sharding file name
        ("20261114T23", 0, 1794690000, 3000, 5, True, False, 0.0),
        ("20261115T00", 1, 1794693600, 3000, 6, True, False, 0.0),
    ]:
        _, ls = gen_hour(raw, stamp, shard, start, n, seed, strip=strip, legacy_name=legacy, trailing_newline=nl)
        lines.append((shard, ls))
    (raw / "date=2026-11-14" / "notes.txt").write_text("not collector output")

    code, log, stdout = run_convert(raw, out, tmp_path / "work", "--dry-run")
    assert code == 0 and [l.split()[0] for l in stdout.splitlines()] == ["20261114T11", "20261114T12", "20261114T23", "20261115T00"]

    code, log, _ = run_convert(raw, out, tmp_path / "work")
    assert code == 0, log
    assert compare(lines, out)
    assert any("1 files under --raw that are not collector output" in l for l in log)

    code, log, _ = run_convert(raw, out, tmp_path / "work")
    assert code == 0 and any("0 hours to convert" in l for l in log)
    assert [p.name for p in (tmp_path / "work").iterdir()] == ["_manifests"]


def test_remote_truncated_input_reruns_and_expiry(tmp_path, fake_remote):
    bucket, env = fake_remote
    raw, out = bucket / "raw", bucket / "parquet"
    args = ("fake:bucket/raw", "fake:bucket/parquet", tmp_path / "work")

    _, l0 = gen_hour(raw, "20261114T12", 0, 1794650000, 20000, 1)
    _, l1 = gen_hour(raw, "20261114T12", 1, 1794650000, 20000, 2, strip=True, truncate=True)
    code, log, _ = run_convert(*args, env=env)
    assert code == 0, log
    man = json.loads((out / "_manifests/date=2026-11-14/20261114T12.json").read_text())
    assert man["repaired_inputs"] == ["kalshi_20261114T12_s1_1794650000.jsonl.zst"]
    kept = man["lines"] - len(l0)
    assert 0 < kept < len(l1)
    assert compare([(0, l0), (1, l1[:kept])], out)   # everything up to the cut, nothing invented

    rows = pq.read_table(out / "deltas/date=2026-11-14/deltas_20261114T12.parquet").to_pylist()
    keys = [(r["market_ticker"], r["recv_ns"], r["seq"]) for r in rows]
    assert keys == sorted(keys)

    code, log, _ = run_convert(*args, env=env)
    assert any("0 hours to convert" in l for l in log)

    late, _ = gen_hour(raw, "20261114T12", 0, 1794651900, 3000, 7)          # late file -> redo
    code, log, _ = run_convert(*args, env=env)
    assert code == 0 and len(json.loads((out / "_manifests/date=2026-11-14/20261114T12.json").read_text())["inputs"]) == 3

    before = (out / "deltas/date=2026-11-14/deltas_20261114T12.parquet").stat().st_mtime_ns
    late.unlink()                                                             # raw expiring -> untouched
    run_convert(*args, env=env)
    assert (out / "deltas/date=2026-11-14/deltas_20261114T12.parquet").stat().st_mtime_ns == before


def test_split_files_and_stale_cleanup(tmp_path, fake_remote):
    bucket, env = fake_remote
    out = bucket / "parquet"
    args = ("fake:bucket/raw", "fake:bucket/parquet", tmp_path / "work")
    _, l0 = gen_hour(bucket / "raw", "20261114T12", 0, 1794650000, 30000, 1)
    _, l1 = gen_hour(bucket / "raw", "20261114T12", 1, 1794650000, 30000, 2, strip=True)
    lines = [(0, l0), (1, l1)]
    deltas = out / "deltas/date=2026-11-14"

    assert run_convert(*args, "--rows-per-file", 7000, env=env)[0] == 0
    files = sorted(deltas.glob("*.parquet"))
    assert len(files) > 1 and compare(lines, out)
    owner = {}
    for f in files:
        rows = pq.read_table(f).to_pylist()
        keys = [(r["market_ticker"], r["recv_ns"], r["seq"]) for r in rows]
        assert keys == sorted(keys)
        for r in rows:
            owner.setdefault(r["market_ticker"], set()).add(f.name)
    assert all(len(v) == 1 for v in owner.values())          # a market lives in exactly one file

    assert run_convert(*args, "--force", env=env)[0] == 0      # unsplit: part files must go
    assert [f.name for f in deltas.glob("*.parquet")] == ["deltas_20261114T12.parquet"]
    assert compare(lines, out)
    assert run_convert(*args, "--force", "--rows-per-file", 7000, env=env)[0] == 0
    assert len(list(deltas.glob("*.parquet"))) == len(files) and compare(lines, out)


def test_ok_echoes_are_slimmed_and_fit_in_little_memory(tmp_path):
    """Kalshi's `ok` echoes every subscribed ticker (~800 KB each). Hundreds of them once
    exhausted DuckDB's memory; they are now stored as a count."""
    raw, out = tmp_path / "raw", tmp_path / "parquet"
    _, lines = gen_hour(raw, "20261114T01", 0, 1794621600, 5000, 21, strip=True)
    tickers = [f"KXNCAAFSPREAD-26NOV14{i:05d}-UGA{i % 40}" for i in range(26000)]
    big = [b'{"recv_ns":%d,"conn":%d,"msg":%s}\n' % (
        1794621600000000000 + i * 2_000_000_000, i % 4,
        json.dumps({"type": "ok", "id": i + 2, "sid": 1, "seq": i + 1,
                    "msg": {"market_tickers": [f"NEW-{i}"] + tickers}}, separators=(",", ":")).encode())
           for i in range(400)]
    with open(raw / "date=2026-11-14" / "kalshi_20261114T01_s1_1794621600.jsonl.zst", "wb") as fh:
        w = zstandard.ZstdCompressor(level=1).stream_writer(fh)
        for b in big:
            w.write(b)
        w.close()
    code, log, _ = run_convert(raw, out, tmp_path / "work", "--memory-limit", "1GB")
    assert code == 0, log
    assert compare([(0, lines), (1, big)], out)
    raw_events = [r["raw"] for r in pq.read_table(next((out / "events").rglob("*.parquet"))).to_pylist()]
    assert any('"market_tickers_count":26001' in r for r in raw_events)


def test_huge_sizes_exact_and_bad_numbers_fail_cleanly(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "parquet"
    write_hour(raw, "20261114T10", [
        delta("111111111.00", seq=1), delta("-111111111.00", seq=2), delta("99999999999.99", seq=3),
        {"type": "orderbook_snapshot", "sid": 1, "seq": 4, "msg": {
            "market_ticker": "KXT-1", "market_id": "x", "clock": 4, "yes_dollars_fp": [["0.0100", "111111111.00"]]}},
        {"type": "trade", "sid": 2, "seq": 1, "msg": {
            "trade_id": "t", "market_ticker": "KXT-1", "yes_price_dollars": "0.4200",
            "no_price_dollars": "0.5800", "count_fp": "111111111.00", "taker_side": "yes", "ts_ms": 1}},
    ])
    assert run_convert(raw, out, tmp_path / "work")[0] == 0
    read = lambda t: pq.read_table(next((out / t).rglob("*.parquet"))).to_pylist()
    assert [r["delta_e2"] for r in read("deltas")] == [11111111100, -11111111100, 9999999999999]
    assert read("snapshots")[0]["yes_size_e2"] == [11111111100]
    assert read("trades")[0]["count_e2"] == 11111111100

    write_hour(raw, "20261114T11", [delta("12.00"), delta("abc", seq=2), delta("1.5", price="oops", seq=3)])
    code, log, _ = run_convert(raw, out, tmp_path / "work")
    assert code == 2 and any("verification failed" in l for l in log)
    assert not (out / "_manifests/date=2026-11-14/20261114T11.json").exists()


def test_new_kalshi_field_is_reported(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "parquet"
    rec = delta()
    rec["msg"]["shiny_new_field"] = 7
    d = raw / "date=2026-11-14"
    d.mkdir(parents=True)
    ns = (1794500000000000000 // 499 + 1) * 499          # the converter samples every 499th record
    (d / "kalshi_20261114T05_s0_1794500000.jsonl.zst").write_bytes(zstandard.ZstdCompressor().compress(
        b'{"recv_ns":%d,"conn":0,"msg":%s}\n' % (ns, json.dumps(rec).encode())))
    code, log, _ = run_convert(raw, out, tmp_path / "work")
    assert code == 0 and any("UNKNOWN FIELDS" in l and "shiny_new_field" in l for l in log)


def test_stalled_download_is_restarted(tmp_path, fake_remote):
    bucket, env = fake_remote
    _, lines = gen_hour(bucket / "raw", "20261114T10", 0, 1794650000, 3000, 3, strip=True)
    marker = tmp_path / "hang"
    marker.write_text("x")
    env = dict(env, FAKE_HANG_ONCE=str(marker))
    code, log, _ = run_convert("fake:bucket/raw", "fake:bucket/parquet", tmp_path / "work",
                               "--stall-minutes", "0.2", env=env, timeout=300)
    assert code == 0, log
    assert any("no progress" in l and "restarting" in l for l in log)
    assert compare([(0, lines)], bucket / "parquet")


def test_bad_hours_are_skipped_and_three_in_a_row_stop(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "parquet"
    write_hour(raw, "20261114T01", [delta()])
    write_hour(raw, "20261114T02", [delta("abc")])
    write_hour(raw, "20261114T03", [{"type": "trade", "sid": 2, "seq": 1, "msg": {
        "trade_id": "t", "market_ticker": "KXT-1", "yes_price_dollars": "5.0000",
        "no_price_dollars": "0.5800", "count_fp": "1.00", "taker_side": "yes", "ts_ms": 1}}])
    write_hour(raw, "20261114T04", [delta()])
    (raw / "date=2026-11-14/kalshi_20261114T05_s0_1794500000.jsonl.zst").write_bytes(b"not zstd at all")
    write_hour(raw, "20261114T06", [delta()])
    code, log, _ = run_convert(raw, out, tmp_path / "work")
    done = sorted(p.stem for p in (out / "_manifests").rglob("*.json"))
    assert code == 2 and done == ["20261114T01", "20261114T04", "20261114T06"]
    assert any("no records could be read" in l for l in log)

    raw2, out2 = tmp_path / "raw2", tmp_path / "parquet2"
    for h, size in (("01", "x"), ("02", "y"), ("03", "z"), ("04", "1.00")):
        write_hour(raw2, f"20261114T{h}", [delta(size)])
    code, log, _ = run_convert(raw2, out2, tmp_path / "work2")
    assert code == 2 and any("3 hours in a row failed" in l for l in log)
    assert not (out2 / "_manifests/date=2026-11-14/20261114T04.json").exists()
