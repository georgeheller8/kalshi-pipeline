#!/usr/bin/env python3
"""
How many distinct Kalshi markets the converted Parquet covers.

  python count_markets.py b2:gh-kalshi-raw/parquet      # straight from B2 (reads only the ticker column)
  python count_markets.py C:\\path\\to\\parquet           # or a local copy

Reports markets that appear in snapshots (subscribed, book seen), deltas (book
changed at least once) and trades, overall and per UTC day, plus the series
with the most active markets. B2 credentials come from your rclone config.
"""
import base64
import json
import subprocess
import sys
import time
import urllib.request

import duckdb


def b2_s3_setup(con, remote: str):
    """Point DuckDB's S3 reader at B2, using the key stored in rclone's config."""
    cfg = json.loads(subprocess.run(["rclone", "config", "dump"], capture_output=True, text=True,
                                    encoding="utf-8", check=True).stdout)
    if remote not in cfg or cfg[remote].get("type") != "b2":
        raise SystemExit(f"rclone remote '{remote}' is not a b2 remote")
    key_id, key = cfg[remote]["account"], cfg[remote]["key"]
    req = urllib.request.Request("https://api.backblazeb2.com/b2api/v3/b2_authorize_account")
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{key_id}:{key}".encode()).decode())
    with urllib.request.urlopen(req, timeout=30) as r:
        s3_url = json.load(r)["apiInfo"]["storageApi"]["s3ApiUrl"]  # e.g. https://s3.us-west-004.backblazeb2.com
    host = s3_url.split("://", 1)[1]
    region = host.split(".")[1]
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET b2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION ?, URL_STYLE 'path')",
                [key_id, key, host, region])


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    src = sys.argv[1].rstrip("/\\")
    con = duckdb.connect()
    if ":" in src and not (len(src) > 1 and src[1] == ":"):   # rclone path, not C:\...
        remote, path = src.split(":", 1)
        b2_s3_setup(con, remote)
        root = f"s3://{path}"
    else:
        root = src.replace("\\", "/")

    def tickers(table: str) -> str:
        return (f"SELECT DISTINCT market_ticker, regexp_extract(filename, 'date=([0-9-]+)', 1) AS day "
                f"FROM read_parquet('{root}/{table}/date=*/*.parquet', filename = true)")

    t = time.time()
    print("reading ticker columns (deltas take a few minutes on a full backlog)...")
    for table in ("snapshots", "deltas", "trades"):
        try:
            con.execute(f"CREATE TABLE {table} AS {tickers(table)}")
        except duckdb.IOException:
            con.execute(f"CREATE TABLE {table} (market_ticker VARCHAR, day VARCHAR)")  # no files yet

    print(f"\nDistinct markets (read in {time.time() - t:.0f}s)")
    print(con.sql("""
        SELECT 'with a snapshot (subscribed)' AS seen_in, count(DISTINCT market_ticker) AS markets FROM snapshots
        UNION ALL SELECT 'with >= 1 delta (book moved)', count(DISTINCT market_ticker) FROM deltas
        UNION ALL SELECT 'with >= 1 trade', count(DISTINCT market_ticker) FROM trades
        UNION ALL SELECT 'any of the above', count(DISTINCT market_ticker)
            FROM (SELECT market_ticker FROM snapshots UNION SELECT market_ticker FROM deltas
                  UNION SELECT market_ticker FROM trades)
    """))
    print("Per UTC day")
    print(con.sql("""
        SELECT day, count(DISTINCT s.market_ticker) AS subscribed,
               (SELECT count(DISTINCT market_ticker) FROM deltas d WHERE d.day = s.day) AS book_moved,
               (SELECT count(DISTINCT market_ticker) FROM trades r WHERE r.day = s.day) AS traded
        FROM snapshots s GROUP BY day ORDER BY day
    """))
    print("Series with the most markets whose book moved")
    print(con.sql("""
        SELECT split_part(market_ticker, '-', 1) AS series, count(DISTINCT market_ticker) AS markets
        FROM deltas GROUP BY series ORDER BY markets DESC LIMIT 15
    """))


if __name__ == "__main__":
    main()
