"""Independent reference: parse raw lines with the json module and Decimal, compare to the Parquet output."""
import json, sys, glob
from decimal import Decimal
from datetime import datetime
import pyarrow.parquet as pq, pyarrow.dataset as ds

def fx(s, scale):
    v = Decimal(s)*scale; assert v == v.to_integral_value(); return int(v)
def expected(lines_by_shard):
    out = {"deltas":[], "snapshots":[], "trades":[], "events":[]}
    for shard, lines in lines_by_shard:
        for ln in lines:
            o = json.loads(ln); e = o["msg"]; t = e["type"]; m = e.get("msg")
            base = (o["recv_ns"], shard, o["conn"])
            if t == "orderbook_delta":
                ts = m.get("ts_ms")
                if ts is None and "ts" in m:
                    ts = int(round(datetime.fromisoformat(m["ts"].replace("Z","+00:00")).timestamp()*1000))
                out["deltas"].append(base+(e["sid"], e["seq"], m["market_ticker"], m["clock"], m["side"], fx(m["price_dollars"],10000), fx(m["delta_fp"],100), ts, e.get("sending_ts_ms"), None, None))
            elif t == "orderbook_snapshot":
                y = m.get("yes_dollars_fp", []); n = m.get("no_dollars_fp", [])
                out["snapshots"].append(base+(e["sid"], e["seq"], m["market_ticker"], m["market_id"], m["clock"],
                    tuple(fx(a,10000) for a,_ in y), tuple(fx(b,100) for _,b in y), tuple(fx(a,10000) for a,_ in n), tuple(fx(b,100) for _,b in n), e.get("sending_ts_ms")))
            elif t == "trade":
                out["trades"].append(base+(e["sid"], e["seq"], m["trade_id"], m["market_ticker"], fx(m["yes_price_dollars"],10000), fx(m["no_price_dollars"],10000), fx(m["count_fp"],100),
                    m["taker_side"], m["taker_outcome_side"], m["taker_book_side"], m["is_block_trade"], m["ts_ms"], e.get("sending_ts_ms")))
            else:
                if t == "ok" and isinstance(m, dict) and "market_tickers" in m:
                    e = dict(e, msg={**{k: v for k, v in m.items() if k != "market_tickers"},
                                     "market_tickers_count": len(m["market_tickers"])})
                out["events"].append(base+(t, e.get("sid"), e.get("seq"), json.dumps(e, sort_keys=True)))
    return out
def actual(out_root):
    res = {}
    for table in ("deltas","snapshots","trades","events"):
        files = sorted(glob.glob(f"{out_root}/{table}/date=*/*.parquet"))
        rows = []
        for f in files:
            t = pq.read_table(f)
            for r in t.to_pylist():
                vals = []
                for k,v in r.items():
                    if isinstance(v, list): v = tuple(v)
                    if k == "raw": v = json.dumps(json.loads(v), sort_keys=True)
                    vals.append(v)
                rows.append(tuple(vals))
        res[table] = rows
    return res
def compare(lines_by_shard, out_root):
    exp = expected(lines_by_shard); act = actual(out_root); ok = True
    for t in exp:
        a = sorted(act[t], key=repr); e = sorted(exp[t], key=repr)
        same = a == e
        ok &= same
        print(f"  {t:9s} expected {len(e):7d} got {len(a):7d}  {'MATCH' if same else 'MISMATCH'}")
        if not same:
            for x,y in zip(a,e):
                if x!=y: print("   got", x, "\n   exp", y); break
    return ok
