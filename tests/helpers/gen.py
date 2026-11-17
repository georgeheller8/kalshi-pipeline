"""Generate raw files in collector.py's exact on-disk format."""
import json, random, uuid, zstandard, os
from pathlib import Path
from datetime import datetime, timezone

def gen_hour(root, stamp, shard, start, n, seed, strip=False, n_markets=400, truncate=False, legacy_name=False,
             trailing_newline=1.0):
    """Write one raw file; return (path, lines). trailing_newline = share of server messages
    that keep Kalshi's trailing newline (as older collector versions stored them)."""
    rnd = random.Random(seed)
    date = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
    d = Path(root)/f"date={date}"; d.mkdir(parents=True, exist_ok=True)
    name = f"kalshi_{stamp}_{start}.jsonl.zst" if legacy_name else f"kalshi_{stamp}_s{shard}_{start}.jsonl.zst"
    series = ["KXNCAAFSPREAD","KXNCAAFTOTAL","KXNCAAMBTOTAL","KXBTCD","KXPGATOUR","KXAAAGASDNM"]
    mk = [(f"{rnd.choice(series)}-26NOV14T{i:04d}-{rnd.choice(['ALA','UGA','OSU'])}{rnd.randint(1,40)}", str(uuid.uuid4())) for i in range(n_markets)]
    hot = mk[:max(1,n_markets//10)]
    t_ms = int(datetime.strptime(stamp, "%Y%m%dT%H").replace(tzinfo=timezone.utc).timestamp()*1000)
    seq = {c:0 for c in range(4)}; tseq = 0; clock = 20192983789
    lines = []
    def emit(conn, obj, ns_extra=0):
        body = obj if isinstance(obj, bytes) else json.dumps(obj, separators=(",",":")).encode()
        if conn >= 0 and rnd.random() < trailing_newline: body += b"\n"      # server messages end with a newline
        lines.append(b'{"recv_ns":%d,"conn":%d,"msg":%s}\n' % (t_ms*1000000 + rnd.randint(0,999999), conn, body))
    def price(): 
        return rnd.choice([f"0.{rnd.randint(1,99):02d}00", f"0.{rnd.randint(1,999):03d}0", f"0.{rnd.randint(1,9999):04d}", "1.0000"])
    def size(neg=False):
        s = f"{rnd.randint(1,99999)}.{rnd.choice([0,0,0,rnd.randint(1,99)]):02d}"
        return ("-" if neg and rnd.random()<.5 else "") + s
    def levels():
        return [[price(), size()] for _ in range(rnd.randint(1,12))]
    for c in range(4):
        emit(-1, {"type":"collector_connect","conn":c})
        emit(c, {"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":1}})
    for i in range(n):
        t_ms += rnd.randint(0,3); clock += rnd.randint(1,50)
        c = rnd.randrange(4); r = rnd.random()
        tk, mid = rnd.choice(hot) if rnd.random() < .8 else rnd.choice(mk)
        ts_iso = datetime.fromtimestamp(t_ms/1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{t_ms%1000:03d}Z"
        if r < .90:
            seq[c]+=1
            m = {"market_ticker":tk,"market_id":mid,"clock":clock,"price_dollars":price(),"delta_fp":size(True),"side":rnd.choice(["yes","no"])}
            v = rnd.random()
            if v < .9: m["ts"]=ts_iso; m["ts_ms"]=t_ms
            elif v < .95: m["ts"]=ts_iso           # only deprecated ts
            # else neither
            if strip: m.pop("market_id"); 
            if strip and "ts_ms" in m: m.pop("ts", None)
            env = {"type":"orderbook_delta","sid":1,"seq":seq[c],"msg":m}
            if rnd.random()<.97: env["sending_ts_ms"]=t_ms+2
            emit(c, env)
        elif r < .96:
            seq[c]+=1
            m = {"market_ticker":tk,"market_id":mid,"clock":rnd.choice([0,clock])}
            v = rnd.random()
            if v < .6: m["yes_dollars_fp"]=levels(); m["no_dollars_fp"]=levels()
            elif v < .8: m["no_dollars_fp"]=levels()
            elif v < .9: m["yes_dollars_fp"]=levels()
            emit(c, {"type":"orderbook_snapshot","sid":1,"seq":seq[c],"msg":m,"sending_ts_ms":t_ms+2})
        elif r < .985 and shard == 0:
            tseq+=1; yp = rnd.randint(1,99)
            m = {"trade_id":str(uuid.uuid4()),"market_ticker":tk,"yes_price_dollars":f"0.{yp:02d}00","no_price_dollars":f"0.{100-yp:02d}00","count_fp":size(),
                 "taker_side":rnd.choice(["yes","no"]),"taker_outcome_side":rnd.choice(["yes","no"]),"taker_book_side":rnd.choice(["bid","ask"]),"is_block_trade":rnd.random()<.01,"ts":t_ms//1000,"ts_ms":t_ms}
            emit(0, {"type":"trade","sid":3,"seq":tseq,"msg":m,"sending_ts_ms":t_ms+1})
        elif r < .99:
            emit(0, {"type":"market_lifecycle_v2","sid":2,"seq":i,"msg":{"market_ticker":tk,"event_type":rnd.choice(["created","settled","determined","close_date_updated","deactivated"]),"open_ts":t_ms//1000,"é":"ünï"}})
        elif r < .995:
            seq[c]+=1
            emit(c, {"type":"ok","id":rnd.randint(2,60),"sid":1,"seq":seq[c],"msg":{"market_tickers":[x[0] for x in rnd.sample(mk, min(50,len(mk)))]}})
        elif r < .998:
            emit(-1, {"type":"collector_gap","conn":c,"sid":1,"expected":seq[c]+1,"got":seq[c]+2})
        else:
            emit(c, {"id":rnd.randint(2,60),"type":"error","msg":{"code":27,"msg":"rate limited"}})
    raw = b"".join(lines)
    p = d/name
    with open(p,"wb") as fh:
        w = zstandard.ZstdCompressor(level=1).stream_writer(fh)
        # write in pieces like the collector does
        for i in range(0, len(raw), 1<<16): w.write(raw[i:i+(1<<16)])
        if truncate:
            w.flush(zstandard.FLUSH_BLOCK); fh.flush()
        else:
            w.close()
    if truncate:
        sz = p.stat().st_size
        with open(p,"r+b") as fh: fh.truncate(sz - 5000)
    return p, lines
