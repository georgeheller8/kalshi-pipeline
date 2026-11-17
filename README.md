# kalshi-pipeline

Records every orderbook change on every open [Kalshi](https://kalshi.com) market, around the clock, and turns it into Parquet for backtesting.

- **Collector** (`collector/`): asyncio service on a 2-core Oracle Cloud Arm VM. About 207,000 markets across 2 processes and 8 WebSocket connections, roughly 18,000–20,000 messages per second. Detects sequence gaps and requests fresh snapshots; writes hourly zstd JSON Lines.
- **Upload** (`deploy/systemd/b2-upload.*`): hourly rclone move to Backblaze B2, deleting local copies only after a checksum-verified upload.
- **Converter** (`converter/`): nightly DuckDB job that turns each raw hour into verified `deltas`, `snapshots`, `trades` and `events` Parquet tables, with exact fixed-point integers and a manifest per hour.

```
Kalshi ──WebSocket──▶ collector (VM) ──hourly──▶ B2 raw/ ──nightly──▶ converter ──▶ B2 parquet/
```

## Data layout

| Table | One row per | Sorted by |
|---|---|---|
| `deltas` | orderbook change | market, then receive time |
| `snapshots` | orderbook snapshot (levels as lists) | market, then receive time |
| `trades` | public trade | market, then receive time |
| `events` | everything else, as raw JSON (lifecycle, acks, the collector's own connect/gap records) | receive time |

Prices are integers in 1/10,000 dollar (`price_e4`: 2000 = $0.20); sizes in 1/100 contract (`delta_e2`: 14800 = 148.00). Files live at `parquet/<table>/date=YYYY-MM-DD/`; a busy hour may be split into several `_pNN` files, each holding a disjoint set of markets, so read whole folders.

## How code reaches the machines

Nothing on GitHub can reach the machines; they pull.

1. Every push runs the tests (`.github/workflows/ci.yml`).
2. When tests pass on `main`, CI moves the `deploy` branch forward to that commit.
3. The VM checks `deploy` every 2 minutes (`deploy/deploy.sh`, run by `kalshi-deploy.timer`). On a new commit it installs changed dependencies and systemd units, then restarts the collector shards **one at a time**, waiting for each to report all connections up. A shard that doesn't recover rolls everything back to the previous commit, and that commit is not retried.
4. The converter machine checks out `deploy` at the start of each nightly run (`converter/nightly.bat.example`).

Secrets (`kalshi.key`, `collector.env`, rclone config) stay on the machines and are git-ignored.

## Development

```bash
pip install -r collector/requirements.txt -r converter/requirements.txt -r requirements-dev.txt
pytest -q
```

The converter tests compare every output table against an independent parse of the raw records, including truncated files, huge order sizes, and stalled downloads. The deploy tests run `deploy.sh` against a simulated VM, including rollback when a shard comes up unhealthy.
