# Kalshi Pipeline

Kalshi does not publish historical orderbook data. Its API returns historical prices, but nothing about orderbook depth, queue position, or how resting orders were filled. A realistic backtest, and in particular a fill model, depends on exactly that information. This pipeline records every orderbook change on every open Kalshi market, around the clock, and converts the recordings into Parquet tables for backtesting.

## Flowchart

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/darkdiagram.svg">
  <img alt="Pipeline diagram" src="docs/lightdiagram.svg">
</picture>

## Infrastructure

**Oracle Cloud Infrastructure**: The collector runs on an Always Free Arm instance with 2 OCPUs and 12 GB of memory, running Ubuntu 24.04 Minimal. The VM is in the US-East (Ashburn) region, the closest available region to Kalshi's exchange infrastructure in Columbus, which keeps network latency and the risk of falling behind the feed low.

**Backblaze B2**: A single bucket holds both the raw recordings and the converted Parquet tables. B2 was chosen for its low storage cost ($6.95 per TB per month on the pay-as-you-go plan), which matters at roughly 75 GB of compressed raw data on a busy day. Raw files are retained until the Parquet output has been validated against a backtester.

**Razer Blade 14**: Conversion runs on a Windows laptop rather than on the VM. The VM's two cores are already 55-70% busy with the collector, and a collector that falls behind loses data permanently, so all heavy processing happens elsewhere. A scheduled task on the laptop runs the converter nightly.

**Raspberry Pi** (planned): A fill model needs data with as few gaps as possible, and an extended outage of either the VM or B2 would leave holes that cannot be recovered later. A Raspberry Pi running a second, independent copy of the collector, with local storage as a fallback for B2, would protect against both. The two feeds can be merged afterwards using the exchange's message ordering.

## Collector Software

**Kalshi Exchange**: Kalshi offers two interfaces: a REST API for querying markets on request, and a WebSocket feed that pushes updates as they happen. When the collector subscribes to a market's orderbook channel, it gets a snapshot of the book followed by a live stream of every change (deltas). It never has to poll individual markets.

**Collector**: On startup, the collector requests the list of open markets from the REST API and subscribes to the orderbook feed of each one. Multivariate combo markets are excluded. It also subscribes to two exchange-wide channels: lifecycle updates, which announce information about market creation/settlement, and trade updates, which are a stream of every trade executed on the exchange. As a safety net, the collector repeats the full REST listing every 30 minutes and subscribes to any open market it has missed.

The collector runs as two shards, one process per CPU core. The system runs on a 2-core VM, the most available on Oracle Cloud's free tier. Each market is assigned to a shard by a hash of its ticker, and each shard has four WebSocket connections. Within a shard, a new market goes to whichever connection has the fewest markets, which keeps the load balanced. Connection zero of each shard also carries the lifecycle subscription, and each shard acts only on its own markets. Connection zero of shard zero also carries the trade subscription.

**Writer and Uploader**: Each shard writes every message it receives to a zstd-compressed JSON Lines file. Each line is tagged with a local receive timestamp and the connection the message arrived on. At the top of each UTC hour, the shard closes the current file and moves it to a staging directory. At ten minutes past each hour, a timer runs an rclone job that moves everything in the staging directory to a Backblaze B2 bucket.

Normally an upload is two files, one per shard, but it can be more. A shard that restarts mid-hour starts a new file for that hour, and files from a failed upload go with the next run. A local file is deleted only after it has been uploaded and its checksum verified.

## Converter Software

**Converter**: Each night, a scheduled task on the laptop checks out the latest tested code and runs the converter. It works through the raw files one UTC hour at a time. It skips hours that ended less than 45 minutes ago, since their files may still be uploading, and hours that were already converted from the same raw files. For each remaining hour, it downloads the raw files, loads them into DuckDB, and sorts every record into one of four tables: deltas, snapshots, trades, and events. The events table holds everything else as raw JSON: lifecycle messages, subscription acknowledgements, errors, and the collector's own connection and gap records. Kalshi sends prices and sizes as decimal strings. The converter stores them as exact integers, with prices in 1/10,000 of a dollar and sizes in 1/100 of a contract.

Before writing anything, the converter checks the hour. Every record must have its required fields, every price and size must convert to an integer exactly, and every price must be in range. A raw file cut off by a collector crash is repaired by keeping every complete record. If an hour fails a check, the converter logs it, skips it, and retries it on the next run. Three failures in a row stop the run, because that usually means a network, credentials, or disk problem rather than bad data.

**Writer and Uploader**: Each table is written as zstd-compressed Parquet under `parquet/<table>/date=YYYY-MM-DD/`, sorted by market, then receive time, then sequence number. Large tables in a busy hour are split into several files by a hash of the market ticker. This keeps each sort within memory, and each market's rows for the hour still end up in exactly one file. After writing, the converter checks that the row counts match the input. It then uploads the files with rclone, with a watchdog that restarts any transfer that moves no data for 10 minutes. The last file uploaded for an hour is a manifest that lists its inputs, outputs, row counts, and check results, and the manifest's presence marks the hour as complete. If more raw files arrive for an hour that was already converted, the converter removes the old manifest, converts the hour again, and deletes any output files the new conversion no longer needs. The converter never modifies or deletes raw files.

## Test Suite

The tests run with pytest in about two and a half minutes and cover all three parts of the pipeline without touching Kalshi, B2, or either machine. The converter tests generate raw files in the collector's exact on-disk format and compare every output table against a separate, independently written parser. They cover truncated files, late-arriving and expired raw files, reruns, split files, huge order sizes, malformed numbers, unknown fields, stalled downloads (using a fake rclone), and skipping failed hours. The collector tests run its message handling against a fake WebSocket, including sequence gaps and fresh snapshots, reconnecting after Kalshi errors, adding new markets when they open, the REST rescan, and hourly file rotation. The deploy tests run `deploy.sh` against a simulated VM with a local git origin and stand-ins for sudo, systemctl, journalctl, and pip, including rollback when a shard comes up unhealthy. Untested: the real GitHub Actions workflow, systemd on the VM, and the Windows batch file.

## CI/CD

Deployment is pull-based, so nothing on GitHub can reach the machines. On every push, GitHub Actions runs shellcheck on the deploy script and runs the full test suite. When the tests pass on `main`, CI moves the `deploy` branch forward to that commit. Every two minutes, a systemd timer on the VM checks `deploy` for a new commit. When it finds one, it installs changed dependencies and unit files, then restarts the collector shards one at a time, waiting for each to report all of its connections up before restarting the next. If a shard does not recover, the VM rolls back to the previous commit and does not retry the bad one. The laptop's nightly script checks out `deploy` before each conversion run. Secrets such as the Kalshi key, the collector config, and the rclone credentials stay on the machines and are never committed.

