# s3rsync

`rsync` for S3-compatible object storage: a fast, highly-concurrent, streaming bucket-to-bucket copier written in Rust.

[![CI](https://github.com/luccinmasirika/s3rsync/actions/workflows/ci.yml/badge.svg)](https://github.com/luccinmasirika/s3rsync/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
![Rust](https://img.shields.io/badge/rust-1.82%2B-orange.svg)

s3rsync copies every object from one S3-compatible bucket to another: AWS S3 to MinIO, MinIO to MinIO, Ceph, Backblaze B2, Cloudflare R2, anything that speaks the S3 API. It streams objects straight through (no temp files), runs hundreds of transfers in parallel, and resumes where it left off. On workloads dominated by small objects it is measurably faster than `rclone`.

```bash
s3rsync --src-bucket photos --dst-bucket photos-backup --transfers 128
```

## Why

Migrating a bucket with hundreds of thousands of small objects is dominated by request rate, not bandwidth, and that scales with concurrency. Two things hold general-purpose tools back on this workload:

1. **Conservative concurrency.** `rclone copy` defaults to a handful of transfers; even tuned, its per-transfer cost (Go runtime, buffers) makes very high concurrency expensive.
2. **List-then-transfer.** `rclone` reconciles source and destination by fully listing a directory before transferring any of it. On a flat prefix with 45k keys that means nothing moves until the whole listing finishes.

s3rsync targets the copy case specifically (not a general two-way sync), which lets it:

- **Pipeline listing and transfer.** It streams the source listing page by page and starts copying immediately.
- **Push concurrency far higher.** Rust + Tokio keeps each in-flight transfer down to ~kilobytes, so 128 to 256 concurrent transfers fit in a few hundred MB of RAM.
- **Adapt to the server.** An AIMD limiter backs off when the destination throttles, then ramps back up.

## Benchmarks

Reproducible with [`bench.sh`](bench.sh). Both tools copy the same source prefix to a fresh destination, sequentially (no interference), for a fixed window. `files/s = objects actually written ÷ window`.

> **Environment:** Apple Silicon Mac to MinIO over a bandwidth-limited VPN (~1-2 MB/s), source AWS S3 (`eu-central-1`). Workload: ~45,000 small JSON objects (~88 KB average). Window: 240 s.

| Tool | Concurrency | Files / 240 s | files/s |
|---|---:|---:|---:|
| rclone 1.74 | 24 | 2,115 | 8.8 |
| **s3rsync** | 128 | **4,512** | **18.8** |
| s3rsync | 256 | 5,243 | 21.8 |

About 2.1 to 2.4x rclone on small objects, on the same link. (Versus a naive `aws`+`mc`-per-file shell loop at ~2 files/s: about 13x.)

**Notes.** These numbers are on a bandwidth-limited link, where the win comes from concurrency hiding latency in the small-object regime. On large objects both tools are bandwidth-bound and roughly tie. In a low-latency, high-bandwidth environment (same-datacenter) the gap should widen, but that benchmark is still pending. Separately, on a large flat prefix, rclone transferred 0 files in the first 90 s (still listing 45k keys) while s3rsync was already copying, because s3rsync pipelines listing and transfer.

## Features

- **Streaming GET to PUT.** Objects flow straight through, never buffered to disk or fully into memory.
- **Decoupled concurrency pools.** A large pool for small objects (`--transfers`, default 128) and a small pool for large/multipart objects (`--big-transfers`), so big files never starve the small-file throughput.
- **AIMD adaptive throttle.** Reduces concurrency on `SlowDown` / connection resets, then recovers.
- **Multipart** for large objects (ranged GET to UploadPart), memory-bounded.
- **Metadata preserved.** Content-Type, Content-Encoding, Cache-Control, Content-Disposition, and user metadata.
- **End-to-end integrity.** `--checksum` sends the source MD5 as `Content-MD5`, so the destination rejects any corrupted single-part upload; `--verify` re-checks object size after copy.
- **Resumable.** Skips objects already present by size, plus an append-only journal for instant re-run skips.
- **Sync mode.** `--delete` removes destination objects that no longer exist in the source (batched).
- **Filters.** `--include` / `--exclude` glob patterns.
- **Bandwidth cap.** `--bwlimit` (token bucket).
- **`--size`.** Report a bucket/prefix object count and total bytes.
- **`--dry-run`.** Plan and report without writing anything.
- **Resilient listing.** Per-page retries; if a listing page ultimately fails, the run exits non-zero rather than silently migrating a partial set.
- **Graceful Ctrl-C.** Drains in-flight transfers and flushes the journal instead of dying mid-write.
- **Any S3-compatible source.** `--src-endpoint` to copy MinIO to MinIO, R2 to S3, etc.

## Install

Requires Rust 1.82+.

```bash
git clone https://github.com/luccinmasirika/s3rsync
cd s3rsync
cargo build --release
# binary at ./target/release/s3rsync
```

## Usage

Destination (S3-compatible) credentials come from the environment (or a `.env`):

```bash
export MINIO_REMOTE_URL=https://minio.example.com:9000
export MINIO_REMOTE_ACCESS_KEY=...
export MINIO_REMOTE_SECRET_KEY=...
# source AWS S3: standard AWS_* env / ~/.aws (SSO & MFA session tokens supported)
```

```bash
# whole bucket
s3rsync --src-bucket photos --dst-bucket photos-backup

# a prefix, into a dest prefix (structure preserved)
s3rsync --src-bucket photos --src-prefix 2024/ --dst-bucket backup/photos-2024

# dry run: count what would copy, write nothing
s3rsync --src-bucket photos --dst-bucket backup --dry-run

# end-to-end integrity
s3rsync --src-bucket photos --dst-bucket backup --checksum --verify

# filters + bandwidth cap
s3rsync --src-bucket data --dst-bucket backup --include 'logs/**' --exclude '*.tmp' --bwlimit 50

# sync mode (delete dest orphans), destructive
s3rsync --src-bucket data --dst-bucket mirror --delete

# report a bucket's size
s3rsync --dst-bucket photos-backup --size

# MinIO to MinIO (or any S3-compatible source)
s3rsync --src-endpoint https://src-minio:9000 --src-access-key K --src-secret-key S \
        --src-bucket a --dst-bucket b
```

Run `s3rsync --help` for the full flag list.

## How it works

```
 list source (paginated, retried)
        │  ObjectMeta stream (lazy, overlaps transfers)
        ▼
   ┌── plan ──┐   skip if journaled / same size on dest
   │          │
   ▼          ▼
 small      big            two MPMC channels → two worker pools
 pool       pool
 (128)      (4×multipart)  small pool gated by an AIMD semaphore
   │          │
   └──► GET(src) ──stream──► PUT(dst) ──► journal + stats
```

- The destination is listed once into a `key → size` map (far cheaper than one HEAD per object) to decide skips.
- Each object streams source to dest with a known content length, so there is no full-object buffer; peak memory is bounded by `big_transfers × part_concurrency × part_size` and validated at startup.
- Throughput is measured at the application level (bytes actually transferred), not from OS counters.

## s3rsync vs rclone

| | rclone | s3rsync |
|---|---|---|
| Small-object throughput | baseline | **~2–2.4× faster** (measured) |
| Listing | list-then-transfer | pipelined (starts immediately) |
| Default concurrency | ~4–24 | 128 (decoupled small/big pools) |
| Adaptive throttling | retries | AIMD concurrency limiter |
| Metadata preserved | ✅ | ✅ |
| Integrity | hash where available | `--checksum` (Content-MD5) + `--verify` |
| Resume | size/hash check | size check **+ journal** |
| Two-way sync / bisync | ✅ | copy + `--delete` only |
| Maturity / breadth | battle-tested, huge | young, focused |

s3rsync is not a drop-in rclone replacement. It does the copy/migrate job, fast, and deliberately omits rclone's breadth (dozens of backends, bidirectional sync, mounts, crypt, and so on). For a large migration, a good pattern is s3rsync for the bulk, then a final `rclone copy` as a trusted reconciliation pass.

## Limitations

- Young code: verify a real migration (counts + checksums) before trusting it end-to-end.
- `--checksum` gives byte-exact integrity for **single-part** objects; multipart objects fall back to size verification (their ETag isn't a plain MD5).
- No per-part resume for an interrupted multipart object (it restarts that object).
- The destination index and `--delete` set are held in memory (~150 MB per 700k keys).
- No TUI/dashboard yet (plain periodic progress lines).

## Contributing

Issues and PRs welcome. Please run `cargo fmt`, `cargo clippy -D warnings`, and `cargo build --release` before submitting.

## License

Licensed under either of **MIT** ([LICENSE-MIT](LICENSE-MIT)) or **Apache-2.0** ([LICENSE-APACHE](LICENSE-APACHE)) at your option.
