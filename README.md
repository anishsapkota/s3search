# s3search

A full-text search engine that stores its index entirely in S3-compatible object storage. Inspired by [Quickwit](https://quickwit.io/) — built from scratch as a hobby project to learn about search engine internals.

## What this is

A learning project. The goal was to understand the pieces that make a real search engine tick by actually building them:

- **FST (Finite-State Transducer)** — compressed term dictionary via [vellum](https://github.com/blevesearch/vellum). Maps terms to IDs in ~5x less space than a sorted string array and supports prefix/range iteration for free.
- **Roaring Bitmaps** — posting lists encoded as [roaring bitmaps](https://github.com/RoaringBitmap/roaring). Boolean AND/OR/NOT directly on the compressed form.
- **Segment-based indexing** — immutable segments uploaded to S3 (MinIO for dev). Stateless searchers pull only what they need via HTTP range-GETs.
- **BM25 scoring** — standard relevance ranking with per-segment doclen arrays.
- **WAL + memtable** — write-ahead log for crash safety, in-memory inverted index that flushes to S3 on threshold.
- **Versioned manifests** — atomic single-writer discipline via S3 conditional PUT (`If-None-Match: *`).
- **Size-tiered compaction** — small segments accumulate until ≥4 share a size tier (bucketed by `bits.Len64(bytes)`), then get merged into one. Merge re-indexes all non-deleted docs newest-segment-first (enables `_id` dedup), writes a new segment, marks sources `deleted:true` in the manifest. Orphaned files are GC'd separately.

Nothing here is production-grade. It is intentionally simplified to make the concepts visible.

## Architecture

```
Client → HTTP Server → Indexer → WAL → Memtable → .seg + .hotcache → S3
                     → Searcher → Manifest → Hotcache (RAM LRU) → range-GET → .seg
```

Each segment produces two S3 objects:
- `.seg` — postings (roaring), positions (varint), doc store (zstd blocks), doclens
- `.hotcache` — FST + offset table. Fetched once per segment, cached in RAM. Tells the searcher exactly which byte ranges to fetch from `.seg`.

## Running locally

Requires Docker.

```bash
make up        # starts MinIO + s3search + Prometheus + Grafana
make seed      # creates a sample index and ingests logs
make query     # runs a test search
```

Or generate and ingest synthetic EDIFACT messages:

```bash
go run ./tools/generate -n 100000 -o edifact.ndjson
# create index first via UI at http://localhost:7700
curl -X POST http://localhost:7700/index/edifact/docs \
  -H "Content-Type: application/x-ndjson" --data-binary @edifact.ndjson
```

**Endpoints:**
- Search UI: http://localhost:7700
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- MinIO console: http://localhost:9001

## Project layout

```
cmd/s3search/        CLI entry point + config
pkg/analyze/         tokenizer (UAX#29 word boundary, lowercase, ASCII fold)
pkg/segment/         .seg + .hotcache format, writer, range-GET reader
pkg/postings/        roaring bitmap wrappers, position encoding
pkg/docstore/        zstd block store for source documents
pkg/wal/             write-ahead log (length-prefix + CRC32 framed)
pkg/index/           memtable, manifest, compactor, delete-by-query
pkg/query/           DSL parser, query-string parser, BM25 scorer, executor
pkg/search/          multi-segment coordinator
pkg/server/          HTTP API
tools/generate/      synthetic EDIFACT generator
docs/                architecture diagram (HTML)
monitoring/          Prometheus config + Grafana dashboard
```

## What I learned

The interesting bits:

- **Range-GET as a design primitive** — the entire hotcache/segment split exists so that a query never downloads a full segment. FST lookup → exact byte offset → one range-GET per term. Cold query ≈ 5–8 S3 API calls regardless of segment size.
- **FST construction requires sorted input** — terms must be inserted in lexicographic order. This forces a sort during flush, which in turn requires the memtable to hold all terms for a field in memory.
- **Roaring bitmap intersections are fast** — `bitmap.And(other)` on the encoded form without full decode. This is what makes boolean queries cheap.
- **Immutable segments make caching trivial** — a hotcache entry for a given segment ID is valid forever. No invalidation logic needed.
- **WAL framing matters** — line-delimited JSON breaks on a torn write (partial newline). CRC-framed records let replay stop exactly at the corruption boundary and truncate cleanly.
- **Size-tiered compaction tradeoffs** — many small segments = many S3 GETs per query (one hotcache load per segment). Compaction reduces query fan-out but requires re-indexing all docs from merged segments. Time-windowed compaction (group by timestamp bucket) would be better for append-only log workloads, but size-tiered works for general FTS.
