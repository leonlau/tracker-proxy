# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

BitTorrent tracker proxy. Single-binary Go service that fronts multiple upstream HTTP and UDP trackers, merges peers, caches results in memory, and speaks the standard `/announce` HTTP endpoint that qBittorrent (and any other BitTorrent client) can consume directly.

## Common commands

```bash
# Build
go build ./...

# Run (listens on :8080)
go run .

# Vet
go vet ./...

# All tests
go test ./... -count=1

# Single test, verbose
go test ./... -run TestBuildAnnouncePacket_Layout -v -count=1

# Subtests of a parent
go test ./... -run 'TestDedupeIPv4Peers/' -v -count=1
```

## Architecture

Two-file project (`main.go`, `main_test.go`) — no sub-packages, no interfaces, no DI. All request handling lives in `main.go` and is small enough to hold in your head end-to-end.

**Request path** (everything keyed on the original `info_hash` from the query string):

1. `announceHandler` (main.go) — entry point on `GET /announce`.
   - Pulls the 20 raw bytes of `info_hash` from `r.URL.Query()` via `rawInfoHash` (NB: `q.Get` URL-decodes, so the value is already the binary form — do not call `Get` for binary fields).
   - Cache hit → return immediately.
   - Otherwise wraps the upstream fan-out in `context.WithTimeout(r.Context(), overallTimeout)`.
2. `queryUpstreams` — fans out to every URL in `httpTrackers` and `udpTrackers` concurrently. The result channel is **buffered to `len(httpTrackers)+len(udpTrackers)`** — without this, writers block on the channel before `wg.Wait()` can finish and the process deadlocks once you add a few trackers. Each writer uses `result <- p / ctx.Done()` so a cancelled context unblocks producers immediately.
3. Per-upstream queries:
   - `queryHTTPTracker` — `http.Client` with `upstreamHTTPTimeout`. Bencode-decodes the response into a struct with `Peers []byte`. All errors log via `slog.Warn` (no silent failures).
   - `queryUDPTracker` — BEP 15 two-step handshake:
     - **connect**: 16-byte request `magic(8) + action=0(4) + tx(4)`. Validates `action==0` and `transaction_id` match before accepting the returned `connection_id`.
     - **announce**: 98-byte request built by `buildAnnouncePacket` — every field offset is laid out per BEP 15. Validates `action==1` and `transaction_id` again. Returns peers from `respBuf[20:n]` (skipping `action + tx + interval + leechers + seeders`).
4. `dedupeIPv4Peers` — collapses all returned peer buffers into one dedup'd 6-byte-per-peer slice. Drops trailing partial bytes (no IPv6 handling today).
5. `sendResponse` — bencode-encodes `{interval, min interval, complete, incomplete, peers}` and writes with `Content-Type: text/plain`.
6. Cache write happens after the merge, keyed by the raw 20-byte `info_hash` stringified.

## Cache

`sync.Map` keyed by the 20-byte `info_hash`. `cacheTTL = 5 * time.Minute`. `cacheGet` evicts on read of an expired item. **No size cap** — long-running deployments with many distinct info hashes will grow unboundedly. If you add an LRU or similar, keep reads lock-free on the hot path.

## Critical correctness points

These were bugs at one point and are easy to re-introduce:

- **UDP announce packet layout is exactly BEP 15.** `num_want` lives at `[92:96]`, `port` at `[96:98]`. Earlier revisions had them swapped, which silently broke every UDP upstream.
- **`info_hash` and `peer_id` must reach the upstream as raw 20 bytes**, not as a string. Use `rawInfoHash(q)` / `rawPeerID(q)`, never `q.Get("info_hash")`.
- **The peer-result channel must be buffered** to the upstream count, otherwise the writers block before any reader can drain.
- **Validate UDP responses** on both `action` and `transaction_id` — a misbehaving or hostile tracker can return garbage that you'd otherwise feed into `connectionID`.

## Configuration knobs

All hard-coded near the top of `main.go`:

- `httpTrackers`, `udpTrackers` — upstream lists. Edit in place.
- `cacheTTL`, `upstreamHTTPTimeout`, `upstreamUDPTimeout`, `overallTimeout`
- `defaultNumWant` (50), `defaultListenPort` (6881)

There is no flag/env loader. If you need one, add it next to `main()` — don't introduce a config package for a single-binary project this size.

## Dependencies

Just one: `github.com/zeebo/bencode` for bencode encode/decode. The standard library covers everything else (HTTP, UDP, sync, slog).

Go version is `1.25.1` (see `go.mod`); range-over-int and `any` are in active use.

## Tests

`main_test.go` is white-box (`package main`). Coverage focuses on pure functions where network isn't involved:

- `buildAnnouncePacket` — byte-level assertions on every field offset (catches protocol-layout regressions).
- `dedupeIPv4Peers` — table-driven across 7 scenarios (empty, in-segment dup, cross-segment dup, same-IP-different-port, truncation, order preservation).
- `cacheGet` / `cacheSet` — round-trip + expiry eviction.
- `rawInfoHash` / `rawPeerID` — binary preservation including a URL-encoded round-trip with non-ASCII bytes.
- `eventCode`, `uint64Param`, `uint32Param` — parameter parsing.
- `sendResponse` — reverse-decode the bencode response to assert structure.

End-to-end HTTP tests are **not** included — they would require either a real upstream or a UDP/HTTP mock layer, which isn't worth the complexity for the surface area covered. Add one only if you change `announceHandler` itself.