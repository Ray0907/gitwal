# gitwal

A Git host where S3 is the only source of truth. ~900 lines of Go, three files, no database, no consensus.

Built to reproduce the storage design Cursor describes in [Git at any scale](https://cursor.com/blog/git-at-any-scale) (Continuity): every push is durably written to a write-ahead log in S3 before it is acknowledged, local bare repositories are disposable caches, and every replica is always consistent because reads verify against S3 first.

The push path is also inspired by Anthropic's [Scion](https://www.anthropic.com/engineering/redesigning-source-control): commits should not be blocked by unrelated concurrent writes, and the only global state is a single compare-and-swap. A push that loses the index CAS retries against the fresh index and fails only if a ref it touches really moved. A rejected push deletes the pack it uploaded.

```text
                    S3
          index CAS + WAL/base packs
                 /       \
                /         \
 client ─HTTP→ node A ←UDP→ node B
              primary     replica
                 │           │
             bare cache  bare cache (GC-able)
```

## Guarantees

- A push is acknowledged only after its packfile and ref transaction are in S3.
- All pushes to a repository are linearized by a compare-and-swap on one index object.
- Any node serves any read, and every read is consistent: nodes catch up from the WAL before answering.
- Delete a node's data directory and restart it; it rebuilds every repository from S3.
- Replicas scale horizontally. Idle replicas are garbage collected and rematerialized on demand.

## How it works

**Push.** The client talks to plain `git http-backend`. A `pre-receive` hook uploads the quarantined packfile to `<repo>/wal/<id>.pack`, then hands `{id, refs}` to its own node over a token-protected local endpoint. The node group-commits: entries that arrive while an index write is in flight share the next one, validated in order, published with one `If-Match` CAS on `<repo>/index.json`. An entry whose ref really moved is rejected and its pack deleted. Git updates local refs only after the CAS succeeds.

**Read.** Before serving any request, the node does a conditional GET on the index. A 304 means it is current. A 200 means it replays the missing entries with `git index-pack` and `git update-ref`, then serves.

**Primary.** Rendezvous hashing over the peer list picks a primary per repository. Non-primaries redirect push discovery there with a 302 while the primary answers a TCP dial, and accept the push themselves otherwise. The primary only avoids CAS retries; any node can accept a push correctly.

**Gossip.** After a successful push the hook sends the repository name as a UDP datagram to every peer, which triggers an early catch-up. Lost datagrams do not matter.

**Compaction.** When the WAL exceeds `-compact-after` entries, the primary repacks, uploads one base pack, snapshots refs, and CAS-replaces the index with `{base, entries: []}`. Replicas download the base pack instead of repacking.

## Run

```sh
go build -o gitwal .
S3_ENDPOINT=127.0.0.1:9000 ./gitwal serve \
  -listen 127.0.0.1:8081 \
  -self 127.0.0.1:8081 \
  -peers 127.0.0.1:8081,127.0.0.1:8082 \
  -data ./data

git push http://127.0.0.1:8081/demo.git main
git clone http://127.0.0.1:8082/demo.git
```

S3 settings come from `S3_ENDPOINT`, `S3_KEY`, `S3_SECRET`, `S3_SSL=1`, `GITWAL_BUCKET`. Any S3-compatible store with conditional writes works (AWS S3, MinIO, R2).

| Flag | Default | Purpose |
|---|---:|---|
| `-listen` | `:8080` | HTTP and UDP bind address |
| `-data` | `./data` | Local bare-repository cache |
| `-self` | listen address | Advertised `host:port` identity |
| `-peers` | self only | Comma-separated rendezvous/gossip members |
| `-compact-after` | `32` | WAL entries that trigger primary compaction |
| `-idle` | `10m` | Non-primary cache idle lifetime |
| `-gc-interval` | `60s` | Idle-cache scan interval |

Debug endpoints: `GET /_/primary/<repo>` and `GET /_/index/<repo>`.

## Test

Requires Go, Git, `curl`, `jq`, and a `minio` binary (`brew install minio`). The script starts its own MinIO and two gitwal nodes, then verifies cross-node consistency, rebuild from S3, compaction, gossip, idle GC, a concurrent-push conflict, and pushing through a replica while the primary is down.

```sh
./test.sh
```

## Not implemented

- Cross-node batching. Group commit is per node; nodes still race on the index CAS.
- Cheaper read freshness. Concurrent requests share one index GET per round, but a request that arrives mid-round waits for the next one, so a quiet repo still pays one S3 round trip per request.
- Primary liveness is one TCP dial per push. No retry, no caching, no gossip of health.
- Per-ref index. Pushes to one repository still share one index object; a push gives up after five lost CAS attempts.
- Orphan pack reconciliation. After an ambiguous S3 write error the uploaded pack is kept, since the entry may have committed.
- Authentication. Put it behind a reverse proxy.
- Compaction deletes old packs after the index CAS. A fetch in flight on an old pack fails once and retries.

## License

MIT
