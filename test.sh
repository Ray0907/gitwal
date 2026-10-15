#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORK=/tmp/gitwal-e2e
MINIO_DATA=$WORK/minio
A_DATA=/tmp/gitwal-A
B_DATA=/tmp/gitwal-B
A_HOST=127.0.0.1:8081
B_HOST=127.0.0.1:8082
PEERS=$A_HOST,$B_HOST
A_HTTP=http://$A_HOST
B_HTTP=http://$B_HOST

export S3_ENDPOINT=127.0.0.1:9000
export S3_KEY=minioadmin
export S3_SECRET=minioadmin
export GITWAL_BUCKET=gitwal

A_PID= B_PID= MINIO_PID= GATE_PID=
cleanup() {
    [ -z "$A_PID" ] || kill "$A_PID" 2>/dev/null || true
    [ -z "$B_PID" ] || kill "$B_PID" 2>/dev/null || true
    [ -z "$MINIO_PID" ] || kill "$MINIO_PID" 2>/dev/null || true
    [ -z "$GATE_PID" ] || kill "$GATE_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

wait_url() {
    url=$1
    pid=$2
    i=0
    while [ "$i" -lt 100 ]; do
        kill -0 "$pid" 2>/dev/null || return 1
        curl -sS "$url" >/dev/null 2>&1 && return 0
        i=$((i + 1))
        sleep .1
    done
    return 1
}

wait_ref() {
    dir=$1
    want=$2
    i=0
    while [ "$i" -lt 40 ]; do
        got=$(git -C "$dir" rev-parse refs/heads/main 2>/dev/null || true)
        [ "$got" = "$want" ] && return 0
        i=$((i + 1))
        sleep .05
    done
    return 1
}

commit_file() {
    repo=$1
    file=$2
    text=$3
    printf '%s\n' "$text" >>"$repo/$file"
    git -C "$repo" add "$file"
    git -C "$repo" -c user.name=gitwal -c user.email=gitwal@example.com commit -qm "$text"
}

start_a() {
    ./gitwal serve -listen "$A_HOST" -self "$A_HOST" -peers "$PEERS" \
        -compact-after 3 -data "$A_DATA" >"$WORK/A.log" 2>&1 &
    A_PID=$!
    wait_url "$A_HTTP/" "$A_PID"
}

start_b() {
    ./gitwal serve -listen "$B_HOST" -self "$B_HOST" -peers "$PEERS" \
        -compact-after 3 -idle 2s -gc-interval 500ms -data "$B_DATA" >"$WORK/B.log" 2>&1 &
    B_PID=$!
    wait_url "$B_HTTP/" "$B_PID"
}

rm -rf "$WORK" "$A_DATA" "$B_DATA"
mkdir -p "$MINIO_DATA"
command -v minio >/dev/null
command -v jq >/dev/null
MINIO_ROOT_USER=$S3_KEY MINIO_ROOT_PASSWORD=$S3_SECRET \
    minio server --quiet --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 "$MINIO_DATA" \
    >"$WORK/minio.log" 2>&1 &
MINIO_PID=$!
wait_url http://127.0.0.1:9000/minio/health/live "$MINIO_PID" || {
    cat "$WORK/minio.log"
    exit 1
}

cd "$ROOT"
go build -o gitwal .
start_a
start_b

P_HOST=$(curl -fsS "$A_HTTP/_/primary/demo")
case "$P_HOST" in
    "$A_HOST") P_HTTP=$A_HTTP; P_DATA=$A_DATA; R_HTTP=$B_HTTP; R_DATA=$B_DATA; R_NODE=B ;;
    "$B_HOST") P_HTTP=$B_HTTP; P_DATA=$B_DATA; R_HTTP=$A_HTTP; R_DATA=$A_DATA; R_NODE=A ;;
    *) echo "unexpected primary: $P_HOST" >&2; exit 1 ;;
esac
P=$P_HTTP/demo.git
R=$R_HTTP/demo.git
curl -sS -D "$WORK/redirect.headers" -o /dev/null "$R/info/refs?service=git-receive-pack"
grep -Eq '^HTTP/[0-9.]+ 302' "$WORK/redirect.headers"

SRC=$WORK/source
git init -q -b main "$SRC"
commit_file "$SRC" README.md first
FIRST=$(git -C "$SRC" rev-parse HEAD)
git -C "$SRC" remote add origin "$R"
git -C "$SRC" push -q origin main
curl -fsS "$P_HTTP/_/index/demo" | jq -e '.entries | length == 1' >/dev/null
wait_ref "$R_DATA/demo.git" "$FIRST"

git clone -q "$R" "$WORK/clone-r"
test "$(git -C "$WORK/clone-r" rev-parse HEAD)" = "$FIRST"

git -C "$SRC" remote set-url origin "$P"
i=1
while [ "$i" -le 4 ]; do
    commit_file "$SRC" README.md "compact-$i"
    git -C "$SRC" push -q origin main
    i=$((i + 1))
done
LATEST=$(git -C "$SRC" rev-parse HEAD)
INDEX=$(curl -fsS "$P_HTTP/_/index/demo")
test "$(printf '%s' "$INDEX" | jq '.entries | length')" -lt 3
printf '%s' "$INDEX" | jq -e '.base.id | length > 0' >/dev/null

git -C "$SRC" branch delete-me
git -C "$SRC" push -q origin delete-me
git -C "$SRC" push -q origin :delete-me
test -z "$(git ls-remote "$R" refs/heads/delete-me)"

if [ "$R_NODE" = A ]; then
    kill "$A_PID"; wait "$A_PID" 2>/dev/null || true; A_PID=
    rm -rf "$A_DATA"
    start_a
else
    kill "$B_PID"; wait "$B_PID" 2>/dev/null || true; B_PID=
    rm -rf "$B_DATA"
    start_b
fi
rm -rf "$WORK/rebuilt-r"
git clone -q "$R" "$WORK/rebuilt-r"
test "$(git -C "$WORK/rebuilt-r" rev-parse HEAD)" = "$LATEST"
git -C "$WORK/rebuilt-r" fsck --full --strict
test "$(find "$R_DATA/demo.git/objects/pack" -name '*.pack' | wc -l | tr -d ' ')" = 1

sleep 3
test ! -d "$R_DATA/demo.git"
test -d "$P_DATA/demo.git"
git clone -q "$R" "$WORK/after-gc"
test "$(git -C "$WORK/after-gc" rev-parse HEAD)" = "$LATEST"

git clone -q "$P" "$WORK/racer-a"
git clone -q "$P" "$WORK/racer-b"
dd if=/dev/urandom of="$WORK/racer-a/a.bin" bs=1048576 count=16 2>/dev/null
dd if=/dev/urandom of="$WORK/racer-b/b.bin" bs=1048576 count=16 2>/dev/null
commit_file "$WORK/racer-a" a.bin racer-a
commit_file "$WORK/racer-b" b.bin racer-b

# Hold index writes until both packs are uploaded: force a real CAS race.
cat >"$WORK/s3-gate.go" <<'EOF'
package main

import (
    "log"
    "net/http"
    "net/http/httputil"
    "net/url"
    "os"
    "strings"
    "sync/atomic"
    "time"
)

func main() {
    target, err := url.Parse("http://" + os.Args[1])
    if err != nil { log.Fatal(err) }
    proxy := httputil.NewSingleHostReverseProxy(target)
    ready := make(chan struct{})
    var writes atomic.Int32
    log.Fatal(http.ListenAndServe("127.0.0.1:9002", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/index.json") {
            if writes.Add(1) == 2 { close(ready) }
            select {
            case <-ready:
            case <-time.After(10 * time.Second):
                http.Error(w, "second push did not reach CAS", http.StatusGatewayTimeout)
                return
            }
        }
        proxy.ServeHTTP(w, r)
    })))
}
EOF
go build -o "$WORK/s3-gate" "$WORK/s3-gate.go"
"$WORK/s3-gate" "$S3_ENDPOINT" >"$WORK/s3-gate.log" 2>&1 &
GATE_PID=$!
wait_url http://127.0.0.1:9002/ "$GATE_PID"
cp "$P_DATA/demo.git/hooks/pre-receive" "$WORK/pre-receive"
printf '#!/bin/sh\nexport S3_ENDPOINT=127.0.0.1:9002\nexec "%s" hook\n' "$ROOT/gitwal" >"$P_DATA/demo.git/hooks/pre-receive"

set +e
git -C "$WORK/racer-a" -c http.postBuffer=67108864 push origin main >"$WORK/push-a.log" 2>&1 &
PUSH_A=$!
git -C "$WORK/racer-b" -c http.postBuffer=67108864 push origin main >"$WORK/push-b.log" 2>&1 &
PUSH_B=$!
wait "$PUSH_A"; A_STATUS=$?
wait "$PUSH_B"; B_STATUS=$?
set -e

if [ "$A_STATUS" -eq 0 ] && [ "$B_STATUS" -ne 0 ]; then
    LOSER=$WORK/racer-b
    LOSER_LOG=$WORK/push-b.log
elif [ "$B_STATUS" -eq 0 ] && [ "$A_STATUS" -ne 0 ]; then
    LOSER=$WORK/racer-a
    LOSER_LOG=$WORK/push-a.log
else
    cat "$WORK/push-a.log" "$WORK/push-b.log"
    echo "expected exactly one concurrent push to fail" >&2
    exit 1
fi
grep -q 'concurrent push' "$LOSER_LOG"

# A rejected push must not leave an unreferenced WAL pack in S3.
curl -fsS "$P_HTTP/_/index/demo" >"$WORK/race-index.json"
if command -v mc >/dev/null 2>&1; then
    MC_HOST_gitwal="http://$S3_KEY:$S3_SECRET@$S3_ENDPOINT" \
        mc ls --json "gitwal/$GITWAL_BUCKET/demo/wal/" >"$WORK/wal-list.json"
    jq -r '.key' "$WORK/wal-list.json" >"$WORK/wal-keys"
else
    find "$MINIO_DATA/$GITWAL_BUCKET/demo/wal" -name '*.pack' -exec basename {} \; >"$WORK/wal-keys"
fi
test -s "$WORK/wal-keys"
while IFS= read -r key; do
    jq -e --arg id "${key%.pack}" \
        '.base.id == $id or any(.entries[]; .pack and .id == $id)' \
        "$WORK/race-index.json" >/dev/null || {
        echo "orphan WAL pack: $key" >&2
        exit 1
    }
done <"$WORK/wal-keys"

git -C "$LOSER" pull -q --rebase origin main
git -C "$LOSER" -c http.postBuffer=67108864 push -q origin main

# Finish primary compaction before resetting the gate for the next race.
curl -fsS "$P/info/refs?service=git-upload-pack" -o /dev/null
curl -fsS "$P_HTTP/_/index/demo" | jq -e '.entries | length < 3' >/dev/null

# Reset the gate so the different-branch test also forces a CAS conflict.
kill "$GATE_PID"; wait "$GATE_PID" 2>/dev/null || true
"$WORK/s3-gate" "$S3_ENDPOINT" >"$WORK/s3-gate.log" 2>&1 &
GATE_PID=$!
wait_url http://127.0.0.1:9002/ "$GATE_PID"

# Different branches may race on the index, but neither push should be rejected.
for who in a b; do
    commit_file "$WORK/racer-$who" "branch-$who.txt" "branch-$who"
done
set +e
git -C "$WORK/racer-a" -c http.postBuffer=67108864 push -q origin HEAD:refs/heads/branch-a >"$WORK/branch-a.log" 2>&1 &
BRANCH_A=$!
git -C "$WORK/racer-b" -c http.postBuffer=67108864 push -q origin HEAD:refs/heads/branch-b >"$WORK/branch-b.log" 2>&1 &
BRANCH_B=$!
wait "$BRANCH_A"; A_STATUS=$?
wait "$BRANCH_B"; B_STATUS=$?
set -e
cp "$WORK/pre-receive" "$P_DATA/demo.git/hooks/pre-receive"
if [ "$A_STATUS" -ne 0 ] || [ "$B_STATUS" -ne 0 ]; then
    cat "$WORK/branch-a.log" "$WORK/branch-b.log" >&2
    exit 1
fi
for who in a b; do
    test "$(git ls-remote "$R" "refs/heads/branch-$who" | cut -f1)" = "$(git -C "$WORK/racer-$who" rev-parse HEAD)"
done

# Primary down: a replica must accept the push itself; the restarted primary catches up from S3.
if [ "$R_NODE" = A ]; then
    kill "$B_PID"; wait "$B_PID" 2>/dev/null || true; B_PID=
else
    kill "$A_PID"; wait "$A_PID" 2>/dev/null || true; A_PID=
fi
commit_file "$SRC" README.md failover
git -C "$SRC" pull -q --rebase "$R" main
commit_file "$SRC" README.md failover-2
FAILOVER=$(git -C "$SRC" rev-parse HEAD)
git -C "$SRC" push -q "$R" main
test "$(git ls-remote "$R" refs/heads/main | cut -f1)" = "$FAILOVER"
if [ "$R_NODE" = A ]; then start_b; else start_a; fi
test "$(git ls-remote "$P" refs/heads/main | cut -f1)" = "$FAILOVER"

echo PASS
