package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
)

// ---------- group commit (node side) ----------

type commitReq struct {
	entry Entry
	done  chan error
}

type committer struct {
	mu      sync.Mutex
	pending []*commitReq
	running bool
}

// handleCommit is called by the pre-receive hook once its pack is in S3.
func (n *node) handleCommit(w http.ResponseWriter, r *http.Request, repo string) {
	if r.Method != http.MethodPost || !validRepo.MatchString(repo) ||
		subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Gitwal-Token")), []byte(n.token)) != 1 {
		http.NotFound(w, r)
		return
	}
	var e Entry
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch err := n.commit(repo, e); {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, errConflict):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, errLost):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// commit queues e and blocks until it is in the index or rejected. Entries that
// arrive while a CAS is in flight ride the next one: one S3 round trip per batch, not per push.
func (n *node) commit(repo string, e Entry) error {
	v, _ := n.commits.LoadOrStore(repo, &committer{})
	c := v.(*committer)
	req := &commitReq{entry: e, done: make(chan error, 1)}
	c.mu.Lock()
	c.pending = append(c.pending, req)
	start := !c.running
	c.running = true
	c.mu.Unlock()
	if start {
		go func() {
			for {
				c.mu.Lock()
				batch := c.pending
				c.pending = nil
				if len(batch) == 0 {
					c.running = false
					c.mu.Unlock()
					return
				}
				c.mu.Unlock()
				n.commitBatch(context.Background(), repo, batch)
			}
		}()
	}
	return <-req.done
}

// commitBatch validates entries in order against the index plus earlier entries of the
// same batch, then publishes all accepted ones with a single CAS.
// ponytail: one batch in flight per repo per node; across nodes the CAS still arbitrates.
func (n *node) commitBatch(ctx context.Context, repo string, batch []*commitReq) {
	reply := func(err error, reqs ...*commitReq) {
		for _, r := range reqs {
			r.done <- err
		}
	}
	// ponytail: five lost CAS attempts per batch; each attempt only loses to another node.
	for attempt := 0; attempt < 5; attempt++ {
		idx, etag, _, err := n.store.getIndex(ctx, repo, "")
		if err != nil {
			reply(err, batch...)
			return
		}
		var accepted, rejected []*commitReq
		for _, r := range batch {
			if stale(idx, r.entry.Refs) {
				rejected = append(rejected, r)
				continue
			}
			idx.Entries = append(idx.Entries, r.entry)
			accepted = append(accepted, r)
		}
		if len(accepted) > 0 {
			if _, err = n.store.putIndex(ctx, repo, idx, etag); errors.Is(err, errConflict) {
				continue
			} else if err != nil {
				reply(err, batch...)
				return
			}
		}
		reply(nil, accepted...)
		reply(errConflict, rejected...)
		return
	}
	reply(errLost, batch...)
}

// stale reports whether any "old new ref" line disagrees with the ref's current value in idx.
// ponytail: scan backward instead of maintaining a second ref index; compact when WAL size matters.
func stale(idx Index, refs []string) bool {
	for _, ref := range refs {
		f := strings.Fields(ref)
		if len(f) != 3 {
			continue
		}
		want := strings.Repeat("0", len(f[0]))
		found := false
	findRef:
		for i := len(idx.Entries) - 1; i >= 0; i-- {
			for _, prior := range idx.Entries[i].Refs {
				p := strings.Fields(prior)
				if len(p) == 3 && p[2] == f[2] {
					want, found = p[1], true
					break findRef
				}
			}
		}
		if !found {
			if sha, ok := idx.Base.Refs[f[2]]; ok {
				want = sha
			}
		}
		if f[0] != want {
			return true
		}
	}
	return false
}
