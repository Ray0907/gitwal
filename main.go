// gitwal: a Git host in the shape of Cursor's Continuity.
// S3 holds the write-ahead log and is the only source of truth; each node keeps
// plain bare repos on local disk as a cache and catches up from the WAL before
// serving any request. Pushes are linearized by a compare-and-swap on the
// per-repo index object.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gitwal serve|hook")
		os.Exit(2)
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ExitOnError)
		listen := fs.String("listen", ":8080", "HTTP and UDP address")
		data := fs.String("data", "./data", "local repo cache dir")
		self := fs.String("self", "", "this node's advertised host:port")
		peers := fs.String("peers", "", "comma-separated host:port nodes")
		compactAfter := fs.Int("compact-after", 32, "compact after this many WAL entries")
		idle := fs.Duration("idle", 10*time.Minute, "remove idle non-primary replicas after this duration")
		gcInterval := fs.Duration("gc-interval", time.Minute, "replica GC scan interval")
		fs.Parse(os.Args[2:])
		if err := serve(ctx, *listen, *data, *self, *peers, *compactAfter, *idle, *gcInterval); err != nil {
			log.Fatal(err)
		}
	case "hook":
		if err := hook(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		os.Exit(2)
	}
}

// ---------- node ----------

type state struct {
	BaseID  string `json:"baseID"`
	Applied int    `json:"applied"` // entries replayed into the local repo
	ETag    string `json:"etag"`    // index etag those entries came from
}

type node struct {
	data         string
	store        *Store
	git          string // path to git-http-backend
	bin          string // this executable, for the hook
	self         string
	peers        []string
	compactAfter int
	idle         time.Duration
	gcInterval   time.Duration
	locks        sync.Map
}

var (
	repoName  = regexp.MustCompile(`^/([A-Za-z0-9._-]+?)(\.git)?(/|$)`)
	validRepo = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

func serve(ctx context.Context, listen, data, self, peerList string, compactAfter int, idle, gcInterval time.Duration) error {
	store, err := newStore(ctx)
	if err != nil {
		return err
	}
	execPath, err := exec.Command("git", "--exec-path").Output()
	if err != nil {
		return err
	}
	bin, _ := os.Executable()
	data, _ = filepath.Abs(data)
	os.MkdirAll(data, 0o755)
	if self == "" {
		self = listen
		if strings.HasPrefix(self, ":") {
			self = "127.0.0.1" + self
		}
	}
	if peerList == "" {
		peerList = self
	}
	var peers []string
	for _, peer := range strings.Split(peerList, ",") {
		if peer = strings.TrimSpace(peer); peer != "" {
			peers = append(peers, peer)
		}
	}
	n := &node{data: data, store: store, bin: bin, self: self, peers: peers,
		compactAfter: compactAfter, idle: idle, gcInterval: gcInterval,
		git: filepath.Join(strings.TrimSpace(string(execPath)), "git-http-backend")}
	udp, err := net.ListenPacket("udp", listen)
	if err != nil {
		return err
	}
	defer udp.Close()
	go n.gossip(ctx, udp)
	go n.gc(ctx)
	log.Printf("gitwal serving %s on %s (primary identity %s)", data, listen, self)
	return http.ListenAndServe(listen, n)
}

func (n *node) primary(repo string) string {
	var best string
	var score uint64
	for _, peer := range n.peers {
		h := fnv.New64()
		io.WriteString(h, repo)
		h.Write([]byte{0})
		io.WriteString(h, peer)
		if sum := h.Sum64(); best == "" || sum > score {
			best, score = peer, sum
		}
	}
	return best
}

func (n *node) gossip(ctx context.Context, conn net.PacketConn) {
	buf := make([]byte, 1024)
	for {
		sz, _, err := conn.ReadFrom(buf)
		if err != nil {
			return
		}
		repo := string(buf[:sz])
		if validRepo.MatchString(repo) {
			go func() {
				if err := n.sync(ctx, repo); err != nil {
					log.Printf("gossip sync %s: %v", repo, err)
				}
			}()
		}
	}
}

func (n *node) gc(ctx context.Context) {
	if n.gcInterval <= 0 || n.idle <= 0 {
		return
	}
	ticker := time.NewTicker(n.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, _ := os.ReadDir(n.data)
			for _, entry := range entries {
				if !entry.IsDir() || !strings.HasSuffix(entry.Name(), ".git") {
					continue
				}
				repo := strings.TrimSuffix(entry.Name(), ".git")
				if n.primary(repo) == n.self {
					continue
				}
				mu, _ := n.locks.LoadOrStore(repo, &sync.Mutex{})
				mu.(*sync.Mutex).Lock()
				stateFile := filepath.Join(n.data, entry.Name(), "gitwal.json")
				if info, err := os.Stat(stateFile); err == nil && time.Since(info.ModTime()) > n.idle {
					if err := os.RemoveAll(filepath.Dir(stateFile)); err != nil {
						log.Printf("gc %s: %v", repo, err)
					}
				}
				mu.(*sync.Mutex).Unlock()
			}
		}
	}
}

func (n *node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/_/primary/") {
		repo := strings.TrimPrefix(r.URL.Path, "/_/primary/")
		if !validRepo.MatchString(repo) {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, n.primary(repo))
		return
	}
	if strings.HasPrefix(r.URL.Path, "/_/index/") {
		repo := strings.TrimPrefix(r.URL.Path, "/_/index/")
		if !validRepo.MatchString(repo) {
			http.NotFound(w, r)
			return
		}
		idx, _, _, err := n.store.getIndex(r.Context(), repo, "")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(idx)
		return
	}
	m := repoName.FindStringSubmatch(r.URL.Path)
	if m == nil {
		http.NotFound(w, r)
		return
	}
	repo := m[1]
	if r.Method == http.MethodGet && r.URL.Query().Get("service") == "git-receive-pack" {
		// Redirect pushes to the primary only while it answers; otherwise accept here.
		// Any node can take a push safely, the S3 CAS is what keeps it correct.
		// ponytail: liveness is one TCP dial per push discovery; cache it if push rate makes this matter.
		if p := n.primary(repo); p != n.self && reachable(p) {
			http.Redirect(w, r, "http://"+p+r.URL.RequestURI(), http.StatusFound)
			return
		}
	}
	if err := n.sync(r.Context(), repo); err != nil {
		log.Printf("sync %s: %v", repo, err)
		http.Error(w, err.Error(), 500)
		return
	}
	h := &cgi.Handler{
		Path: n.git,
		Dir:  n.data,
		Env: []string{
			"GIT_PROJECT_ROOT=" + n.data,
			"GIT_HTTP_EXPORT_ALL=1",
			"GITWAL_BIN=" + n.bin,
			"GITWAL_SELF=" + n.self,
			"GITWAL_PEERS=" + strings.Join(n.peers, ","),
		},
		InheritEnv: []string{"PATH", "HOME", "S3_ENDPOINT", "S3_KEY", "S3_SECRET", "S3_SSL", "GITWAL_BUCKET"},
	}
	h.ServeHTTP(w, r)
}

// sync materializes the repo if missing and replays any WAL entries we have not applied.
func (n *node) sync(ctx context.Context, repo string) error {
	mu, _ := n.locks.LoadOrStore(repo, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	dir := filepath.Join(n.data, repo+".git")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := n.initRepo(dir); err != nil {
			return err
		}
	}
	var st state
	stateFile := filepath.Join(dir, "gitwal.json")
	if b, err := os.ReadFile(stateFile); err == nil {
		json.Unmarshal(b, &st)
	}
	idx, etag, changed, err := n.store.getIndex(ctx, repo, st.ETag)
	if err != nil {
		return err
	}
	if !changed {
		now := time.Now()
		return os.Chtimes(stateFile, now, now)
	}
	if idx.Base.ID != st.BaseID || len(idx.Entries) < st.Applied {
		if idx.Base.ID == "" {
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
			if err := n.initRepo(dir); err != nil {
				return err
			}
		} else if err := n.applyBase(ctx, repo, dir, idx.Base); err != nil {
			return fmt.Errorf("apply base %s: %w", idx.Base.ID, err)
		}
		st = state{BaseID: idx.Base.ID}
	}
	for _, e := range idx.Entries[st.Applied:] {
		if err := n.apply(ctx, repo, dir, e); err != nil {
			return fmt.Errorf("apply %s: %w", e.ID, err)
		}
	}
	b, _ := json.Marshal(state{BaseID: idx.Base.ID, Applied: len(idx.Entries), ETag: etag})
	if err := os.WriteFile(stateFile, b, 0o644); err != nil {
		return err
	}
	if n.primary(repo) == n.self && n.compactAfter > 0 && len(idx.Entries) >= n.compactAfter {
		return n.compact(ctx, repo, dir, stateFile, idx, etag)
	}
	return nil
}

func (n *node) compact(ctx context.Context, repo, dir, stateFile string, old Index, etag string) error {
	if out, err := exec.Command("git", "-C", dir, "repack", "-a", "-d", "-q").CombinedOutput(); err != nil {
		return fmt.Errorf("repack: %s", out)
	}
	out, err := exec.Command("git", "-C", dir, "for-each-ref", "--format=%(objectname) %(refname)").Output()
	if err != nil {
		return err
	}
	refs := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			refs[f[1]] = f[0]
		}
	}
	packs, _ := filepath.Glob(filepath.Join(dir, "objects", "pack", "pack-*.pack"))
	if len(packs) == 0 {
		return errors.New("repack produced no pack")
	}
	pack := packs[0]
	if len(packs) > 1 {
		log.Printf("compact %s: repack left %d packs; using newest", repo, len(packs))
		for _, candidate := range packs[1:] {
			ci, cerr := os.Stat(candidate)
			pi, perr := os.Stat(pack)
			if cerr == nil && (perr != nil || ci.ModTime().After(pi.ModTime())) {
				pack = candidate
			}
		}
	}
	var rnd [4]byte
	io.ReadFull(rand.Reader, rnd[:])
	id := fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(rnd[:]))
	if err := n.store.putBase(ctx, repo, id, pack); err != nil {
		return err
	}
	newETag, err := n.store.putIndex(ctx, repo, Index{Base: Base{ID: id, Refs: refs}, Entries: []Entry{}}, etag)
	if errors.Is(err, errConflict) {
		_ = n.store.remove(ctx, baseKey(repo, id))
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range old.Entries {
		if entry.Pack {
			if err := n.store.remove(ctx, packKey(repo, entry.ID)); err != nil {
				log.Printf("compact %s cleanup %s: %v", repo, entry.ID, err)
			}
		}
	}
	if old.Base.ID != "" {
		if err := n.store.remove(ctx, baseKey(repo, old.Base.ID)); err != nil {
			log.Printf("compact %s cleanup base %s: %v", repo, old.Base.ID, err)
		}
	}
	b, _ := json.Marshal(state{BaseID: id, Applied: 0, ETag: newETag})
	return os.WriteFile(stateFile, b, 0o644)
}

func (n *node) applyBase(ctx context.Context, repo, dir string, base Base) error {
	rc, err := n.store.getBase(ctx, repo, base.ID)
	if err != nil {
		return err
	}
	cmd := exec.Command("git", "-C", dir, "index-pack", "--stdin")
	cmd.Stdin = rc
	out, err := cmd.CombinedOutput()
	rc.Close()
	if err != nil {
		return fmt.Errorf("index-pack: %s", out)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return errors.New("index-pack returned no hash")
	}
	keep := "pack-" + fields[len(fields)-1]
	current, err := exec.Command("git", "-C", dir, "for-each-ref", "--format=%(refname)").Output()
	if err != nil {
		return err
	}
	var updates strings.Builder
	for _, ref := range strings.Fields(string(current)) {
		if _, ok := base.Refs[ref]; !ok {
			fmt.Fprintf(&updates, "delete %s\n", ref)
		}
	}
	for ref, sha := range base.Refs {
		fmt.Fprintf(&updates, "update %s %s\n", ref, sha)
	}
	cmd = exec.Command("git", "-C", dir, "update-ref", "--stdin")
	cmd.Stdin = strings.NewReader(updates.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("update-ref: %s", out)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "objects", "pack", "pack-*"))
	// ponytail: upload-pack may still hold an old pack; a failed client retries after this sync.
	for _, file := range files {
		name := filepath.Base(file)
		if !strings.HasPrefix(name, keep+".") {
			if err := os.Remove(file); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *node) initRepo(dir string) error {
	steps := [][]string{
		{"init", "--bare", "-q", dir},
		{"-C", dir, "config", "http.receivepack", "true"},
		{"-C", dir, "config", "receive.unpackLimit", "1"}, // always keep pushes as packfiles
	}
	for _, a := range steps {
		if out, err := exec.Command("git", a...).CombinedOutput(); err != nil {
			return fmt.Errorf("git %v: %s", a, out)
		}
	}
	hook := "#!/bin/sh\nexec \"$GITWAL_BIN\" hook\n"
	return os.WriteFile(filepath.Join(dir, "hooks", "pre-receive"), []byte(hook), 0o755)
}

func (n *node) apply(ctx context.Context, repo, dir string, e Entry) error {
	if e.Pack {
		rc, err := n.store.getPack(ctx, repo, e.ID)
		if err != nil {
			return err
		}
		defer rc.Close()
		cmd := exec.Command("git", "-C", dir, "index-pack", "--stdin", "--fix-thin")
		cmd.Stdin = rc
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("index-pack: %s", out)
		}
	}
	var sb strings.Builder
	for _, line := range e.Refs {
		f := strings.Fields(line)
		if len(f) != 3 {
			continue
		}
		if strings.Trim(f[1], "0") == "" {
			fmt.Fprintf(&sb, "delete %s\n", f[2])
		} else {
			fmt.Fprintf(&sb, "update %s %s\n", f[2], f[1]) // WAL is truth: no old-value check
		}
	}
	cmd := exec.Command("git", "-C", dir, "update-ref", "--stdin")
	cmd.Stdin = strings.NewReader(sb.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("update-ref: %s", out)
	}
	return nil
}

// ---------- hook (pre-receive, run by git inside the bare repo) ----------

func hook(ctx context.Context) error {
	store, err := newStore(ctx)
	if err != nil {
		return err
	}
	cwd, _ := os.Getwd()
	repo := strings.TrimSuffix(filepath.Base(cwd), ".git")

	var refs []string
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); l != "" {
			refs = append(refs, l)
		}
	}
	packs, _ := filepath.Glob(filepath.Join(os.Getenv("GIT_QUARANTINE_PATH"), "pack", "*.pack"))

	idx, etag, _, err := store.getIndex(ctx, repo, "")
	if err != nil {
		return err
	}
	// ponytail: scan backward instead of maintaining a second ref index; compact when WAL size matters.
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
			return errConflict
		}
	}
	var rnd [4]byte
	io.ReadFull(rand.Reader, rnd[:])
	e := Entry{ID: fmt.Sprintf("%012d-%s", len(idx.Entries)+1, hex.EncodeToString(rnd[:])), Refs: refs}
	for _, p := range packs {
		if err := store.putPack(ctx, repo, e.ID, p); err != nil {
			return err
		}
		e.Pack = true
		break // ponytail: receive-pack produces one pack per push
	}
	idx.Entries = append(idx.Entries, e)
	// Durable + linearized before git touches a single local ref.
	if _, err = store.putIndex(ctx, repo, idx, etag); err != nil {
		return err
	}
	self := os.Getenv("GITWAL_SELF")
	for _, peer := range strings.Split(os.Getenv("GITWAL_PEERS"), ",") {
		if peer == "" || peer == self {
			continue
		}
		conn, dialErr := net.Dial("udp", peer)
		if dialErr == nil {
			_, _ = conn.Write([]byte(repo))
			_ = conn.Close()
		}
	}
	return nil
}

func reachable(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}
