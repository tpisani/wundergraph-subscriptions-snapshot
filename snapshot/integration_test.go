package snapshot_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wundergraph/cosmo/router/core"
	"github.com/wundergraph/cosmo/router/pkg/config"
	"go.uber.org/zap"

	// Registers the module, exactly like cmd/router does.
	_ "github.com/tpisani/wundergraph-subscriptions-snapshot/snapshot"
)

// These tests run the real Cosmo Router in-process, built from a config file
// through the same RouterSupervisor the router binary uses, against an
// in-process subgraph that serves the subscription over SSE.
//
// The subgraph only sends a message when the test publishes one. A message a
// client receives without a publish can therefore only be a snapshot.

const (
	waitTimeout  = 5 * time.Second
	quietTimeout = 700 * time.Millisecond
)

func TestIntegrationLateSubscriberGetsSnapshot(t *testing.T) {
	env := startEnv(t, "")

	alice := env.subscribe(t, sseClient, "Bearer alice", `{"matchId":"m1"}`)
	env.sub.waitOpened(t, 1)
	env.sub.publish(homeGoal)
	alice.expect(t, want("m1", homeGoal))
	env.sub.publish(awayEqualizer)
	alice.expect(t, want("m1", awayEqualizer))

	// Joins the deduplicated subscription: gets the current state right away.
	late := env.subscribe(t, sseClient, "Bearer alice", `{"matchId":"m1"}`)
	late.expect(t, want("m1", awayEqualizer))

	// Multipart clients are served from the same snapshot.
	multipart := env.subscribe(t, multipartClient, "Bearer alice", `{"matchId":"m1"}`)
	multipart.expect(t, want("m1", awayEqualizer))

	if got := env.sub.opened.Load(); got != 1 {
		t.Fatalf("expected one deduplicated upstream stream, got %d", got)
	}

	// Afterwards everybody keeps receiving live updates, without duplicates.
	env.sub.publish(homeWinner)
	for _, c := range []*client{alice, late, multipart} {
		c.expect(t, want("m1", homeWinner))
		c.expectQuiet(t)
	}
}

func TestIntegrationNoSnapshotAcrossUsersOrArguments(t *testing.T) {
	env := startEnv(t, "")

	alice := env.subscribe(t, sseClient, "Bearer alice", `{"matchId":"m1"}`)
	env.sub.waitOpened(t, 1)
	env.sub.publish(homeGoal)
	alice.expect(t, want("m1", homeGoal))

	// Authorization is not forwarded to the subgraph, so the router shares the
	// upstream stream with bob, but bob must not receive alice's snapshot.
	bob := env.subscribe(t, sseClient, "Bearer bob", `{"matchId":"m1"}`)
	anonymous := env.subscribe(t, sseClient, "", `{"matchId":"m1"}`)
	bob.expectQuiet(t)
	anonymous.expectQuiet(t)

	// Different arguments, also when passed as inline literals.
	otherVar := env.subscribe(t, sseClient, "Bearer alice", `{"matchId":"m2"}`)
	otherInline := env.subscribeQuery(t, sseClient, "Bearer alice", `subscription { matchUpdated(matchId: "m2") { id minute score { home away } } }`, "")
	otherVar.expectQuiet(t)
	otherInline.expectQuiet(t)

	env.sub.publish(awayEqualizer)
	bob.expect(t, want("m1", awayEqualizer))
}

func TestIntegrationNoStaleSnapshotAfterUpstreamCloses(t *testing.T) {
	env := startEnv(t, "")

	first := env.subscribe(t, sseClient, "Bearer alice", `{"matchId":"m1"}`)
	env.sub.waitOpened(t, 1)
	env.sub.publish(homeGoal)
	first.expect(t, want("m1", homeGoal))

	first.close()
	env.sub.waitOpen(t, 0) // the router closed the upstream stream

	again := env.subscribe(t, sseClient, "Bearer alice", `{"matchId":"m1"}`)
	env.sub.waitOpened(t, 2)
	again.expectQuiet(t)
}

func TestIntegrationSharedSnapshotWithoutKeyHeaders(t *testing.T) {
	env := startEnv(t, "    key_headers: []\n")

	alice := env.subscribe(t, sseClient, "Bearer alice", `{"matchId":"m1"}`)
	env.sub.waitOpened(t, 1)
	env.sub.publish(homeGoal)
	alice.expect(t, want("m1", homeGoal))

	bob := env.subscribe(t, sseClient, "Bearer bob", `{"matchId":"m1"}`)
	bob.expect(t, want("m1", homeGoal))
}

// --- match updates ---------------------------------------------------------

// update is a live match state published by the subgraph.
type update struct{ minute, home, away int }

var (
	homeGoal      = update{minute: 12, home: 1, away: 0}
	awayEqualizer = update{minute: 34, home: 1, away: 1}
	homeWinner    = update{minute: 67, home: 2, away: 1}
)

// want returns the compact `data` object clients receive for match id in state u.
func want(id string, u update) string {
	return fmt.Sprintf(`{"matchUpdated":{"id":%q,"minute":%d,"score":{"home":%d,"away":%d}}}`, id, u.minute, u.home, u.away)
}

// --- environment -----------------------------------------------------------

type env struct {
	routerURL string
	sub       *subgraph
}

func startEnv(t *testing.T, moduleConfig string) *env {
	t.Helper()
	t.Setenv("COSMO_TELEMETRY_DISABLED", "true")

	sub := newSubgraph()
	srv := httptest.NewServer(sub)
	t.Cleanup(srv.Close)
	t.Cleanup(sub.closeAll) // runs before srv.Close

	dir := t.TempDir()
	execConfig, err := os.ReadFile(filepath.Join("testdata", "router_config.json"))
	if err != nil {
		t.Fatal(err)
	}
	execConfig = bytes.ReplaceAll(execConfig, []byte("http://subgraph.invalid"), []byte(srv.URL))
	execPath := filepath.Join(dir, "router_config.json")
	if err := os.WriteFile(execPath, execConfig, 0o600); err != nil {
		t.Fatal(err)
	}

	addr := freeAddr(t)
	configPath := filepath.Join(dir, "config.yaml")
	cfgYAML := fmt.Sprintf(`version: "1"
listen_addr: %q
shutdown_delay: 15s
grace_period: 1s
execution_config:
  file:
    path: %q
telemetry:
  tracing:
    enabled: false
  metrics:
    otlp:
      enabled: false
    prometheus:
      enabled: false
modules:
  subscriptionSnapshot:
%s`, addr, execPath, moduleConfig+"    ttl: \"\"\n")
	if err := os.WriteFile(configPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	logger := zap.NewNop()
	rs, err := core.NewRouterSupervisor(&core.RouterSupervisorOpts{
		BaseLogger:            logger,
		ReloadPersistentState: core.NewReloadPersistentState(logger),
		ConfigFactory: func() (*config.Config, error) {
			res, err := config.LoadConfig([]string{configPath})
			if err != nil {
				return nil, err
			}
			return &res.Config, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	startErr := make(chan error, 1)
	go func() { startErr <- rs.Start() }()

	routerURL := "http://" + addr
	if err := waitReady(routerURL, startErr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		rs.Stop()
		select {
		case <-startErr:
		case <-time.After(10 * time.Second):
			t.Error("router did not stop in time")
		}
	})

	return &env{routerURL: routerURL, sub: sub}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitReady(routerURL string, startErr <-chan error) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-startErr:
			return fmt.Errorf("router stopped during startup: %v", err)
		default:
		}
		resp, err := http.Get(routerURL + "/health/ready")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("router did not become ready")
}

// --- subgraph --------------------------------------------------------------

// subgraph serves `subscription { matchUpdated(matchId) }` over GraphQL SSE
// and only emits a message when publish is called.
type subgraph struct {
	opened atomic.Int64
	open   atomic.Int64

	mu      sync.Mutex
	streams map[chan update]string // channel -> match id
	done    chan struct{}
	once    sync.Once
}

func newSubgraph() *subgraph {
	return &subgraph{streams: map[chan update]string{}, done: make(chan struct{})}
}

func (s *subgraph) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Variables map[string]any `json:"variables"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"status":"ok"}}`))
		return
	}

	matchID := "?"
	for _, v := range body.Variables { // the router may rename variables
		if str, ok := v.(string); ok {
			matchID = str
		}
	}

	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := make(chan update, 16)
	s.mu.Lock()
	s.streams[ch] = matchID
	s.mu.Unlock()
	s.opened.Add(1)
	s.open.Add(1)
	defer func() {
		s.mu.Lock()
		delete(s.streams, ch)
		s.mu.Unlock()
		s.open.Add(-1)
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done:
			return
		case u := <-ch:
			_, _ = fmt.Fprintf(w, "event: next\ndata: {\"data\":%s}\n\n", want(matchID, u))
			flusher.Flush()
		}
	}
}

func (s *subgraph) publish(u update) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.streams {
		ch <- u
	}
}

func (s *subgraph) closeAll() { s.once.Do(func() { close(s.done) }) }

func (s *subgraph) waitOpened(t *testing.T, n int64) {
	t.Helper()
	eventually(t, func() bool { return s.opened.Load() >= n }, "upstream streams opened >= %d (got %d)", n, s.opened.Load())
}

func (s *subgraph) waitOpen(t *testing.T, n int64) {
	t.Helper()
	eventually(t, func() bool { return s.open.Load() == n }, "upstream streams open == %d (got %d)", n, s.open.Load())
}

func eventually(t *testing.T, cond func() bool, format string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for "+format, args...)
}

// --- clients ---------------------------------------------------------------

type clientKind int

const (
	sseClient clientKind = iota
	multipartClient
)

type client struct {
	cancel context.CancelFunc
	data   chan string
}

const matchQuery = `subscription OnMatchUpdated($matchId: ID!) { matchUpdated(matchId: $matchId) { id minute score { home away } } }`

func (e *env) subscribe(t *testing.T, kind clientKind, auth, variables string) *client {
	return e.subscribeQuery(t, kind, auth, matchQuery, variables)
}

func (e *env) subscribeQuery(t *testing.T, kind clientKind, auth, query, variables string) *client {
	t.Helper()
	payload := map[string]any{"query": query}
	if variables != "" {
		payload["variables"] = json.RawMessage(variables)
	}
	body, _ := json.Marshal(payload)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.routerURL+"/graphql", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if kind == multipartClient {
		req.Header.Set("Accept", `multipart/mixed;subscriptionSpec="1.0", application/json`)
	} else {
		req.Header.Set("Accept", "text/event-stream")
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	// The router sends the SSE response headers with the first message or
	// heartbeat, so the request runs asynchronously, like a real client.
	// Failures are reported through the data channel and fail expect/expectQuiet.
	c := &client{cancel: cancel, data: make(chan string, 16)}
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if ctx.Err() == nil {
				c.data <- "request failed: " + err.Error()
			}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			c.data <- "subscription request failed: " + resp.Status
			return
		}
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if data, ok := extractData(sc.Text()); ok {
				c.data <- data
			}
		}
	}()
	t.Cleanup(c.close)
	return c
}

// extractData returns the compact `data` object of an SSE or multipart line.
func extractData(line string) (string, bool) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if !strings.HasPrefix(line, "{") {
		return "", false
	}
	var msg struct {
		Data    json.RawMessage `json:"data"`
		Payload *struct {
			Data json.RawMessage `json:"data"`
		} `json:"payload"`
	}
	if json.Unmarshal([]byte(line), &msg) != nil {
		return "", false
	}
	raw := msg.Data
	if msg.Payload != nil {
		raw = msg.Payload.Data
	}
	if len(raw) == 0 {
		return "", false // heartbeat
	}
	var out bytes.Buffer
	if json.Compact(&out, raw) != nil {
		return "", false
	}
	return out.String(), true
}

func (c *client) close() { c.cancel() }

func (c *client) expect(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-c.data:
		if got != want {
			t.Fatalf("unexpected message:\n got: %s\nwant: %s", got, want)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("timed out waiting for %s", want)
	}
}

func (c *client) expectQuiet(t *testing.T) {
	t.Helper()
	select {
	case got := <-c.data:
		t.Fatalf("expected no message, got %s", got)
	case <-time.After(quietTimeout):
	}
}
