package snapshot

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mitchellh/mapstructure"
	"github.com/wundergraph/astjson"
	"github.com/wundergraph/cosmo/router/core"
	"github.com/wundergraph/cosmo/router/pkg/authentication"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/nats"
	"go.uber.org/zap"
)

// fakeOp implements core.OperationContext; unused methods panic via the nil
// embedded interface.
type fakeOp struct {
	core.OperationContext
	name, typ string
	hash      uint64
	vars      string
}

func (o *fakeOp) Name() string { return o.name }
func (o *fakeOp) Type() string { return o.typ }
func (o *fakeOp) Hash() uint64 { return o.hash }
func (o *fakeOp) Variables() *astjson.Value {
	if o.vars == "" {
		return nil
	}
	return astjson.MustParse(o.vars)
}

// fakeReqCtx implements core.RequestContext for OnOriginResponse.
type fakeReqCtx struct {
	core.RequestContext
	op  core.OperationContext
	req *http.Request
}

func (c *fakeReqCtx) Operation() core.OperationContext { return c.op }
func (c *fakeReqCtx) Request() *http.Request           { return c.req }
func (c *fakeReqCtx) Logger() *zap.Logger              { return zap.NewNop() }

type fakeEvent struct{ data []byte }

func (e *fakeEvent) GetData() []byte                      { return e.data }
func (e *fakeEvent) SetData(d []byte)                     { e.data = d }
func (e *fakeEvent) Clone() datasource.MutableStreamEvent { return &fakeEvent{data: e.data} }

// fakeStartCtx implements core.SubscriptionOnStartHandlerContext for an
// engine (subgraph) subscription.
type fakeStartCtx struct {
	op      core.OperationContext
	req     *http.Request
	evCfg   datasource.SubscriptionEventConfiguration
	emitted []string
}

func (c *fakeStartCtx) Request() *http.Request                        { return c.req }
func (c *fakeStartCtx) Logger() *zap.Logger                           { return zap.NewNop() }
func (c *fakeStartCtx) Operation() core.OperationContext              { return c.op }
func (c *fakeStartCtx) Authentication() authentication.Authentication { return nil }
func (c *fakeStartCtx) SubscriptionEventConfiguration() datasource.SubscriptionEventConfiguration {
	return c.evCfg
}
func (c *fakeStartCtx) EmitEvent(e datasource.StreamEvent) bool {
	c.emitted = append(c.emitted, string(e.GetData()))
	return true
}
func (c *fakeStartCtx) NewEvent(d []byte) datasource.MutableStreamEvent { return &fakeEvent{data: d} }

func provisioned(t *testing.T, m *Module) *Module {
	t.Helper()
	if err := m.Provision(&core.ModuleContext{Context: context.Background(), Module: m, Logger: zap.NewNop()}); err != nil {
		t.Fatal(err)
	}
	return m
}

func clientReq(auth string) *http.Request {
	r, _ := http.NewRequest(http.MethodPost, "http://router/graphql", nil)
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	return r
}

func sseResponse(stream string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream; charset=utf-8"}},
		Body:       io.NopCloser(strings.NewReader(stream)),
	}
}

var priceOp = &fakeOp{name: "OnPrice", typ: "subscription", hash: 42, vars: `{"symbol":"ABC"}`}

// upstream simulates the engine reading the subgraph SSE stream that was
// opened for the first subscriber (the trigger owner).
func upstream(t *testing.T, m *Module, op core.OperationContext, auth, stream string) *http.Response {
	t.Helper()
	resp := sseResponse(stream)
	if out := m.OnOriginResponse(resp, &fakeReqCtx{op: op, req: clientReq(auth)}); out != nil {
		t.Fatal("handler must modify in place and return nil")
	}
	return resp
}

func TestReplaysLastUpstreamMessageToLateSubscriber(t *testing.T) {
	m := provisioned(t, &Module{})
	stream := "event: next\ndata: {\"data\":{\"price\":1}}\n\nevent: next\ndata: {\"data\":{\"price\":2}}\n\n"
	resp := upstream(t, m, priceOp, "Bearer alice", stream)

	// Read only part of the stream: the subscription is still live.
	buf := make([]byte, len(stream))
	n, _ := io.ReadFull(resp.Body, buf)
	if string(buf[:n]) != stream {
		t.Fatal("engine must see the original stream")
	}

	late := &fakeStartCtx{op: &fakeOp{name: "OnPrice", typ: "subscription", hash: 42, vars: `{"symbol":"ABC"}`}, req: clientReq("Bearer alice")}
	if err := m.SubscriptionOnStart(late); err != nil {
		t.Fatal(err)
	}
	if len(late.emitted) != 1 || late.emitted[0] != `{"data":{"price":2}}` {
		t.Fatalf("expected latest upstream message, got %q", late.emitted)
	}
}

func TestDoesNotLeakAcrossUsersOrVariables(t *testing.T) {
	m := provisioned(t, &Module{})
	resp := upstream(t, m, priceOp, "Bearer alice", "event: next\ndata: {\"data\":{\"price\":1}}\n\n")
	buf := make([]byte, 64)
	_, _ = resp.Body.Read(buf)

	for name, sc := range map[string]*fakeStartCtx{
		"other user":      {op: priceOp, req: clientReq("Bearer bob")},
		"anonymous":       {op: priceOp, req: clientReq("")},
		"other variables": {op: &fakeOp{name: "OnPrice", typ: "subscription", hash: 42, vars: `{"symbol":"XYZ"}`}, req: clientReq("Bearer alice")},
		"other operation": {op: &fakeOp{name: "OnPrice", typ: "subscription", hash: 43, vars: `{"symbol":"ABC"}`}, req: clientReq("Bearer alice")},
	} {
		_ = m.SubscriptionOnStart(sc)
		if len(sc.emitted) != 0 {
			t.Fatalf("%s must not receive the snapshot", name)
		}
	}
}

func TestSnapshotDroppedWhenUpstreamEnds(t *testing.T) {
	m := provisioned(t, &Module{})
	resp := upstream(t, m, priceOp, "Bearer alice", "event: next\ndata: {\"data\":{\"price\":1}}\n\n")
	_, _ = io.ReadAll(resp.Body) // stream ends (EOF)
	_ = resp.Body.Close()

	sc := &fakeStartCtx{op: priceOp, req: clientReq("Bearer alice")}
	_ = m.SubscriptionOnStart(sc)
	if len(sc.emitted) != 0 {
		t.Fatal("a finished upstream stream must not leave a snapshot behind")
	}
}

func TestIgnoresNonSubscriptionAndNonSSEResponses(t *testing.T) {
	m := provisioned(t, &Module{})

	query := &fakeOp{name: "Q", typ: "query", hash: 1}
	resp := sseResponse("event: next\ndata: {\"data\":{\"a\":1}}\n\n")
	orig := resp.Body
	m.OnOriginResponse(resp, &fakeReqCtx{op: query, req: clientReq("")})
	if resp.Body != orig {
		t.Fatal("queries must not be tapped")
	}

	jsonResp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{}`))}
	orig = jsonResp.Body
	m.OnOriginResponse(jsonResp, &fakeReqCtx{op: priceOp, req: clientReq("")})
	if jsonResp.Body != orig {
		t.Fatal("non-SSE responses (e.g. entity fetches) must not be tapped")
	}

	if m.OnOriginResponse(nil, &fakeReqCtx{op: priceOp}) != nil {
		t.Fatal("nil response must be ignored")
	}
}

func TestOperationsAllowlist(t *testing.T) {
	m := provisioned(t, &Module{Operations: []string{"Other"}})
	resp := sseResponse("event: next\ndata: {\"data\":{\"a\":1}}\n\n")
	orig := resp.Body
	m.OnOriginResponse(resp, &fakeReqCtx{op: priceOp, req: clientReq("")})
	if resp.Body != orig {
		t.Fatal("operations outside the allowlist must not be tapped")
	}
}

func TestSkipsEDFSSubscriptions(t *testing.T) {
	m := provisioned(t, &Module{})
	resp := upstream(t, m, priceOp, "", "event: next\ndata: {\"data\":{\"a\":1}}\n\n")
	_, _ = resp.Body.Read(make([]byte, 64))

	sc := &fakeStartCtx{op: priceOp, req: clientReq(""), evCfg: &nats.SubscriptionEventConfiguration{Subjects: []string{"x"}}}
	_ = m.SubscriptionOnStart(sc)
	if len(sc.emitted) != 0 {
		t.Fatal("EDFS subscriptions are out of scope")
	}
}

func TestConfigDecoding(t *testing.T) {
	// The router decodes module config with mapstructure.Decode.
	m := &Module{}
	err := mapstructure.Decode(map[string]any{
		"key_headers":     []any{"authorization", "x-tenant-id", "X-Tenant-Id"},
		"operations":      []any{"OnPrice"},
		"ttl":             "5m",
		"max_entries":     5,
		"max_event_bytes": 2048,
		"max_total_bytes": -1,
	}, m)
	if err != nil {
		t.Fatal(err)
	}
	provisioned(t, m)
	if m.store.maxEntries != 5 || m.store.maxBytes != 0 || m.MaxEventBytes != 2048 || m.store.ttl.String() != "5m0s" {
		t.Fatalf("limits not applied: entries=%d bytes=%d event=%d ttl=%s", m.store.maxEntries, m.store.maxBytes, m.MaxEventBytes, m.store.ttl)
	}
	if strings.Join(m.keyHeaders, ",") != "Authorization,X-Tenant-Id" {
		t.Fatalf("headers must be canonicalized, sorted and deduplicated: %v", m.keyHeaders)
	}

	def := provisioned(t, &Module{})
	if strings.Join(def.keyHeaders, ",") != "Authorization" {
		t.Fatalf("default key header must be Authorization, got %v", def.keyHeaders)
	}

	empty := &Module{}
	if err := mapstructure.Decode(map[string]any{"key_headers": []any{}}, empty); err != nil {
		t.Fatal(err)
	}
	provisioned(t, empty)
	if len(empty.keyHeaders) != 0 {
		t.Fatalf("an explicit empty list must disable header keys, got %v", empty.keyHeaders)
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, m := range []*Module{{TTL: "banana"}, {TTL: "-1s"}, {MaxEventBytes: -1}, {MaxEntries: -2}, {MaxTotalBytes: -5}} {
		if err := m.Provision(&core.ModuleContext{Context: context.Background(), Module: m, Logger: zap.NewNop()}); err == nil {
			t.Fatalf("expected error for %+v", m)
		}
	}
}

func TestModuleInfo(t *testing.T) {
	info := (&Module{}).Module()
	if info.ID != ModuleID {
		t.Fatalf("unexpected module id %q", info.ID)
	}
	if info.Priority <= 0 {
		t.Fatal("priority must be greater than 0")
	}
	if _, ok := info.New().(*Module); !ok {
		t.Fatal("New must return a fresh *Module")
	}
}

func TestDefaults(t *testing.T) {
	m := provisioned(t, &Module{})
	if m.store.maxEntries != defaultMaxEntries || m.store.maxBytes != defaultMaxTotalBytes || m.MaxEventBytes != defaultMaxEventBytes || m.store.ttl != 0 {
		t.Fatalf("unexpected defaults: entries=%d bytes=%d event=%d ttl=%s", m.store.maxEntries, m.store.maxBytes, m.MaxEventBytes, m.store.ttl)
	}
}

func TestCleanupDropsSnapshots(t *testing.T) {
	m := provisioned(t, &Module{})
	resp := upstream(t, m, priceOp, "", "event: next\ndata: {\"data\":{\"a\":1}}\n\n")
	_, _ = resp.Body.Read(make([]byte, 64))
	if m.store.Len() != 1 {
		t.Fatal("expected a stored snapshot")
	}
	if err := m.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if m.store.Len() != 0 {
		t.Fatal("cleanup must drop stored snapshots")
	}
	if err := (&Module{}).Cleanup(); err != nil {
		t.Fatal("cleanup on an unprovisioned module must not fail")
	}
}
