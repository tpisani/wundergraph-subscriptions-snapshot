package snapshot

import (
	"context"
	"net/http"
	"testing"

	"github.com/wundergraph/cosmo/router/core"
	"github.com/wundergraph/cosmo/router/pkg/authentication"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/kafka"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/nats"
	"go.uber.org/zap"
)

type testEvent struct{ data []byte }

func (e *testEvent) GetData() []byte                       { return e.data }
func (e *testEvent) SetData(d []byte)                      { e.data = d }
func (e *testEvent) Clone() datasource.MutableStreamEvent { return &testEvent{data: append([]byte(nil), e.data...)} }

type dispatchCtx struct{ cfg datasource.SubscriptionEventConfiguration }

func (c *dispatchCtx) Context() context.Context { return context.Background() }
func (c *dispatchCtx) Logger() *zap.Logger      { return zap.NewNop() }
func (c *dispatchCtx) SubscriptionEventConfiguration() datasource.SubscriptionEventConfiguration {
	return c.cfg
}
func (c *dispatchCtx) NewEvent(d []byte) datasource.MutableStreamEvent { return &testEvent{data: d} }

type startCtx struct {
	cfg     datasource.SubscriptionEventConfiguration
	emitted [][]byte
}

func (c *startCtx) Request() *http.Request                        { return nil }
func (c *startCtx) Logger() *zap.Logger                           { return zap.NewNop() }
func (c *startCtx) Operation() core.OperationContext              { return nil }
func (c *startCtx) Authentication() authentication.Authentication { return nil }
func (c *startCtx) SubscriptionEventConfiguration() datasource.SubscriptionEventConfiguration {
	return c.cfg
}
func (c *startCtx) EmitEvent(e datasource.StreamEvent) bool {
	c.emitted = append(c.emitted, e.GetData())
	return true
}
func (c *startCtx) NewEvent(d []byte) datasource.MutableStreamEvent { return &testEvent{data: d} }

func provisioned(t *testing.T, m *Module) *Module {
	t.Helper()
	if err := m.Provision(&core.ModuleContext{Context: context.Background(), Module: m, Logger: zap.NewNop()}); err != nil {
		t.Fatal(err)
	}
	return m
}

func events(payloads ...string) datasource.StreamEvents {
	evts := make([]datasource.StreamEvent, 0, len(payloads))
	for _, p := range payloads {
		evts = append(evts, &testEvent{data: []byte(p)})
	}
	return datasource.NewStreamEvents(evts)
}

func natsCfg(field string, subjects ...string) *nats.SubscriptionEventConfiguration {
	return &nats.SubscriptionEventConfiguration{Provider: "default", Subjects: subjects, FieldName: field}
}

func TestReplaysLastEventToNewSubscriber(t *testing.T) {
	m := provisioned(t, &Module{})
	cfg := natsCfg("employeeUpdated", "employeeUpdated.1")

	out, err := m.BeforeEventsDispatch(&dispatchCtx{cfg: cfg}, events(
		`{"__typename":"Employee","id":1,"v":1}`,
		`{"__typename":"Employee","id":1,"v":2}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() != 2 {
		t.Fatal("events must pass through unchanged")
	}

	sc := &startCtx{cfg: natsCfg("employeeUpdated", "employeeUpdated.1")}
	if err := m.SubscriptionOnStart(sc); err != nil {
		t.Fatal(err)
	}
	if len(sc.emitted) != 1 || string(sc.emitted[0]) != `{"__typename":"Employee","id":1,"v":2}` {
		t.Fatalf("expected last event of batch, got %q", sc.emitted)
	}
}

func TestNoSnapshotForOtherSubjectOrEmptyStore(t *testing.T) {
	m := provisioned(t, &Module{})

	sc := &startCtx{cfg: natsCfg("employeeUpdated", "employeeUpdated.1")}
	_ = m.SubscriptionOnStart(sc)
	if len(sc.emitted) != 0 {
		t.Fatal("nothing should be emitted before any event")
	}

	_, _ = m.BeforeEventsDispatch(&dispatchCtx{cfg: natsCfg("employeeUpdated", "employeeUpdated.1")}, events(`{"id":1}`))

	other := &startCtx{cfg: natsCfg("employeeUpdated", "employeeUpdated.2")}
	_ = m.SubscriptionOnStart(other)
	if len(other.emitted) != 0 {
		t.Fatal("snapshot must not leak to a different subject")
	}
}

func TestKeyIgnoresSubjectOrderButSeparatesProviders(t *testing.T) {
	m := provisioned(t, &Module{})
	_, _ = m.BeforeEventsDispatch(&dispatchCtx{cfg: natsCfg("f", "b", "a")}, events(`{"id":1}`))

	same := &startCtx{cfg: natsCfg("f", "a", "b")}
	_ = m.SubscriptionOnStart(same)
	if len(same.emitted) != 1 {
		t.Fatal("subject order must not matter")
	}

	kafkaSc := &startCtx{cfg: &kafka.SubscriptionEventConfiguration{Provider: "default", Topics: []string{"a", "b"}, FieldName: "f"}}
	_ = m.SubscriptionOnStart(kafkaSc)
	if len(kafkaSc.emitted) != 0 {
		t.Fatal("snapshot must not leak across provider types")
	}
}

func TestFieldsAllowlist(t *testing.T) {
	m := provisioned(t, &Module{Fields: []string{"employeeUpdated"}})

	_, _ = m.BeforeEventsDispatch(&dispatchCtx{cfg: natsCfg("other", "x")}, events(`{"id":1}`))
	sc := &startCtx{cfg: natsCfg("other", "x")}
	_ = m.SubscriptionOnStart(sc)
	if len(sc.emitted) != 0 {
		t.Fatal("fields outside the allowlist must be ignored")
	}
}

func TestEngineSubscriptionsAreIgnored(t *testing.T) {
	m := provisioned(t, &Module{})
	sc := &startCtx{cfg: nil}
	if err := m.SubscriptionOnStart(sc); err != nil {
		t.Fatal(err)
	}
	if len(sc.emitted) != 0 {
		t.Fatal("engine subscriptions have no event config and must be skipped")
	}
}

func TestInvalidTTL(t *testing.T) {
	m := &Module{TTL: "banana"}
	if err := m.Provision(&core.ModuleContext{Context: context.Background(), Module: m, Logger: zap.NewNop()}); err == nil {
		t.Fatal("expected error for invalid ttl")
	}
}
