// Package snapshot provides a Cosmo Router custom module that replays the
// latest message of a subgraph-served GraphQL subscription to every new
// subscriber, even when the router deduplicates upstream subscriptions.
//
// With deduplication, all clients subscribed with the same operation,
// variables and forwarded headers share one upstream subscription to the
// subgraph. A client that joins later only receives future messages. This
// module:
//
//  1. watches the router -> subgraph SSE stream (EnginePostOriginHandler) and
//     remembers the last data message per subscription key, without altering
//     the stream;
//  2. emits that message to each new client right when it subscribes
//     (SubscriptionOnStartHandler). The engine treats it exactly like a
//     message from the subgraph and resolves the client's own selection set.
//
// The subscription key is: normalized operation hash + variables + values of
// the configured identity headers (default: Authorization).
//
// Only the SSE protocol between router and subgraph is supported (configure
// the subgraph with `--subscription-protocol sse` or `sse_post`). Clients can
// still use any protocol the router accepts (graphql-ws, SSE, multipart, ...).
package snapshot

import (
	"fmt"
	"mime"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wundergraph/cosmo/router/core"
	"go.uber.org/zap"
)

// ModuleID is the key used under `modules:` in the router config.yaml.
const ModuleID = "subscriptionSnapshot"

// ModulePriority places this module after typical authorization modules
// (lower numbers run first), so their SubscriptionOnStart hooks can reject a
// client before a snapshot is emitted to it, and so this module observes the
// subgraph response after other OnOriginResponse handlers have run.
const ModulePriority = 10_000

const (
	defaultMaxEntries    = 10_000
	defaultMaxTotalBytes = 256 << 20 // 256 MiB
	defaultMaxEventBytes = 1 << 20   // 1 MiB
)

var defaultKeyHeaders = []string{"Authorization"}

func init() {
	core.RegisterModule(&Module{})
}

// Module is the subscription snapshot custom module.
//
// Configuration is decoded by the router from `modules.subscriptionSnapshot`
// in config.yaml. All fields are optional:
//
//	modules:
//	  subscriptionSnapshot:
//	    key_headers: ["Authorization"] # client headers that identify who may see a snapshot
//	    operations: ["OnPriceChanged"] # operation names to snapshot; empty = all subscriptions
//	    ttl: "10m"                     # discard snapshots older than this; empty/"0" = never
//	    max_entries: 10000             # max stored snapshots; 0 = default, -1 = unbounded
//	    max_total_bytes: 268435456     # max total snapshot size; 0 = default (256 MiB), -1 = unbounded
//	    max_event_bytes: 1048576       # largest SSE event tracked; 0 = default (1 MiB)
type Module struct {
	// KeyHeaders lists the client request headers whose values are part of the
	// snapshot key. Nil (not configured) defaults to [Authorization]; an
	// explicit empty list disables header keys.
	KeyHeaders []string `mapstructure:"key_headers"`
	// Operations restricts snapshots to these operation names. Empty = all.
	Operations []string `mapstructure:"operations"`
	// TTL is a Go duration string. The router decodes module config without
	// decode hooks, so durations are taken as strings and parsed in Provision.
	TTL string `mapstructure:"ttl"`
	// MaxEntries bounds the number of stored snapshots (LRU eviction).
	MaxEntries int `mapstructure:"max_entries"`
	// MaxTotalBytes bounds the total size of stored snapshots (LRU eviction).
	MaxTotalBytes int `mapstructure:"max_total_bytes"`
	// MaxEventBytes is the largest SSE event that is tracked. A larger event
	// stops tracking that stream; the stream itself is not affected.
	MaxEventBytes int `mapstructure:"max_event_bytes"`

	keyHeaders []string
	operations map[string]struct{}
	store      *store
	streamSeq  atomic.Uint64
	logger     *zap.Logger
}

// Module returns the registration info of the module.
func (m *Module) Module() core.ModuleInfo {
	return core.ModuleInfo{
		ID:       ModuleID,
		Priority: ModulePriority,
		New: func() core.Module {
			return &Module{}
		},
	}
}

// Provision validates the configuration and initializes the store.
func (m *Module) Provision(ctx *core.ModuleContext) error {
	m.logger = ctx.Logger
	if m.logger == nil {
		m.logger = zap.NewNop()
	}

	var ttl time.Duration
	if m.TTL != "" && m.TTL != "0" {
		d, err := time.ParseDuration(m.TTL)
		if err != nil {
			return fmt.Errorf("%s: invalid ttl %q: %w", ModuleID, m.TTL, err)
		}
		if d < 0 {
			return fmt.Errorf("%s: ttl must not be negative", ModuleID)
		}
		ttl = d
	}

	maxEntries, err := limit("max_entries", m.MaxEntries, defaultMaxEntries)
	if err != nil {
		return err
	}
	maxTotalBytes, err := limit("max_total_bytes", m.MaxTotalBytes, defaultMaxTotalBytes)
	if err != nil {
		return err
	}
	if m.MaxEventBytes < 0 {
		return fmt.Errorf("%s: max_event_bytes must not be negative", ModuleID)
	}
	if m.MaxEventBytes == 0 {
		m.MaxEventBytes = defaultMaxEventBytes
	}

	headers := m.KeyHeaders
	if headers == nil {
		headers = defaultKeyHeaders
	}
	m.keyHeaders = make([]string, 0, len(headers))
	for _, h := range headers {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		m.keyHeaders = append(m.keyHeaders, http.CanonicalHeaderKey(h))
	}
	slices.Sort(m.keyHeaders)
	m.keyHeaders = slices.Compact(m.keyHeaders)
	if len(m.keyHeaders) == 0 {
		m.logger.Warn("no key_headers configured: snapshots are shared by every client with the same operation and variables")
	}

	m.operations = make(map[string]struct{}, len(m.Operations))
	for _, op := range m.Operations {
		m.operations[op] = struct{}{}
	}

	m.store = newStore(maxEntries, maxTotalBytes, ttl)

	m.logger.Info("subscription snapshot module provisioned",
		zap.Strings("key_headers", m.keyHeaders),
		zap.Strings("operations", m.Operations),
		zap.Duration("ttl", ttl),
		zap.Int("max_entries", maxEntries),
		zap.Int("max_total_bytes", maxTotalBytes),
		zap.Int("max_event_bytes", m.MaxEventBytes),
	)
	return nil
}

// Cleanup releases the stored snapshots when the router shuts down.
func (m *Module) Cleanup() error {
	if m.store != nil {
		m.store.Reset()
	}
	return nil
}

// OnOriginResponse attaches a tap to router -> subgraph SSE subscription
// streams. Only the response Body is wrapped; nil is returned so that the
// response continues through the remaining handlers unchanged.
//
// This handler only reads from ctx, so it is safe when an operation fans out
// to several subgraphs concurrently.
func (m *Module) OnOriginResponse(resp *http.Response, ctx core.RequestContext) *http.Response {
	if resp == nil || resp.Body == nil || resp.StatusCode != http.StatusOK || ctx == nil {
		return nil
	}
	if !isEventStream(resp.Header.Get("Content-Type")) {
		return nil
	}

	key, ok := m.keyFor(ctx.Operation(), ctx.Request())
	if !ok {
		return nil
	}

	owner := m.streamSeq.Add(1)
	logger := ctx.Logger()
	if logger == nil {
		logger = m.logger
	}
	logger = logger.With(zap.String("component", ModuleID), zap.Uint64("stream", owner))
	logger.Debug("tracking upstream subscription stream")

	resp.Body = newSSETap(resp.Body, m.MaxEventBytes,
		func(payload []byte) { m.store.Set(key, owner, payload) },
		func(reason string) {
			m.store.DeleteOwned(key, owner)
			if reason == endEventTooLarge {
				logger.Warn("stopped tracking upstream subscription stream", zap.String("reason", reason), zap.Int("max_event_bytes", m.MaxEventBytes))
				return
			}
			logger.Debug("stopped tracking upstream subscription stream", zap.String("reason", reason))
		},
	)
	return nil
}

// SubscriptionOnStart emits the stored snapshot (if any) to the client that is
// subscribing right now. Other subscribers are not affected.
func (m *Module) SubscriptionOnStart(ctx core.SubscriptionOnStartHandlerContext) error {
	// Cosmo Streams (EDFS) subscriptions carry an event configuration; this
	// module only handles subgraph-served subscriptions.
	if ctx.SubscriptionEventConfiguration() != nil {
		return nil
	}

	key, ok := m.keyFor(ctx.Operation(), ctx.Request())
	if !ok {
		return nil
	}

	data, ok := m.store.Get(key)
	if !ok {
		return nil
	}

	logger := ctx.Logger()
	if !ctx.EmitEvent(ctx.NewEvent(data)) {
		if logger != nil {
			logger.Warn("subscription snapshot was dropped", zap.String("component", ModuleID))
		}
		return nil
	}
	if logger != nil {
		logger.Debug("subscription snapshot emitted", zap.String("component", ModuleID), zap.Int("bytes", len(data)))
	}
	return nil
}

func (m *Module) keyFor(op core.OperationContext, req *http.Request) (string, bool) {
	if m.store == nil || op == nil || op.Type() != "subscription" {
		return "", false
	}
	if len(m.operations) > 0 {
		if _, ok := m.operations[op.Name()]; !ok {
			return "", false
		}
	}

	var variables []byte
	if v := op.Variables(); v != nil {
		variables = v.MarshalTo(nil)
	}

	var header http.Header
	if req != nil {
		header = req.Header
	}

	return subscriptionKey(op.Hash(), variables, header, m.keyHeaders), true
}

// limit applies the shared semantics of size limits: 0 = default, -1 =
// unbounded, other negative values are invalid.
func limit(name string, value, def int) (int, error) {
	switch {
	case value == 0:
		return def, nil
	case value == -1:
		return 0, nil
	case value < 0:
		return 0, fmt.Errorf("%s: %s must be -1 (unbounded), 0 (default) or positive", ModuleID, name)
	default:
		return value, nil
	}
}

func isEventStream(contentType string) bool {
	if contentType == "" {
		return false
	}
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == "text/event-stream"
}

// Interface guards.
var (
	_ core.Module                     = (*Module)(nil)
	_ core.Provisioner                = (*Module)(nil)
	_ core.Cleaner                    = (*Module)(nil)
	_ core.EnginePostOriginHandler    = (*Module)(nil)
	_ core.SubscriptionOnStartHandler = (*Module)(nil)
)
