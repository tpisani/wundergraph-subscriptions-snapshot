// Package snapshot provides a Cosmo Router custom module that replays the
// latest event of a Cosmo Streams (EDFS) subscription to every new subscriber.
//
// The router deduplicates EDFS subscriptions: all clients subscribed to the same
// provider + subjects/topics/channels share a single broker subscription. A
// client that joins later only receives future events. This module remembers
// the last event delivered on each broker subscription (via the
// BeforeEventsDispatch hook, which runs once per batch, not once per
// subscriber) and emits it to each new client right when it subscribes (via
// the SubscriptionOnStart hook).
//
// Since EDFS events usually carry only the entity key, the router resolves all
// requested fields from the subgraphs when the snapshot is emitted, so clients
// receive fresh data.
package snapshot

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/wundergraph/cosmo/router/core"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/kafka"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/nats"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/redis"
	"go.uber.org/zap"
)

// ModuleID is the key used under `modules:` in the router config.yaml.
const ModuleID = "subscriptionSnapshot"

const defaultMaxEntries = 10_000

func init() {
	core.RegisterModule(&Module{})
}

// Module is the subscription snapshot custom module.
//
// Config (all optional), decoded from `modules.subscriptionSnapshot` in config.yaml:
//
//	modules:
//	  subscriptionSnapshot:
//	    fields: ["employeeUpdated"] # root subscription fields to snapshot; empty = all EDFS subscriptions
//	    ttl: "10m"                  # discard snapshots older than this; empty/"0" = never
//	    max_entries: 10000          # LRU bound on stored snapshots; 0 = default (10000), -1 = unbounded
type Module struct {
	Fields     []string `mapstructure:"fields"`
	TTL        string   `mapstructure:"ttl"`
	MaxEntries int      `mapstructure:"max_entries"`

	fields map[string]struct{}
	store  *store
	logger *zap.Logger
}

func (m *Module) Module() core.ModuleInfo {
	return core.ModuleInfo{
		ID: ModuleID,
		// No priority on purpose: modules without one run last, so any
		// authorization module's SubscriptionOnStart can reject the client
		// before the snapshot is emitted.
		New: func() core.Module {
			return &Module{}
		},
	}
}

// Provision validates the config and initializes the store.
func (m *Module) Provision(ctx *core.ModuleContext) error {
	m.logger = ctx.Logger

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

	maxEntries := m.MaxEntries
	if maxEntries == 0 {
		maxEntries = defaultMaxEntries
	}

	m.fields = make(map[string]struct{}, len(m.Fields))
	for _, f := range m.Fields {
		m.fields[f] = struct{}{}
	}

	m.store = newStore(maxEntries, ttl)

	m.logger.Info("subscription snapshot module provisioned",
		zap.Strings("fields", m.Fields),
		zap.Duration("ttl", ttl),
		zap.Int("max_entries", maxEntries),
	)
	return nil
}

// BeforeEventsDispatch runs once per batch received from the broker and records
// the last event of the batch. Events are passed through unchanged.
func (m *Module) BeforeEventsDispatch(ctx core.StreamBeforeEventsDispatchHandlerContext, events datasource.StreamEvents) (datasource.StreamEvents, error) {
	key, ok := m.keyFor(ctx.SubscriptionEventConfiguration())
	if !ok {
		return events, nil
	}

	var last []byte
	for _, evt := range events.All() {
		if evt == nil {
			continue
		}
		if data := evt.GetData(); len(data) > 0 {
			last = data
		}
	}
	if last != nil {
		m.store.Set(key, last)
	}

	return events, nil
}

// SubscriptionOnStart emits the stored snapshot (if any) to the client that is
// subscribing right now. Other subscribers are not affected.
func (m *Module) SubscriptionOnStart(ctx core.SubscriptionOnStartHandlerContext) error {
	// Engine (non-EDFS) subscriptions have no event configuration; skip them.
	key, ok := m.keyFor(ctx.SubscriptionEventConfiguration())
	if !ok {
		return nil
	}

	data, ok := m.store.Get(key)
	if !ok {
		return nil
	}

	if !ctx.EmitEvent(ctx.NewEvent(data)) {
		ctx.Logger().Warn("subscription snapshot was dropped", zap.String("key", key))
	}
	return nil
}

// keyFor builds a stable cache key matching the broker subscription the router
// deduplicates on: provider type + provider id + sorted subjects/topics/channels.
// Subjects are already interpolated with the operation arguments at this point.
func (m *Module) keyFor(cfg datasource.SubscriptionEventConfiguration) (string, bool) {
	if cfg == nil || m.store == nil {
		return "", false
	}
	if len(m.fields) > 0 {
		if _, ok := m.fields[cfg.RootFieldName()]; !ok {
			return "", false
		}
	}

	var targets []string
	switch c := cfg.(type) {
	case *nats.SubscriptionEventConfiguration:
		targets = c.Subjects
	case *kafka.SubscriptionEventConfiguration:
		targets = c.Topics
	case *redis.SubscriptionEventConfiguration:
		targets = c.Channels
	default:
		return "", false
	}
	if len(targets) == 0 {
		return "", false
	}

	sorted := slices.Clone(targets)
	slices.Sort(sorted)

	return string(cfg.ProviderType()) + "|" + cfg.ProviderID() + "|" + strings.Join(sorted, ","), true
}

// Interface guards.
var (
	_ core.Module                            = (*Module)(nil)
	_ core.Provisioner                       = (*Module)(nil)
	_ core.SubscriptionOnStartHandler        = (*Module)(nil)
	_ core.StreamBeforeEventsDispatchHandler = (*Module)(nil)
)
