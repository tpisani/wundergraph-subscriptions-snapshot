package snapshot_test

import (
	"context"
	"testing"

	"github.com/mitchellh/mapstructure"
	"github.com/wundergraph/cosmo/router/core"
	"github.com/wundergraph/cosmo/router/pkg/config"
	"go.uber.org/zap"

	"github.com/tpisani/wundergraph-subscriptions-snapshot/snapshot"
)

// The example config must pass the router's JSON schema validation and be
// accepted by the module, decoded the same way the router decodes it.
func TestExampleConfigIsValid(t *testing.T) {
	res, err := config.LoadConfig([]string{"../config.example.yaml"})
	if err != nil {
		t.Fatal(err)
	}

	raw, ok := res.Config.Modules[snapshot.ModuleID]
	if !ok {
		t.Fatalf("config.example.yaml has no modules.%s section", snapshot.ModuleID)
	}

	m := &snapshot.Module{}
	if err := mapstructure.Decode(raw, m); err != nil {
		t.Fatal(err)
	}
	if err := m.Provision(&core.ModuleContext{Context: context.Background(), Module: m, Logger: zap.NewNop()}); err != nil {
		t.Fatal(err)
	}
}
