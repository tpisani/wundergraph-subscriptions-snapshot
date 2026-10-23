package snapshot

import (
	"net/http"
	"testing"
)

var authOnly = []string{"Authorization"}

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Add(kv[i], kv[i+1])
	}
	return h
}

func TestKeyStableAcrossVariableOrder(t *testing.T) {
	a := subscriptionKey(1, []byte(`{"a":1,"b":{"y":2,"x":1}}`), hdr("Authorization", "t"), authOnly)
	b := subscriptionKey(1, []byte(`{"b":{"x":1,"y":2},"a":1}`), hdr("Authorization", "t"), authOnly)
	if a != b {
		t.Fatal("variable key order must not change the key")
	}
}

func TestKeyEmptyVariablesEquivalent(t *testing.T) {
	h := hdr("Authorization", "t")
	k := subscriptionKey(1, nil, h, authOnly)
	for _, v := range []string{``, `null`, `{}`, ` {} `} {
		if subscriptionKey(1, []byte(v), h, authOnly) != k {
			t.Fatalf("variables %q should equal no variables", v)
		}
	}
}

func TestKeySeparatesOperationsVariablesAndUsers(t *testing.T) {
	base := subscriptionKey(1, []byte(`{"id":1}`), hdr("Authorization", "alice"), authOnly)
	for name, other := range map[string]string{
		"operation": subscriptionKey(2, []byte(`{"id":1}`), hdr("Authorization", "alice"), authOnly),
		"variables": subscriptionKey(1, []byte(`{"id":2}`), hdr("Authorization", "alice"), authOnly),
		"user":      subscriptionKey(1, []byte(`{"id":1}`), hdr("Authorization", "bob"), authOnly),
		"no auth":   subscriptionKey(1, []byte(`{"id":1}`), hdr(), authOnly),
	} {
		if other == base {
			t.Fatalf("different %s must produce a different key", name)
		}
	}
}

func TestKeyIgnoresUnconfiguredHeaders(t *testing.T) {
	a := subscriptionKey(1, nil, hdr("Authorization", "t", "X-Request-Id", "1"), authOnly)
	b := subscriptionKey(1, nil, hdr("Authorization", "t", "X-Request-Id", "2"), authOnly)
	if a != b {
		t.Fatal("headers outside key_headers must not affect the key")
	}
}

func TestKeyExtraHeaders(t *testing.T) {
	headers := []string{"Authorization", "X-Tenant-Id"}
	a := subscriptionKey(1, nil, hdr("Authorization", "t", "X-Tenant-Id", "acme"), headers)
	b := subscriptionKey(1, nil, hdr("Authorization", "t", "X-Tenant-Id", "globex"), headers)
	if a == b {
		t.Fatal("configured extra headers must be part of the key")
	}
}

func TestKeyNoConcatenationCollision(t *testing.T) {
	headers := []string{"A", "B"}
	a := subscriptionKey(1, nil, hdr("A", "xy", "B", "z"), headers)
	b := subscriptionKey(1, nil, hdr("A", "x", "B", "yz"), headers)
	if a == b {
		t.Fatal("length-prefixing must prevent concatenation collisions")
	}
}
