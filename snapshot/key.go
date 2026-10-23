package snapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
)

// subscriptionKey identifies the upstream stream a client shares through the
// router's deduplication: same normalized operation, same variables and same
// values for the configured identity headers (e.g. Authorization).
//
// The key is a SHA-256 digest, so header values such as bearer tokens are
// never kept in memory in clear text.
func subscriptionKey(opHash uint64, variables []byte, header http.Header, keyHeaders []string) string {
	h := sha256.New()

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], opHash)
	h.Write(b[:])

	writeField(h, canonicalJSON(variables))

	for _, name := range keyHeaders { // already canonical and sorted
		writeField(h, []byte(name))
		values := header.Values(name)
		writeLen(h, len(values))
		for _, v := range values {
			writeField(h, []byte(v))
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

// writeField writes a length-prefixed field so that distinct inputs can never
// collide by concatenation.
func writeField(h interface{ Write([]byte) (int, error) }, p []byte) {
	writeLen(h, len(p))
	h.Write(p)
}

func writeLen(h interface{ Write([]byte) (int, error) }, n int) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	h.Write(b[:])
}

// canonicalJSON re-encodes JSON with sorted object keys, so that clients
// sending the same variables in a different order share a snapshot. Numbers
// are kept verbatim. Invalid or empty input is returned as-is.
func canonicalJSON(raw []byte) []byte {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw
	}
	if m, ok := v.(map[string]any); ok && len(m) == 0 {
		return nil // `{}` and no variables are the same operation input
	}
	out, err := json.Marshal(v) // encoding/json sorts map keys
	if err != nil {
		return raw
	}
	return out
}
