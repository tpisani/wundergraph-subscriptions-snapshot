package snapshot

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// chunkReader returns the stream in fixed-size chunks to exercise events that
// are split across Read calls.
type chunkReader struct {
	data   []byte
	size   int
	closed bool
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := min(c.size, len(p), len(c.data))
	copy(p, c.data[:n])
	c.data = c.data[n:]
	return n, nil
}

func (c *chunkReader) Close() error { c.closed = true; return nil }

type recorder struct {
	payloads []string
	ended    int
}

func (r *recorder) tap(stream string, chunk, maxBuf int) (*sseTap, *chunkReader) {
	body := &chunkReader{data: []byte(stream), size: chunk}
	return newSSETap(body, maxBuf,
		func(p []byte) { r.payloads = append(r.payloads, string(p)) },
		func() { r.ended++ },
	), body
}

// engineBytes reproduces what the engine passes to the updater for an SSE
// "next" event: json.Marshal(common.ExecutionResult{...}).
func engineBytes(t *testing.T, data string) string {
	t.Helper()
	var res executionResult
	if err := json.Unmarshal([]byte(data), &res); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(&res)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestTapPassesBytesThroughUnchanged(t *testing.T) {
	stream := "event: next\ndata: {\"data\":{\"price\":1}}\n\n: keep-alive\n\nevent: next\r\ndata: {\"data\":{\"price\":2}}\r\n\r\n"
	var r recorder
	tap, _ := r.tap(stream, 3, 1<<20)

	got, err := io.ReadAll(tap)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != stream {
		t.Fatalf("tap altered the stream:\n%q\n%q", got, stream)
	}
}

func TestTapRecordsDataEventsAcrossChunks(t *testing.T) {
	stream := strings.Join([]string{
		"event: next\ndata: {\"data\":{\"price\":1}}\n\n",
		": comment\n\n",
		"event: next\r\ndata: {\"data\":{\"price\":2},\"extensions\":{\"x\":1}}\r\n\r\n",
		"data: {\"data\":{\"price\":3}}\n\n", // untyped event = data
	}, "")

	for _, chunk := range []int{1, 2, 7, 4096} {
		var r recorder
		tap, _ := r.tap(stream, chunk, 1<<20)
		if _, err := io.ReadAll(tap); err != nil {
			t.Fatal(err)
		}
		want := []string{
			engineBytes(t, `{"data":{"price":1}}`),
			engineBytes(t, `{"data":{"price":2},"extensions":{"x":1}}`),
			engineBytes(t, `{"data":{"price":3}}`),
		}
		if strings.Join(r.payloads, "|") != strings.Join(want, "|") {
			t.Fatalf("chunk=%d: got %q want %q", chunk, r.payloads, want)
		}
		if r.ended != 1 {
			t.Fatalf("chunk=%d: expected end once at EOF, got %d", chunk, r.ended)
		}
	}
}

func TestTapMultilineData(t *testing.T) {
	var r recorder
	tap, _ := r.tap("event: next\ndata: {\"data\":\ndata: {\"a\":1}}\n\n", 5, 1<<20)
	_, _ = io.ReadAll(tap)
	if len(r.payloads) != 1 || r.payloads[0] != engineBytes(t, "{\"data\":\n{\"a\":1}}") {
		t.Fatalf("got %q", r.payloads)
	}
}

func TestTapIgnoresErrorOnlyPayloads(t *testing.T) {
	var r recorder
	tap, _ := r.tap("event: next\ndata: {\"data\":{\"a\":1}}\n\nevent: next\ndata: {\"errors\":[{\"message\":\"x\"}]}\n\nevent: next\ndata: {\"data\":null}\n\n", 64, 1<<20)
	_, _ = io.ReadAll(tap)
	if len(r.payloads) != 1 {
		t.Fatalf("only the good payload should be recorded, got %q", r.payloads)
	}
}

func TestTapEndsOnCompleteErrorAndClose(t *testing.T) {
	for name, stream := range map[string]string{
		"complete": "event: next\ndata: {\"data\":{\"a\":1}}\n\nevent: complete\n\nevent: next\ndata: {\"data\":{\"a\":2}}\n\n",
		"error":    "event: error\ndata: [{\"message\":\"boom\"}]\n\nevent: next\ndata: {\"data\":{\"a\":2}}\n\n",
	} {
		var r recorder
		tap, _ := r.tap(stream, 4096, 1<<20)
		_, _ = io.ReadAll(tap)
		if r.ended != 1 {
			t.Fatalf("%s: expected end once, got %d", name, r.ended)
		}
		for _, p := range r.payloads {
			if strings.Contains(p, `"a":2`) {
				t.Fatalf("%s: nothing after the end should be recorded, got %q", name, r.payloads)
			}
		}
	}

	var r recorder
	tap, body := r.tap("event: next\ndata: {\"data\":{\"a\":1}}\n\n", 4096, 1<<20)
	buf := make([]byte, 8)
	_, _ = tap.Read(buf)
	if err := tap.Close(); err != nil {
		t.Fatal(err)
	}
	_ = tap.Close()
	if !body.closed || r.ended != 1 {
		t.Fatalf("close must close the body and end exactly once (closed=%v ended=%d)", body.closed, r.ended)
	}
}

func TestTapStopsTrackingOversizedEvents(t *testing.T) {
	big := "event: next\ndata: {\"data\":{\"s\":\"" + strings.Repeat("x", 200) + "\"}}\n\n"
	stream := "event: next\ndata: {\"data\":{\"a\":1}}\n\n" + big + "event: next\ndata: {\"data\":{\"a\":2}}\n\n"

	var r recorder
	tap, _ := r.tap(stream, 16, 64)
	got, _ := io.ReadAll(tap)
	if !bytes.Equal(got, []byte(stream)) {
		t.Fatal("oversized events must still pass through to the engine")
	}
	if r.ended != 1 {
		t.Fatalf("expected the snapshot to be dropped once, got %d", r.ended)
	}
	if len(r.payloads) != 1 {
		t.Fatalf("nothing after the oversized event should be recorded, got %q", r.payloads)
	}
}
