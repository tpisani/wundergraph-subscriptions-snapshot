package snapshot

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"
)

// executionResult mirrors the engine's common.ExecutionResult. The engine
// unmarshals every SSE "next" event into this shape and passes json.Marshal of
// it to the subscription updater, so marshalling the same struct yields the
// exact bytes the engine would have produced.
type executionResult struct {
	Data       json.RawMessage `json:"data,omitempty"`
	Errors     json.RawMessage `json:"errors,omitempty"`
	Extensions json.RawMessage `json:"extensions,omitempty"`
}

// Reasons passed to onEnd.
const (
	endEventTooLarge = "event exceeds max_event_bytes"
	endUpstreamEOF   = "upstream stream ended"
	endClosed        = "upstream stream closed"
	endComplete      = "upstream sent complete"
	endError         = "upstream sent error"
)

var (
	headerData  = []byte("data:")
	headerEvent = []byte("event:")
)

// sseTap wraps an upstream SSE response body. It passes every byte through to
// the engine unchanged and, on the side, splits the stream into events with
// the same rules as the engine (r3labs/sse delimiters + graphql-go-tools event
// parsing) to report the latest data payload.
//
// The engine reads the body from one goroutine and may Close it from another
// one when the subscription ends, so all tap state is guarded by mu. onData
// and onEnd are invoked while mu is held: once onEnd has run, onData is never
// called again, which guarantees a closed stream cannot re-insert a snapshot.
type sseTap struct {
	body   io.ReadCloser
	maxBuf int
	onData func(payload []byte)
	onEnd  func(reason string)

	mu    sync.Mutex
	buf   []byte
	ended bool
}

func newSSETap(body io.ReadCloser, maxBuf int, onData func([]byte), onEnd func(reason string)) *sseTap {
	return &sseTap{body: body, maxBuf: maxBuf, onData: onData, onEnd: onEnd}
}

func (t *sseTap) Read(p []byte) (int, error) {
	n, err := t.body.Read(p)

	t.mu.Lock()
	defer t.mu.Unlock()

	if n > 0 && !t.ended {
		t.buf = append(t.buf, p[:n]...)
		t.drainLocked()
		if !t.ended && t.maxBuf > 0 && len(t.buf) > t.maxBuf {
			// An event larger than we are willing to buffer: stop tracking this
			// stream and drop its snapshot so it can never go stale.
			t.endLocked(endEventTooLarge)
		}
	}
	if err != nil {
		t.endLocked(endUpstreamEOF)
	}
	return n, err
}

func (t *sseTap) Close() error {
	err := t.body.Close()

	t.mu.Lock()
	t.endLocked(endClosed)
	t.mu.Unlock()

	return err
}

func (t *sseTap) endLocked(reason string) {
	if t.ended {
		return
	}
	t.ended = true
	t.buf = nil
	if t.onEnd != nil {
		t.onEnd(reason)
	}
}

// drainLocked extracts and handles every complete event currently buffered.
func (t *sseTap) drainLocked() {
	for !t.ended {
		i, nlen := containsDoubleNewline(t.buf)
		if i < 0 {
			break
		}
		event, rest := t.buf[:i], t.buf[i+nlen:]
		t.handleEventLocked(event)
		if t.ended {
			return // the event ended the stream and released the buffer
		}
		t.buf = rest
	}
	if len(t.buf) == 0 {
		t.buf = nil // release the backing array between events
	}
}

func (t *sseTap) handleEventLocked(raw []byte) {
	eventType, data := parseEventBytes(raw)
	switch eventType {
	case "complete":
		t.endLocked(endComplete)
	case "error":
		t.endLocked(endError)
	case "next", "":
		if eventType == "" && data == nil {
			return // keep-alive / comment
		}
		if len(data) == 0 {
			t.endLocked(endComplete) // the engine treats an empty untyped event as complete
			return
		}
		var res executionResult
		if err := json.Unmarshal(data, &res); err != nil {
			return // the engine surfaces this as an error; nothing to cache
		}
		if len(res.Data) == 0 || bytes.Equal(bytes.TrimSpace(res.Data), []byte("null")) {
			return // keep the previous good state instead of an error-only payload
		}
		payload, err := json.Marshal(&res)
		if err != nil {
			return
		}
		t.onData(payload)
	}
}

// parseEventBytes is a copy of the engine's SSE event parsing
// (graphql-go-tools subscriptionclient/transport/sse_conn.go).
func parseEventBytes(msg []byte) (eventType string, data []byte) {
	if len(msg) == 0 {
		return "", nil
	}
	for line := range bytes.Lines(msg) {
		line = bytes.TrimRight(line, "\r\n")
		switch {
		case bytes.HasPrefix(line, headerEvent):
			eventType = string(trimHeader(len(headerEvent), line))
		case bytes.HasPrefix(line, headerData):
			data = append(data, trimHeader(len(headerData), line)...)
			data = append(data, '\n')
		case bytes.Equal(line, []byte("data")):
			data = append(data, '\n')
		}
	}
	data = bytes.TrimSuffix(data, []byte("\n"))
	return eventType, data
}

func trimHeader(size int, data []byte) []byte {
	if len(data) < size {
		return data
	}
	data = data[size:]
	if len(data) > 0 && data[0] == ' ' {
		data = data[1:]
	}
	return data
}

// containsDoubleNewline is a copy of r3labs/sse's event delimiter detection,
// which the engine uses to split the stream into events.
func containsDoubleNewline(data []byte) (int, int) {
	crcr := bytes.Index(data, []byte("\r\r"))
	lflf := bytes.Index(data, []byte("\n\n"))
	crlflf := bytes.Index(data, []byte("\r\n\n"))
	lfcrlf := bytes.Index(data, []byte("\n\r\n"))
	crlfcrlf := bytes.Index(data, []byte("\r\n\r\n"))
	minPos := minPosInt(crcr, minPosInt(lflf, minPosInt(crlflf, minPosInt(lfcrlf, crlfcrlf))))
	nlen := 2
	if minPos == crlfcrlf {
		nlen = 4
	} else if minPos == crlflf || minPos == lfcrlf {
		nlen = 3
	}
	return minPos, nlen
}

func minPosInt(a, b int) int {
	if a < 0 {
		return b
	}
	if b < 0 {
		return a
	}
	if a > b {
		return b
	}
	return a
}
