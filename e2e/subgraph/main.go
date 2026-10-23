// Test subgraph: serves `subscription { price(symbol) }` over GraphQL SSE.
// A global ticker bumps the price every TICK; every open stream receives it.
// GET /stats reports how many upstream streams were opened and are open.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	mu      sync.Mutex
	price   = 100
	subs    = map[chan int]struct{}{}
	opened  atomic.Int64
	current atomic.Int64
)

func main() {
	tick, _ := time.ParseDuration(os.Getenv("TICK"))
	if tick == 0 {
		tick = 4 * time.Second
	}
	go func() {
		for range time.Tick(tick) {
			mu.Lock()
			price++
			for ch := range subs {
				select {
				case ch <- price:
				default:
				}
			}
			mu.Unlock()
		}
	}()

	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"opened":%d,"open":%d}`, opened.Load(), current.Load())
	})

	http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			// Plain queries (unused by the test, but keep the subgraph valid).
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"data":{"hello":"world"}}`)
			return
		}

		symbol := ""
		for _, v := range body.Variables { // the router may rename variables
			if s, ok := v.(string); ok {
				symbol = s
			}
		}
		if symbol == "" {
			symbol = "?"
		}
		log.Printf("upstream subscription opened: query=%q vars=%v", body.Query, body.Variables)

		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		opened.Add(1)
		current.Add(1)
		defer current.Add(-1)

		ch := make(chan int, 1)
		mu.Lock()
		subs[ch] = struct{}{}
		mu.Unlock()
		defer func() { mu.Lock(); delete(subs, ch); mu.Unlock() }()

		for {
			select {
			case <-r.Context().Done():
				log.Printf("upstream subscription closed")
				return
			case p := <-ch:
				fmt.Fprintf(w, "event: next\ndata: {\"data\":{\"price\":{\"symbol\":%q,\"value\":%d}}}\n\n", symbol, p)
				flusher.Flush()
			}
		}
	})

	log.Fatal(http.ListenAndServe("127.0.0.1:4001", nil))
}
