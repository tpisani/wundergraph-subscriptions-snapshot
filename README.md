# wundergraph-subscriptions-snapshot

A [Cosmo Router](https://github.com/wundergraph/cosmo) custom module that sends the **latest message** of a subgraph-served GraphQL subscription to every client **as soon as it subscribes**, while keeping the router's subscription deduplication.

## The problem

With deduplication, all clients that subscribe with the same operation and variables (and the same headers forwarded to the subgraph) share **one** subscription between the router and the subgraph. A client that joins later only receives future messages, so it shows nothing until the next update.

## How it works

```
client A ──┐                          ┌── tap: remembers the last message
client B ──┼── router (dedup) ── SSE ─┤      key = operation + variables + identity headers
client C ──┘                          └── subgraph
```

The module uses two public extension points of the router:

| Hook | What it does |
|---|---|
| `EnginePostOriginHandler` (`OnOriginResponse`) | When a subgraph answers a subscription with `text/event-stream`, the response body is wrapped with a reader that **passes every byte through unchanged** to the engine and, on the side, keeps the last data message. Event parsing mirrors the engine exactly. |
| `SubscriptionOnStartHandler` (`SubscriptionOnStart`) | Runs for **every** subscribing client, including clients that join an already deduplicated subscription. If a message is stored for the client's key, it is emitted **to that client only** with `EmitEvent`. |

The stored message is the full subgraph response, in the format the engine receives. The engine treats it exactly like a message from the subgraph and resolves the client's own selection set, including entity fields fetched from other subgraphs.

**Snapshot key.** A SHA-256 digest of:

- the router's normalized operation hash,
- the variables (with sorted object keys),
- the values of the `key_headers` client request headers (default: `Authorization`).

A client only receives a snapshot recorded with the **same** `Authorization` value. Header values only enter the digest, so tokens are never kept in memory in clear text.

**Lifecycle.** The snapshot is dropped as soon as the upstream stream ends: every client left, the subgraph sent `complete` or `error`, or the connection broke. A stream that is shutting down never removes the snapshot of a newer stream for the same key, and a closed stream can never re-insert one.

**Ordering.** The module registers with priority `10000` (lower numbers run first), so authorization modules with a lower priority can reject a client in `SubscriptionOnStart` before a snapshot is emitted, and this module sees subgraph responses after other `OnOriginResponse` handlers.

## Requirement: SSE between router and subgraph

The module only observes the **SSE** protocol between the router and the subgraph. Over WebSocket, the router multiplexes many subscriptions over a single connection, which cannot be observed safely.

This does **not** affect clients: they keep using graphql-ws, SSE or multipart, and the router translates between protocols.

```bash
npx wgc subgraph publish prices --subscription-protocol sse_post --schema prices.graphql
```

Or in a local `graph.yaml`:

```yaml
subgraphs:
  - name: prices
    routing_url: http://prices:4001/graphql
    subscription:
      protocol: sse_post   # or sse (GET)
```

## Usage

Requires Go 1.26+ (the version required by the router).

```bash
go build -o router ./cmd/router
cp config.example.yaml config.yaml   # then add your graph / execution config settings
GRAPH_API_TOKEN=... ./router
```

With Docker:

```bash
docker build \
  --build-arg TARGETOS=linux --build-arg TARGETARCH=amd64 \
  --build-arg VERSION=$(git describe --tags --always --dirty) \
  --build-arg COMMIT=$(git rev-parse HEAD) \
  --build-arg DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
  -t router-snapshot .

docker run --rm -p 3002:3002 \
  -e GRAPH_API_TOKEN=... -e LISTEN_ADDR=0.0.0.0:3002 -e CONFIG_PATH=/config.yaml \
  -v "$PWD/config.yaml:/config.yaml" router-snapshot
```

If you already build a custom router with other modules, import the package in your `main.go`:

```go
import _ "github.com/tpisani/wundergraph-subscriptions-snapshot/snapshot"
```

## Configuration

All options are optional. See [`config.example.yaml`](config.example.yaml).

```yaml
modules:
  subscriptionSnapshot:
    key_headers: [Authorization]  # client headers that are part of the key; [] disables
    operations: []                # operation names that get a snapshot; empty = all
    ttl: ""                       # discard older snapshots (Go duration); empty = never
    max_entries: 10000            # max stored snapshots (LRU); 0 = default, -1 = unbounded
    max_total_bytes: 268435456    # max total snapshot size (LRU); 0 = default (256 MiB), -1 = unbounded
    max_event_bytes: 1048576      # largest SSE event tracked; 0 = default (1 MiB)
```

To isolate snapshots per tenant, for example: `key_headers: [Authorization, X-Tenant-Id]`.

The router decodes module config without reporting unknown keys, so a misspelled option is silently ignored. The module logs its effective configuration at startup (`subscription snapshot module provisioned`).

## Security and correctness notes

- **`key_headers` must reflect who is allowed to see the data.** If the subgraph returns per-user data based on something not listed there (a cookie, for example), add that header.
- **Use `key_headers: []` only for public subscriptions**, whose data is identical for every client.
- **Deduplication and snapshot keys are independent.** The router deduplicates on the headers it *forwards* to the subgraph; the snapshot key uses the *client* headers listed in `key_headers`. If `Authorization` is not forwarded, clients with different tokens share the upstream stream but never each other's snapshot.
- **Error-only messages** (no `data`) do not replace the last good state.

## Operational notes

- **In-memory, per router instance.** Every router replica keeps its own snapshots, fed by its own upstream connections, so a new replica fills itself from the first message it receives.
- **The first client** of a key opens the upstream subscription and gets no snapshot, since none exists yet. Its initial state depends on the subgraph emitting a value when the subscription starts.
- **Memory** is bounded by `max_entries` and `max_total_bytes`; events larger than `max_event_bytes` stop tracking that stream (a warning is logged) without affecting it.
- **Logs.** Debug logs report when a stream is tracked, when it stops being tracked and when a snapshot is emitted.

## Development

```bash
go test -race ./...            # unit tests + in-process router integration tests
./scripts/generate-testdata.sh # regenerate the test router config (requires Node.js)
```

The integration tests start the real Cosmo Router in-process, built from a config file through the same `RouterSupervisor` the router binary uses, against an in-process SSE subgraph that only emits a message when the test publishes one. They verify that:

- a late subscriber receives the current state immediately, over SSE and multipart, and then live updates without duplicates;
- a client with another `Authorization`, an anonymous client or a client with other arguments (variables or inline literals) never receives the snapshot, even when sharing the upstream stream;
- no stale snapshot is served after the upstream stream closes;
- with `key_headers: []`, every client shares the snapshot.

## Compatibility and upgrades

Built and tested against `github.com/wundergraph/cosmo/router` at `router@0.362.0`. Custom modules are compiled into the router binary, so every router upgrade requires a rebuild:

```bash
go get github.com/wundergraph/cosmo/router@<commit-sha-of-the-release>
go mod tidy
go test -race ./...
```

The module mirrors how the engine parses the SSE stream from the subgraph, so always run the tests when upgrading the router. Router releases are listed on the [Cosmo releases page](https://github.com/wundergraph/cosmo/releases?q=router%40&expanded=false).

## References

- [Cosmo Router custom modules](https://cosmo-docs.wundergraph.com/router/custom-modules)
- [SubscriptionOnStart handler](https://cosmo-docs.wundergraph.com/router/cosmo-streams/custom-modules/subscription-on-start)
- [Router subscriptions](https://cosmo-docs.wundergraph.com/router/subscriptions)
- [wundergraph/router-examples](https://github.com/wundergraph/router-examples)
