#!/usr/bin/env bash
# End-to-end check of the subscription snapshot module against a real router.
set -u
# Requires Go 1.26+ and Node.js (for `npx wgc`). Run from anywhere:
#   ./e2e/run.sh
cd "$(dirname "$0")"
mkdir -p bin out
rm -f out/*
go build -o bin/subgraph ./subgraph
go build -o bin/router ../cmd/router
npx -y wgc@latest router compose -i graph.yaml -o bin/exec.json >/dev/null
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost

TICK=4s ./bin/subgraph > out/subgraph.log 2>&1 &
SUB=$!
TELEMETRY_ENABLED=false CONFIG_PATH=config.yaml ./bin/router > out/router.log 2>&1 &
ROUTER=$!
trap 'kill $SUB $ROUTER $(jobs -p) 2>/dev/null' EXIT

for _ in $(seq 50); do curl -s 127.0.0.1:3002/health/ready >/dev/null && break; sleep 0.2; done

QUERY='{"query":"subscription($s: String!) { price(symbol: $s) { symbol value } }","variables":{"s":"ABC"}}'

# client <name> <auth> <accept>
client() {
  curl -sN -X POST 127.0.0.1:3002/graphql \
    -H "Content-Type: application/json" -H "Accept: $3" -H "Authorization: $2" \
    -d "$QUERY" | while IFS= read -r line; do
      case "$line" in *value*) printf '%s %s\n' "$(date +%s.%N)" "$line";; esac
    done > "out/$1.out" &
  echo $!
}
first_after() { # first data line of <name> and its delay after <start>
  local l; l=$(head -1 "out/$1.out")
  [ -z "$l" ] && { echo "$1: (no data)"; return; }
  awk -v s="$2" -v n="$1" '{printf "%s: first value after %.2fs -> %s\n", n, $1-s, substr($0, index($0,$2))}' <<<"$l"
}

A=$(client A "Bearer alice" text/event-stream)
sleep 9.5   # A sees two ticks

echo "A so far:"; sed 's/^/  /' out/A.out
T=$(date +%s.%N)
B=$(client B "Bearer alice" text/event-stream)
C=$(client C "Bearer bob"   text/event-stream)
E=$(client E "Bearer alice" 'multipart/mixed;subscriptionSpec="1.0", application/json')
sleep 1
echo "--- 1s after B/C/E joined (next upstream tick is ~1.5-2.5s away):"
first_after B "$T"; first_after C "$T"; first_after E "$T"
echo "upstream stats: $(curl -s 127.0.0.1:4001/stats)"

sleep 4
echo "--- after the next tick:"
first_after C "$T"
echo "B:"; sed 's/^/  /' out/B.out

# All clients leave -> router closes the upstream stream -> snapshot must go.
pkill -P $A; pkill -P $B; pkill -P $C; pkill -P $E; pkill -f "curl -sN -X POST 127.0.0.1:3002"
sleep 2
echo "upstream stats after clients left: $(curl -s 127.0.0.1:4001/stats)"
T=$(date +%s.%N)
D=$(client D "Bearer alice" text/event-stream)
sleep 1
echo "--- fresh subscriber after upstream closed:"
first_after D "$T"
echo "router warnings/errors:"; grep -iE '"level":"(warn|error)"' out/router.log | grep -v 'No graph token' | head -5
