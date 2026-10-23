#!/usr/bin/env bash
# Regenerates the router execution config used by the integration test.
# Requires Node.js. The wgc version is pinned for reproducible output.
set -euo pipefail
cd "$(dirname "$0")/../snapshot/testdata"
npx --yes wgc@0.133.3 router compose -i graph.yaml -o router_config.json
