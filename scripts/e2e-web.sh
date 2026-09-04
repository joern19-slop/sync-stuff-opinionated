#!/usr/bin/env bash
# End-to-end: run the browser (wasm) client's e2e tests against a live hub +
# CouchDB. Requires docker, wasm-pack, geckodriver, and a headless browser.
set -euo pipefail

HUB_PORT=8080
COUCH_PORT=5984
TOKEN=test-token
CONTAINER=filesync-e2e-couch

cleanup() {
  echo "[e2e-web] tearing down..."
  if [ -n "${HUB_PID:-}" ]; then kill "$HUB_PID" 2>/dev/null || true; fi
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[e2e-web] starting CouchDB..."
docker run -d --name "$CONTAINER" -p "$COUCH_PORT:5984" \
  -e COUCHDB_USER=hub -e COUCHDB_PASSWORD=hub-password couchdb:3.3 >/dev/null

for i in $(seq 1 120); do
  if curl -sf -u hub:hub-password "http://127.0.0.1:$COUCH_PORT/" >/dev/null 2>&1; then break; fi
  sleep 0.5
done

echo "[e2e-web] starting hub-api..."
COUCH_URL="http://127.0.0.1:$COUCH_PORT" \
COUCH_USER=hub COUCH_PASSWORD=hub-password \
HUB_DEVICE_TOKENS="$TOKEN" \
HUB_BIND_ADDR="0.0.0.0:$HUB_PORT" \
cargo run -p hub-api >/tmp/filesync-e2e-hub.log 2>&1 &
HUB_PID=$!

for i in $(seq 1 120); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$HUB_PORT/changes" || true)
  if [ -n "$code" ] && [ "$code" != "000" ]; then break; fi
  sleep 0.5
done

echo "[e2e-web] running browser tests..."
(cd crates/client-web && wasm-pack test --headless --firefox --features e2e)
