#!/usr/bin/env bash
# Local dev stack for this project: CouchDB (podman), the hub API, and the
# web calendar in dev mode (auto-rebuild + static serve).
#
#   ./scripts/dev.sh            start everything (default)
#   ./scripts/dev.sh stop       stop the hub/web/watcher and the container
#
# Overridable env: COUCH_PORT, HUB_PORT, WEB_PORT, COUCH_USER,
# COUCH_PASSWORD, HUB_DEVICE_TOKENS (hub token; also what you type in the
# app when prompted), IMAGE.
set -euo pipefail

PROJECT=sync-stuff-opinionated
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORK="$ROOT/third_party/tutanota"
BUILD_DIR="$FORK/build-calendar-app"
RUN_DIR="/tmp/${PROJECT}-dev"

IMAGE=${IMAGE:-docker.io/library/couchdb:3.3}
CONTAINER="${PROJECT}-couchdb"
VOLUME="${PROJECT}-couchdb-data"
COUCH_PORT=${COUCH_PORT:-5984}
COUCH_USER=${COUCH_USER:-hub}
COUCH_PASSWORD=${COUCH_PASSWORD:-hub-password}
HUB_PORT=${HUB_PORT:-8080}
HUB_TOKEN=${HUB_DEVICE_TOKENS:-dev-token-1}
WEB_PORT=${WEB_PORT:-9000}
WATCH_POLL=${WATCH_POLL:-2}
RETRY_COOLDOWN=${RETRY_COOLDOWN:-30}

RUST_SRC_DIRS=("$ROOT/crates/client-web/src" "$ROOT/crates/client-core/src" "$ROOT/crates/common/src")
WASM_SRC_DIR="$FORK/src/applications/calendar-app/filesync"

MK="$RUN_DIR/build.marker"

log() { echo "[$PROJECT] $*"; }
die() { echo "[$PROJECT] error: $*" >&2; exit 1; }

js_runner() {
  if command -v node >/dev/null 2>&1; then echo node
  elif command -v bun >/dev/null 2>&1; then echo bun
  else echo ""; fi
}

newest_mtime() {
  find "$@" -type f -printf '%T@\n' 2>/dev/null | sort -rn | head -1
}

source_changed() { # ref file, then dirs
  local ref=$1; shift
  find "$@" -type f -newer "$ref" -print -quit 2>/dev/null | grep -q .
}

changed_ts() { source_changed "$MK" "$FORK/src"; }
changed_rust() { source_changed "$MK" "${RUST_SRC_DIRS[@]}"; }

snapshot() {
  mkdir -p "$RUN_DIR/snap"
  rm -rf "$RUN_DIR/snap"/*
  cp -a "$BUILD_DIR"/. "$RUN_DIR/snap/" 2>/dev/null || true
}

restore_snapshot() {
  if [ -d "$RUN_DIR/snap" ] && ls -A "$RUN_DIR/snap" >/dev/null 2>&1; then
    cp -a "$RUN_DIR/snap"/. "$BUILD_DIR/" 2>/dev/null || true
  fi
}

# `webapp prod` empties BUILD_DIR and does not write index.html/index-app.html
# back, so restore whatever host pages the snapshot had.
restore_index() {
  for f in index.html index-app.html; do
    if [ ! -e "$BUILD_DIR/$f" ] && [ -e "$RUN_DIR/snap/$f" ]; then
      cp "$RUN_DIR/snap/$f" "$BUILD_DIR/$f"
    fi
  done
}

rebuild() {
  log "rebuilding web client..."
  snapshot
  local need_ts=false need_wasm=false
  # No current build at all: always run the app build.
  [ -f "$BUILD_DIR/app.js" ] || need_ts=true
  if changed_rust; then need_wasm=true; fi
  if changed_ts; then need_ts=true; fi

  if $need_wasm; then
    if ! command -v wasm-pack >/dev/null 2>&1; then
      log "rebuild failed: wasm-pack not found"
      restore_snapshot; return 1
    fi
    log "rust changed: rebuilding wasm..."
    (cd "$ROOT" && wasm-pack build crates/client-web --target web >/dev/null) || { restore_snapshot; return 1; }
    cp -f "$ROOT/crates/client-web/pkg/client_web.js" \
      "$ROOT/crates/client-web/pkg/client_web_bg.wasm" \
      "$ROOT/crates/client-web/pkg/client_web.d.ts" "$WASM_SRC_DIR/"
  fi

  if $need_ts || $need_wasm; then
    local runner; runner=$(js_runner)
    if [ -z "$runner" ]; then
      log "rebuild failed: no node or bun on PATH"
      restore_snapshot; return 1
    fi
    (cd "$FORK" && "$runner" webapp.js prod --app calendar --disable-minify) || { restore_snapshot; return 1; }
  fi

  restore_index
  rm -rf "$RUN_DIR/snap"
  log "web client rebuilt"
}

watch_loop() {
  local last_attempt=0
  while :; do
    local now; now=$(date +%s)
    if { changed_ts || changed_rust; } && [ $((now - last_attempt)) -ge "$RETRY_COOLDOWN" ]; then
      last_attempt=$now
      if rebuild; then
        touch "$MK"
      else
        log "rebuild failed; retrying in ${RETRY_COOLDOWN}s"
      fi
    fi
    sleep "$WATCH_POLL"
  done
}

ensure_couch() {
  if podman container exists "$CONTAINER" 2>/dev/null; then
    local state
    state=$(podman inspect --format '{{.State.Status}}' "$CONTAINER" 2>/dev/null || true)
    if [ "$state" != "running" ]; then
      podman start "$CONTAINER" >/dev/null
    fi
  else
    podman volume exists "$VOLUME" 2>/dev/null || podman volume create "$VOLUME" >/dev/null
    podman run -d --name "$CONTAINER" -p "$COUCH_PORT:5984" \
      -v "$VOLUME:/opt/couchdb/data" \
      -e COUCHDB_USER="$COUCH_USER" -e COUCHDB_PASSWORD="$COUCH_PASSWORD" \
      "$IMAGE" >/dev/null
  fi
  log "waiting for CouchDB on :$COUCH_PORT..."
  local i
  for i in $(seq 1 120); do
    curl -sf -u "$COUCH_USER:$COUCH_PASSWORD" "http://127.0.0.1:$COUCH_PORT/" >/dev/null 2>&1 && return 0
    sleep 1
  done
  die "CouchDB did not become ready on :$COUCH_PORT"
}

start_hub() {
  log "building hub-api..."
  (cd "$ROOT" && cargo build -p hub-api >/dev/null)
  log "starting hub-api on :$HUB_PORT..."
  local envs=(COUCH_URL="http://127.0.0.1:$COUCH_PORT" COUCH_USER="$COUCH_USER"
    COUCH_PASSWORD="$COUCH_PASSWORD" HUB_BIND_ADDR="0.0.0.0:$HUB_PORT"
    HUB_DEVICE_TOKENS="$HUB_TOKEN")
  env "${envs[@]}" "$ROOT/target/debug/hub-api" >"$RUN_DIR/hub.log" 2>&1 &
  echo $! >"$RUN_DIR/hub.pid"
  local i
  for i in $(seq 1 300); do
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$HUB_PORT/changes" || true)
    [ "$code" != "000" ] && [ -n "$code" ] && return 0
    sleep 1
  done
  log "hub-api did not become ready; see $RUN_DIR/hub.log"
}

start_web() {
  local runner
  if [ ! -f "$BUILD_DIR/app.js" ]; then
    log "no existing web build; attempting one"
    if ! rebuild; then
      log "initial build failed (is the emsdk/bun toolchain set up?). Not starting the web client."
      return 1
    fi
    touch "$MK"
  else
    # Rebuild now if sources moved past the current build.
    local src newest
    src=$(newest_mtime "$FORK/src" "${RUST_SRC_DIRS[@]}")
    newest=$(newest_mtime "$BUILD_DIR")
    if [ -n "$src" ] && [ -n "$newest" ] && awk "BEGIN{exit !($src > $newest)}"; then
      log "sources are newer than the build; rebuilding"
      if ! rebuild; then
        log "rebuild failed; serving the previous build"
        restore_snapshot
      fi
    fi
    touch "$MK"
  fi

  python3 -m http.server "$WEB_PORT" --bind 127.0.0.1 --directory "$BUILD_DIR" \
    >"$RUN_DIR/web.log" 2>&1 &
  echo $! >"$RUN_DIR/web.pid"
  log "serving $BUILD_DIR on http://localhost:$WEB_PORT"
}

start_all() {
  mkdir -p "$RUN_DIR"
  ensure_couch
  start_hub
  if start_web; then
    watch_loop >"$RUN_DIR/watch.log" 2>&1 &
    echo $! >"$RUN_DIR/watch.pid"
  fi
  cat <<EOF

Up:
  CouchDB   container $CONTAINER  (:${COUCH_PORT})
  hub-api   PID $(cat "$RUN_DIR/hub.pid")   (http://localhost:${HUB_PORT}, token: $HUB_TOKEN)
  web       PID $(cat "$RUN_DIR/web.pid")   (http://localhost:${WEB_PORT})

Open http://localhost:${WEB_PORT} - when the app asks for the hub/token, use
http://localhost:${HUB_PORT} and $HUB_TOKEN. Edit TS under $FORK/src or the
Rust under crates/client-{web,core} and it rebuilds automatically.
Stop everything with: $0 stop
EOF
}

stop_all() {
  for pidf in hub.pid web.pid watch.pid; do
    if [ -f "$RUN_DIR/$pidf" ]; then
      kill "$(cat "$RUN_DIR/$pidf")" 2>/dev/null || true
      rm -f "$RUN_DIR/$pidf"
    fi
  done
  if podman container exists "$CONTAINER" 2>/dev/null; then
    podman stop "$CONTAINER" >/dev/null 2>&1 || true
  fi
  log "stopped hub, web, watcher, and $CONTAINER (volume $VOLUME kept)"
}

case "${1:-start}" in
  start) start_all ;;
  stop) stop_all ;;
  *) die "usage: $0 [start|stop]" ;;
esac
