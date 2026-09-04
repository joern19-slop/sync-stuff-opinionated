#!/usr/bin/env bash
# Bootstraps the two-node docker-compose setup into the Stage 1 redundancy
# proof: two standalone CouchDB nodes, each continuously replicating its
# "filesync" db to the other. Safe to re-run.
set -euo pipefail

USER=hub
PASS=hub-password
DB=filesync

NODE_A=http://localhost:5984
NODE_B=http://localhost:5985

wait_for() {
  local url=$1
  until curl -sf -u "$USER:$PASS" "$url/" >/dev/null 2>&1; do
    echo "waiting for $url ..."
    sleep 1
  done
}

echo "waiting for both nodes to accept requests..."
wait_for "$NODE_A"
wait_for "$NODE_B"

# Recent official couchdb images auto-finish single-node setup when
# COUCHDB_USER/COUCHDB_PASSWORD are set at container start, so this call is
# often a no-op that just confirms setup - it's kept here (with `|| true`)
# because that behavior isn't guaranteed across every image version.
bootstrap_single_node() {
  local url=$1
  curl -s -u "$USER:$PASS" -X POST "$url/_cluster_setup" \
    -H 'Content-Type: application/json' \
    -d "{\"action\":\"enable_single_node\",\"username\":\"$USER\",\"password\":\"$PASS\",\"bind_address\":\"0.0.0.0\",\"port\":5984,\"singlenode\":true}" \
    >/dev/null || true
}
bootstrap_single_node "$NODE_A"
bootstrap_single_node "$NODE_B"

echo "ensuring $DB exists on both nodes..."
curl -s -u "$USER:$PASS" -X PUT "$NODE_A/$DB" >/dev/null || true
curl -s -u "$USER:$PASS" -X PUT "$NODE_B/$DB" >/dev/null || true

# One continuous *push* replication configured at each node, addressing the
# other by its docker-compose service name (both containers share the
# compose network, so "node-a"/"node-b" resolve there even though the host
# only sees them on localhost:5984/5985).
create_replication() {
  local at_url=$1 target_service=$2 repl_id=$3
  curl -s -u "$USER:$PASS" -X PUT "$at_url/_replicator/$repl_id" \
    -H 'Content-Type: application/json' \
    -d "{\"source\":\"$DB\",\"target\":\"http://$USER:$PASS@$target_service:5984/$DB\",\"continuous\":true}" \
    >/dev/null || true
}

echo "configuring bidirectional continuous replication..."
create_replication "$NODE_A" node-b "a-to-b"
create_replication "$NODE_B" node-a "b-to-a"

echo "done. Try:"
echo "  curl -u $USER:$PASS -X PUT $NODE_A/$DB/hello -d '{\"msg\":\"hi\"}' -H 'Content-Type: application/json'"
echo "  sleep 2 && curl -u $USER:$PASS $NODE_B/$DB/hello"
