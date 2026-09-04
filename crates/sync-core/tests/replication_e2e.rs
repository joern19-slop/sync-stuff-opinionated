//! Stage 1 of the build order, automated: prove two-node CouchDB
//! redundancy with *zero custom code* on the replication path - the only
//! code here is the test itself, driving CouchDB's own REST API.
//!
//! Requires a working Docker daemon. Not run by default: `cargo test`
//! skips `#[ignore]`d tests, so run explicitly with
//! `cargo test --test replication_e2e -- --ignored --nocapture`.
//!
//! NOTE ON VERSION RISK: the `testcontainers` crate's API (network/hostname
//! wiring in particular, via `ImageExt::with_network` +
//! `with_container_name`) has changed across versions and this was written
//! without the ability to compile-check it. If this file doesn't compile
//! against the `testcontainers` version you land on, check its docs.rs
//! page for the current network API - the `docker-compose.yml` +
//! `deploy/init-replication.sh` at the repo root prove the identical thing
//! without depending on this crate at all, and are the more reliable
//! reference if this test needs adjustment.

use std::time::Duration;

use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const COUCH_USER: &str = "hub";
const COUCH_PASS: &str = "hub-password";
const DB: &str = "filesync";

fn couch_image(
  container_name: &str,
  network: &str,
) -> testcontainers::ContainerRequest<testcontainers::GenericImage> {
  GenericImage::new("couchdb", "3.3")
    .with_wait_for(WaitFor::message_on_stderr("Apache CouchDB has started"))
    .with_exposed_port(ContainerPort::Tcp(5984))
    .with_env_var("COUCHDB_USER", COUCH_USER)
    .with_env_var("COUCHDB_PASSWORD", COUCH_PASS)
    .with_network(network)
    .with_container_name(container_name)
}

async fn ensure_db(http: &reqwest::Client, base_url: &str) {
  // The official couchdb image auto-finishes single-node setup when
  // COUCHDB_USER/COUCHDB_PASSWORD are set (the admin is asserted in its
  // preflight check *before* the "Apache CouchDB has started" line the
  // container's wait condition matches). No manual `_cluster_setup` call -
  // re-issuing it against an already-setup node restarts chttpd and breaks
  // the next request.
  //
  // `_replicator` is not auto-created on a fresh single-node install, so
  // create it alongside the data db - writing a replication doc to a
  // missing `_replicator` db is a 404.
  for db in [DB, "_replicator"] {
    let resp = http
      .put(format!("{base_url}/{db}"))
      .basic_auth(COUCH_USER, Some(COUCH_PASS))
      .send()
      .await
      .expect("create db request");
    assert!(
      resp.status().is_success() || resp.status().as_u16() == 412,
      "failed to create db {db}: {}",
      resp.status()
    );
  }
}

async fn wait_for_doc(
  http: &reqwest::Client,
  base_url: &str,
  id: &str,
  timeout: Duration,
) -> serde_json::Value {
  let deadline = tokio::time::Instant::now() + timeout;
  loop {
    let resp = http
      .get(format!("{base_url}/{DB}/{id}"))
      .basic_auth(COUCH_USER, Some(COUCH_PASS))
      .send()
      .await
      .expect("get doc request");
    if resp.status().is_success() {
      return resp.json().await.expect("doc json");
    }
    if tokio::time::Instant::now() >= deadline {
      panic!("doc {id} did not replicate to {base_url} within {timeout:?}");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
  }
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn two_nodes_replicate_bidirectionally_and_survive_one_going_down() {
  let network = "filesync-e2e-net";
  let http = reqwest::Client::new();

  let node_a = couch_image("node-a", network)
    .start()
    .await
    .expect("start node-a");
  let node_b = couch_image("node-b", network)
    .start()
    .await
    .expect("start node-b");

  let a_port = node_a.get_host_port_ipv4(5984).await.expect("node-a port");
  let b_port = node_b.get_host_port_ipv4(5984).await.expect("node-b port");
  // 127.0.0.1, not `localhost`: podman's IPv6 (`::1`) port forwarding drops
  // request bodies, so force IPv4.
  let a_url = format!("http://127.0.0.1:{a_port}");
  let b_url = format!("http://127.0.0.1:{b_port}");

  ensure_db(&http, &a_url).await;
  ensure_db(&http, &b_url).await;

  // Wire up bidirectional continuous replication using each node's
  // in-network hostname (they share `network`, so "node-a"/"node-b"
  // resolve to each other from *inside* the containers - unlike the
  // localhost:port URLs the test itself uses from the host side).
  //
  // Both source and target are full URLs (not the bare local db name):
  // CouchDB 3.2+ rejects `_replicator` docs whose source/target is a local
  // endpoint ("local_endpoints_not_supported"), so the source points back
  // at the node itself by name.
  for (at_url, source_host, target_host, repl_id) in [
    (&a_url, "node-a", "node-b", "a-to-b"),
    (&b_url, "node-b", "node-a", "b-to-a"),
  ] {
    let resp = http
      .put(format!("{at_url}/_replicator/{repl_id}"))
      .basic_auth(COUCH_USER, Some(COUCH_PASS))
      .json(&serde_json::json!({
          "source": format!("http://{COUCH_USER}:{COUCH_PASS}@{source_host}:5984/{DB}"),
          "target": format!("http://{COUCH_USER}:{COUCH_PASS}@{target_host}:5984/{DB}"),
          "continuous": true
      }))
      .send()
      .await
      .expect("create replication doc");
    assert!(
      resp.status().is_success(),
      "replication setup failed: {}",
      resp.status()
    );
  }

  // Write to A, confirm it shows up on B.
  let put_resp = http
    .put(format!("{a_url}/{DB}/hello-from-a"))
    .basic_auth(COUCH_USER, Some(COUCH_PASS))
    .json(&serde_json::json!({ "msg": "hi from a" }))
    .send()
    .await
    .expect("write to a");
  assert!(put_resp.status().is_success());

  let replicated = wait_for_doc(&http, &b_url, "hello-from-a", Duration::from_secs(15)).await;
  assert_eq!(replicated["msg"], "hi from a");

  // Write to B, confirm it shows up on A - proves it's bidirectional,
  // not just A -> B.
  let put_resp = http
    .put(format!("{b_url}/{DB}/hello-from-b"))
    .basic_auth(COUCH_USER, Some(COUCH_PASS))
    .json(&serde_json::json!({ "msg": "hi from b" }))
    .send()
    .await
    .expect("write to b");
  assert!(put_resp.status().is_success());

  let replicated = wait_for_doc(&http, &a_url, "hello-from-b", Duration::from_secs(15)).await;
  assert_eq!(replicated["msg"], "hi from b");

  // Stop A; B must keep serving reads *and* writes entirely on its own.
  node_a.stop().await.expect("stop node-a");

  let resp = http
    .get(format!("{b_url}/{DB}/hello-from-a"))
    .basic_auth(COUCH_USER, Some(COUCH_PASS))
    .send()
    .await
    .expect("read from b after a is down");
  assert!(
    resp.status().is_success(),
    "b should still serve reads with a down"
  );

  let resp = http
    .put(format!("{b_url}/{DB}/written-while-a-down"))
    .basic_auth(COUCH_USER, Some(COUCH_PASS))
    .json(&serde_json::json!({ "msg": "b is fine alone" }))
    .send()
    .await
    .expect("write to b after a is down");
  assert!(
    resp.status().is_success(),
    "b should still accept writes with a down"
  );
}
