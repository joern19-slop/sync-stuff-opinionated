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

fn couch_image(container_name: &str, network: &str) -> impl testcontainers::Image {
    GenericImage::new("couchdb", "3.3")
        .with_wait_for(WaitFor::message_on_stdout("Apache CouchDB has started"))
        .with_exposed_port(ContainerPort::Tcp(5984))
        .with_env_var("COUCHDB_USER", COUCH_USER)
        .with_env_var("COUCHDB_PASSWORD", COUCH_PASS)
        .with_network(network)
        .with_container_name(container_name)
}

async fn bootstrap_single_node(http: &reqwest::Client, base_url: &str) {
    let resp = http
        .post(format!("{base_url}/_cluster_setup"))
        .basic_auth(COUCH_USER, Some(COUCH_PASS))
        .json(&serde_json::json!({
            "action": "enable_single_node",
            "username": COUCH_USER,
            "password": COUCH_PASS,
            "bind_address": "0.0.0.0",
            "port": 5984,
            "singlenode": true
        }))
        .send()
        .await
        .expect("cluster setup request");
    if !resp.status().is_success() {
        // Recent official images auto-finish single-node setup already;
        // only fail the test on a *different* error.
        let body = resp.text().await.unwrap_or_default();
        assert!(
            body.contains("cluster_finished") || body.contains("already"),
            "unexpected cluster setup failure: {body}"
        );
    }

    let resp = http
        .put(format!("{base_url}/{DB}"))
        .basic_auth(COUCH_USER, Some(COUCH_PASS))
        .send()
        .await
        .expect("create db request");
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 412,
        "failed to create db: {}",
        resp.status()
    );
}

async fn wait_for_doc(http: &reqwest::Client, base_url: &str, id: &str, timeout: Duration) -> serde_json::Value {
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
    let a_url = format!("http://localhost:{a_port}");
    let b_url = format!("http://localhost:{b_port}");

    bootstrap_single_node(&http, &a_url).await;
    bootstrap_single_node(&http, &b_url).await;

    // Wire up bidirectional continuous replication using each node's
    // in-network hostname (they share `network`, so "node-a"/"node-b"
    // resolve to each other from *inside* the containers - unlike the
    // localhost:port URLs the test itself uses from the host side).
    for (at_url, target_host, repl_id) in [
        (&a_url, "node-b", "a-to-b"),
        (&b_url, "node-a", "b-to-a"),
    ] {
        let resp = http
            .put(format!("{at_url}/_replicator/{repl_id}"))
            .basic_auth(COUCH_USER, Some(COUCH_PASS))
            .json(&serde_json::json!({
                "source": DB,
                "target": format!("http://{COUCH_USER}:{COUCH_PASS}@{target_host}:5984/{DB}"),
                "continuous": true
            }))
            .send()
            .await
            .expect("create replication doc");
        assert!(resp.status().is_success(), "replication setup failed: {}", resp.status());
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
    assert!(resp.status().is_success(), "b should still serve reads with a down");

    let resp = http
        .put(format!("{b_url}/{DB}/written-while-a-down"))
        .basic_auth(COUCH_USER, Some(COUCH_PASS))
        .json(&serde_json::json!({ "msg": "b is fine alone" }))
        .send()
        .await
        .expect("write to b after a is down");
    assert!(resp.status().is_success(), "b should still accept writes with a down");
}
