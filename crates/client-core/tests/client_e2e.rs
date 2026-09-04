//! Client core e2e: run the `SyncEngine` against a real hub (axum service +
//! CouchDB in a testcontainer) to prove push/pull round-trips, incremental
//! checkpointing, concurrent-edit resolution, deletion propagation, binary
//! fidelity, path encoding, and multi-hub failover.
//!
//! Run explicitly: `cargo test -p client-core --test client_e2e -- --ignored --nocapture`

use std::collections::HashSet;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use client_core::{BlobStore, HubClient, MemStore, SyncEngine};
use hub_api::config::Config;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const DEVICE_TOKEN: &str = "test-device-token";

/// Starts CouchDB + the hub API in-process and returns the hub's base URL and
/// the container handle (kept alive for the whole test).
async fn start_hub() -> (String, ContainerAsync<GenericImage>) {
    let _ = tracing_subscriber::fmt().try_init();

    let couch = GenericImage::new("couchdb", "3.3")
        .with_wait_for(WaitFor::message_on_stderr("Apache CouchDB has started"))
        .with_exposed_port(ContainerPort::Tcp(5984))
        .with_env_var("COUCHDB_USER", "hub")
        .with_env_var("COUCHDB_PASSWORD", "hub-password")
        .start()
        .await
        .expect("start couchdb");
    let couch_port = couch.get_host_port_ipv4(5984).await.expect("couch port");
    // 127.0.0.1, not `localhost`: podman's IPv6 port forwarding drops bodies.
    let couch_url = format!("http://127.0.0.1:{couch_port}");

    let mut device_tokens = HashSet::new();
    device_tokens.insert(DEVICE_TOKEN.to_string());
    let cfg = Config {
        bind_addr: "127.0.0.1:0".to_string(),
        couch_url,
        couch_db: "filesync".to_string(),
        couch_user: "hub".to_string(),
        couch_password: "hub-password".to_string(),
        device_tokens,
        fcm_server_key: None,
        fcm_device_tokens: vec![],
        discord_webhook_url: None,
        watcher_poll_secs: 2,
        repl_staleness_secs: 300,
    };

    let app = hub_api::build_app(&cfg).await.expect("build app");
    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr)
        .await
        .expect("bind hub-api");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve hub-api");
    });

    (format!("http://{addr}"), couch)
}

fn engine_with(hub_urls: Vec<String>, store: Arc<dyn BlobStore>) -> SyncEngine {
    let hubs = hub_urls
        .into_iter()
        .map(|u| HubClient::new(u, DEVICE_TOKEN))
        .collect();
    SyncEngine::new(hubs, store)
}

fn engine(hub_urls: Vec<String>) -> SyncEngine {
    engine_with(hub_urls, Arc::new(MemStore::default()))
}

async fn read_content(e: &SyncEngine, path: &str) -> Option<Vec<u8>> {
    let stored = e.read_file(path).await.unwrap()?;
    STANDARD.decode(stored.content_base64).ok()
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn two_devices_roundtrip_and_incrementally_checkpoint() {
    let (hub_url, _couch) = start_hub().await;

    // Device A pushes two files (a subdirectory path exercises path
    // encoding through the hub's wildcard route).
    let a = engine(vec![hub_url.clone()]);
    a.record_upsert("notes/hello.txt", 1, "text/plain", b"hello from A")
        .await
        .unwrap();
    a.record_upsert("readme.txt", 2, "text/plain", b"top-level file")
        .await
        .unwrap();
    let report = a.sync().await.unwrap();
    assert_eq!(report.push.pushed, 2);
    assert!(a.pending().await.unwrap().is_empty());

    // Device B, starting from nothing, pulls both.
    let b = engine(vec![hub_url.clone()]);
    let report = b.sync().await.unwrap();
    assert_eq!(report.pull.pulled, 2);
    assert_eq!(
        read_content(&b, "notes/hello.txt").await,
        Some(b"hello from A".to_vec())
    );

    // Device A adds a third file; B's next sync must pull only that one.
    a.record_upsert("third.txt", 3, "text/plain", b"third")
        .await
        .unwrap();
    a.sync().await.unwrap();

    let report = b.sync().await.unwrap();
    assert_eq!(report.pull.pulled, 1);
    assert_eq!(read_content(&b, "third.txt").await, Some(b"third".to_vec()));
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn concurrent_edit_to_same_path_merges_cleanly() {
    let (hub_url, _couch) = start_hub().await;
    let a = engine(vec![hub_url.clone()]);
    let b = engine(vec![hub_url.clone()]);

    // Seed a file both devices pull. The two edits below land on *separated*
    // lines (b and d, with an unchanged c between them), which diff3 can
    // merge cleanly.
    a.record_upsert("note.txt", 1, "text/plain", b"a\nb\nc\nd\n")
        .await
        .unwrap();
    a.sync().await.unwrap();
    b.sync().await.unwrap();

    // Two devices diverge on different, non-adjacent lines of the same base.
    a.record_upsert("note.txt", 2, "text/plain", b"a\nB1\nc\nd\n")
        .await
        .unwrap();
    b.record_upsert("note.txt", 3, "text/plain", b"a\nb\nc\nD1\n")
        .await
        .unwrap();

    // Neither push is rejected as a conflict; the hub branches + merges.
    let ra = a.sync().await.unwrap();
    assert_eq!(ra.push.pushed, 1);
    assert!(ra.push.conflicts.is_empty());
    let rb = b.sync().await.unwrap();
    assert_eq!(rb.push.pushed, 1);
    assert!(rb.push.conflicts.is_empty());

    // Both converge to the diff3 merge of the two edits.
    a.sync().await.unwrap();
    assert_eq!(
        read_content(&a, "note.txt").await,
        Some(b"a\nB1\nc\nD1\n".to_vec())
    );
    assert_eq!(
        read_content(&b, "note.txt").await,
        Some(b"a\nB1\nc\nD1\n".to_vec())
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn concurrent_adjacent_edit_keeps_newer_and_converges() {
    let (hub_url, _couch) = start_hub().await;
    let a = engine(vec![hub_url.clone()]);
    let b = engine(vec![hub_url.clone()]);

    a.record_upsert("note.txt", 1, "text/plain", b"a\nb\nc\n")
        .await
        .unwrap();
    a.sync().await.unwrap();
    b.sync().await.unwrap();

    // Both edit the same line - diff3 conflicts, so the hub keeps the newer
    // (by mtime) and preserves the older as a `.conflict-*` copy.
    a.record_upsert("note.txt", 2, "text/plain", b"a\nB1\nc\n")
        .await
        .unwrap();
    b.record_upsert("note.txt", 3, "text/plain", b"a\nB2\nc\n")
        .await
        .unwrap();

    let ra = a.sync().await.unwrap();
    assert_eq!(ra.push.pushed, 1);
    assert!(ra.push.conflicts.is_empty());
    let rb = b.sync().await.unwrap();
    assert_eq!(rb.push.pushed, 1);
    assert!(rb.push.conflicts.is_empty());

    // The newer edit (B2, mtime 3) wins; both devices converge to it.
    a.sync().await.unwrap();
    assert_eq!(
        read_content(&a, "note.txt").await,
        Some(b"a\nB2\nc\n".to_vec())
    );
    assert_eq!(
        read_content(&b, "note.txt").await,
        Some(b"a\nB2\nc\n".to_vec())
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn delete_propagates_and_removes_local_file() {
    let (hub_url, _couch) = start_hub().await;
    let a = engine(vec![hub_url.clone()]);
    let b = engine(vec![hub_url.clone()]);

    a.record_upsert("gone.txt", 1, "text/plain", b"temporary")
        .await
        .unwrap();
    a.sync().await.unwrap();
    b.sync().await.unwrap();
    assert!(b.read_file("gone.txt").await.unwrap().is_some());

    a.record_delete("gone.txt", 2).await.unwrap();
    a.sync().await.unwrap();
    assert!(a.read_file("gone.txt").await.unwrap().is_none());

    b.sync().await.unwrap();
    assert!(b.read_file("gone.txt").await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn binary_content_roundtrips_byte_for_byte() {
    let (hub_url, _couch) = start_hub().await;
    let a = engine(vec![hub_url.clone()]);
    let b = engine(vec![hub_url.clone()]);

    let bytes: Vec<u8> = (0u16..=255).map(|i| i as u8).collect();
    a.record_upsert("bin.dat", 1, "application/octet-stream", &bytes)
        .await
        .unwrap();
    a.sync().await.unwrap();

    b.sync().await.unwrap();
    assert_eq!(read_content(&b, "bin.dat").await, Some(bytes));
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn paths_with_spaces_roundtrip() {
    let (hub_url, _couch) = start_hub().await;
    let a = engine(vec![hub_url.clone()]);
    let b = engine(vec![hub_url.clone()]);

    a.record_upsert(
        "folder/my file (1).txt",
        1,
        "text/plain",
        b"spaces are fine",
    )
    .await
    .unwrap();
    a.sync().await.unwrap();

    b.sync().await.unwrap();
    assert_eq!(
        read_content(&b, "folder/my file (1).txt").await,
        Some(b"spaces are fine".to_vec())
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn checkpoint_persists_across_engine_restart() {
    let (hub_url, _couch) = start_hub().await;
    let store: Arc<dyn BlobStore> = Arc::new(MemStore::default());

    let a = engine_with(vec![hub_url.clone()], store.clone());
    a.record_upsert("a.txt", 1, "text/plain", b"hello")
        .await
        .unwrap();
    a.sync().await.unwrap();

    // "Restart": a brand-new engine over the same durable store must not
    // re-pull what it already checkpointed.
    let b = engine_with(vec![hub_url], store);
    let report = b.sync().await.unwrap();
    assert_eq!(report.pull.pulled, 0);
    assert_eq!(read_content(&b, "a.txt").await, Some(b"hello".to_vec()));
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn failover_reaches_a_live_hub_when_the_first_is_down() {
    let (hub_url, _couch) = start_hub().await;

    // A guaranteed-unreachable first hub.
    let dead_addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };

    let client = engine(vec![format!("http://{dead_addr}"), hub_url.clone()]);
    client
        .record_upsert("failover.txt", 1, "text/plain", b"survived the outage")
        .await
        .unwrap();
    let report = client.sync().await.unwrap();
    assert_eq!(report.push.pushed, 1);

    // The file really did land on the live hub: a fresh device can pull it.
    let fresh = engine(vec![hub_url]);
    fresh.sync().await.unwrap();
    assert_eq!(
        read_content(&fresh, "failover.txt").await,
        Some(b"survived the outage".to_vec())
    );
}
