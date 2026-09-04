//! Engine-level tests: sync logic against a `wiremock` hub and an in-memory
//! store. No Docker needed. The crash-safety test uses a store wrapper that
//! fails on a chosen write to prove the checkpoint is only advanced after a
//! full batch is durable.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use client_core::{
    checkpoint_key, file_key, FileMeta, FileStore, HubClient, MemStore, MetaStore, Notifier,
    StoreError, SyncEngine, SyncError,
};
use serde_json::json;
use wiremock::matchers::{bearer_token, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn hub_client(server: &MockServer) -> HubClient {
    HubClient::new(server.uri(), "dev-token")
}

/// A `Notifier` that records every error it's asked to surface.
#[derive(Default)]
struct CapturingNotifier {
    errors: Mutex<Vec<String>>,
}

impl Notifier for CapturingNotifier {
    fn notify_error(&self, error: &SyncError) {
        self.errors.lock().unwrap().push(error.to_string());
    }
}

impl CapturingNotifier {
    fn captured(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }
}

fn engine(
    server: &MockServer,
    meta: Arc<dyn MetaStore>,
    files: Arc<dyn FileStore>,
) -> SyncEngine {
    SyncEngine::new(vec![hub_client(server)], meta, files)
}

/// Returns a `MemStore` plus a `SyncEngine` sharing the same storage, so a
/// test can assert on the store directly while the engine owns the trait
/// objects.
fn engine_with_mem(server: &MockServer) -> (MemStore, SyncEngine) {
    let mem = MemStore::default();
    let engine = engine(server, Arc::new(mem.clone()), Arc::new(mem.clone()));
    (mem, engine)
}

fn changes_body(checkpoint: &str, entries: &[(&str, bool, &str)]) -> serde_json::Value {
    json!({
        "changes": entries.iter().map(|(p, d, r)| json!({
            "path": p, "deleted": d, "rev": r
        })).collect::<Vec<_>>(),
        "checkpoint": checkpoint,
    })
}

fn file_response(rev: &str, mtime: i64, content_type: &str, body: &[u8]) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("x-file-rev", rev)
        .insert_header("x-file-mtime", mtime.to_string())
        .insert_header("content-type", content_type)
        .set_body_bytes(body.to_vec())
}

#[tokio::test]
async fn pull_writes_batch_and_advances_checkpoint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/changes"))
        .and(query_param_is_missing("since"))
        .respond_with(ResponseTemplate::new(200).set_body_json(changes_body(
            "cp-1",
            &[("a.txt", false, "1-a"), ("b.txt", false, "1-b")],
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file/a.txt"))
        .respond_with(file_response("1-a", 1, "text/plain", b"A"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file/b.txt"))
        .respond_with(file_response("1-b", 2, "text/plain", b"B"))
        .mount(&server)
        .await;

    let (mem, engine) = engine_with_mem(&server);
    let report = engine.pull().await.unwrap();

    assert_eq!(report.pulled, 2);
    assert_eq!(report.checkpoint.as_deref(), Some("cp-1"));
    assert_eq!(
        MetaStore::get(&mem, &checkpoint_key(&server.uri()))
            .await
            .unwrap()
            .unwrap(),
        b"cp-1"
    );
    assert!(MetaStore::get(&mem, &file_key("a.txt")).await.unwrap().is_some());
    assert!(MetaStore::get(&mem, &file_key("b.txt")).await.unwrap().is_some());
    assert_eq!(FileStore::get(&mem, "a.txt").await.unwrap(), Some(b"A".to_vec()));
}

#[tokio::test]
async fn incremental_pull_uses_stored_checkpoint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/changes"))
        .and(query_param_is_missing("since"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(changes_body("cp-1", &[("a.txt", false, "1-a")])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/changes"))
        .and(query_param("since", "cp-1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(changes_body("cp-2", &[("b.txt", false, "1-b")])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file/a.txt"))
        .respond_with(file_response("1-a", 1, "text/plain", b"A"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file/b.txt"))
        .respond_with(file_response("1-b", 2, "text/plain", b"B"))
        .mount(&server)
        .await;

    let (mem, engine) = engine_with_mem(&server);
    engine.pull().await.unwrap();

    let report = engine.pull().await.unwrap();
    assert_eq!(report.pulled, 1); // only b.txt this time
    assert_eq!(report.checkpoint.as_deref(), Some("cp-2"));
    assert_eq!(
        MetaStore::get(&mem, &checkpoint_key(&server.uri()))
            .await
            .unwrap()
            .unwrap(),
        b"cp-2"
    );
}

/// A `MetaStore` wrapper that fails the first `put` of a chosen key,
/// simulating a crash mid-batch.
struct FailOnceMetaStore {
    inner: MemStore,
    fail_key: String,
    failed: AtomicBool,
}

#[async_trait]
impl MetaStore for FailOnceMetaStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        MetaStore::get(&self.inner, key).await
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StoreError> {
        if key == self.fail_key && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(StoreError::Io(format!("simulated crash writing {key}")));
        }
        MetaStore::put(&self.inner, key, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        MetaStore::delete(&self.inner, key).await
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        MetaStore::list_keys(&self.inner, prefix).await
    }
}

#[tokio::test]
async fn checkpoint_is_not_advanced_when_batch_fails_partway() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/changes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(changes_body(
            "cp-1",
            &[("a.txt", false, "1-a"), ("b.txt", false, "1-b")],
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file/a.txt"))
        .respond_with(file_response("1-a", 1, "text/plain", b"A"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file/b.txt"))
        .respond_with(file_response("1-b", 2, "text/plain", b"B"))
        .mount(&server)
        .await;

    let mem = MemStore::default();
    let meta = FailOnceMetaStore {
        inner: mem.clone(),
        fail_key: file_key("b.txt"),
        failed: AtomicBool::new(false),
    };
    let engine = SyncEngine::new(vec![hub_client(&server)], Arc::new(meta), Arc::new(mem.clone()));

    // First pull dies writing b.txt's metadata.
    assert!(engine.pull().await.is_err());
    // a.txt landed, but the checkpoint must not have advanced.
    assert!(MetaStore::get(&mem, &file_key("a.txt")).await.unwrap().is_some());
    assert!(MetaStore::get(&mem, &checkpoint_key(&server.uri()))
        .await
        .unwrap()
        .is_none());

    // Retry: same batch re-pulls (idempotent), completes, checkpoint lands.
    let report = engine.pull().await.unwrap();
    assert_eq!(report.pulled, 2);
    assert_eq!(
        MetaStore::get(&mem, &checkpoint_key(&server.uri()))
            .await
            .unwrap()
            .unwrap(),
        b"cp-1"
    );
}

#[tokio::test]
async fn push_upsert_clears_pending_and_records_new_rev() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/changes"))
        .and(bearer_token("dev-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "path": "a.txt", "status": "ok", "rev": "2-b" }
        ])))
        .mount(&server)
        .await;

    let (mem, engine) = engine_with_mem(&server);
    engine
        .record_upsert("a.txt", 1, "text/plain", b"hello")
        .await
        .unwrap();

    let report = engine.push().await.unwrap();
    assert_eq!(report.pushed, 1);
    assert!(report.conflicts.is_empty());

    // Pending queue is now empty.
    assert!(engine.pending().await.unwrap().is_empty());
    // Local file kept its content but now points at the hub's new revision.
    let meta: FileMeta = serde_json::from_slice(
        &MetaStore::get(&mem, &file_key("a.txt")).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(meta.rev, "2-b");
    assert_eq!(FileStore::get(&mem, "a.txt").await.unwrap(), Some(b"hello".to_vec()));
}

#[tokio::test]
async fn push_conflict_keeps_local_change_queued() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/changes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "path": "a.txt", "status": "conflict" }
        ])))
        .mount(&server)
        .await;

    let (mem, engine) = engine_with_mem(&server);
    engine
        .record_upsert("a.txt", 1, "text/plain", b"hello")
        .await
        .unwrap();

    let report = engine.push().await.unwrap();
    assert_eq!(report.pushed, 0);
    assert_eq!(report.conflicts, vec!["a.txt"]);

    // The local change is still queued and the content is still present.
    let pending = engine.pending().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].path, "a.txt");
    assert!(MetaStore::get(&mem, &file_key("a.txt")).await.unwrap().is_some());
    assert_eq!(FileStore::get(&mem, "a.txt").await.unwrap(), Some(b"hello".to_vec()));
}

#[tokio::test]
async fn delete_removes_local_file_and_pushes_a_delete() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/changes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "path": "a.txt", "status": "ok", "rev": "3-d" }
        ])))
        .mount(&server)
        .await;

    let (mem, engine) = engine_with_mem(&server);
    // Seed a file so the delete has a base_rev.
    engine
        .record_upsert("a.txt", 1, "text/plain", b"hello")
        .await
        .unwrap();
    engine.push().await.unwrap(); // get it to rev 2-b, clear pending

    engine.record_delete("a.txt", 2).await.unwrap();
    assert!(MetaStore::get(&mem, &file_key("a.txt")).await.unwrap().is_none());
    assert!(FileStore::get(&mem, "a.txt").await.unwrap().is_none());

    engine.push().await.unwrap();
    assert!(engine.pending().await.unwrap().is_empty());
}

#[tokio::test]
async fn list_file_paths_lists_known_paths() {
    let server = MockServer::start().await;
    let (_mem, engine) = engine_with_mem(&server);
    engine
        .record_upsert("a.txt", 1, "text/plain", b"hi")
        .await
        .unwrap();
    engine
        .record_upsert("notes/b.txt", 2, "text/plain", b"there")
        .await
        .unwrap();

    let mut paths = engine.list_file_paths().await.unwrap();
    paths.sort();
    assert_eq!(paths, vec!["a.txt", "notes/b.txt"]);
}

#[tokio::test]
async fn failover_tries_next_hub_when_first_is_unreachable() {
    // A port that was just closed: guaranteed unreachable.
    let dead_addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let dead = HubClient::new(format!("http://{dead_addr}"), "dev-token");

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/changes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "path": "a.txt", "status": "ok", "rev": "1-a" }
        ])))
        .mount(&server)
        .await;

    let mem = MemStore::default();
    let engine = SyncEngine::new(
        vec![dead, hub_client(&server)],
        Arc::new(mem.clone()),
        Arc::new(mem),
    );
    engine
        .record_upsert("a.txt", 1, "text/plain", b"hi")
        .await
        .unwrap();

    let report = engine.push().await.unwrap();
    assert_eq!(report.pushed, 1);
    assert!(engine.pending().await.unwrap().is_empty());
}

#[tokio::test]
async fn notifier_receives_unexpected_errors() {
    // A closed port: every hub is unreachable, so sync must fail and the
    // failure must be surfaced to the notifier.
    let dead_addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };

    let notifier = Arc::new(CapturingNotifier::default());
    let mem = MemStore::default();
    let engine = SyncEngine::new(
        vec![HubClient::new(format!("http://{dead_addr}"), "dev-token")],
        Arc::new(mem.clone()),
        Arc::new(mem),
    )
    .with_notifier(notifier.clone());

    engine
        .record_upsert("a.txt", 1, "text/plain", b"hi")
        .await
        .unwrap();

    let result = engine.sync().await;
    assert!(result.is_err());
    assert!(!notifier.captured().is_empty());
}

#[tokio::test]
async fn notifier_stays_silent_when_failover_recovers() {
    // First hub down, second live: the error is recovered, so it must *not*
    // be surfaced to the user.
    let dead_addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/changes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "path": "a.txt", "status": "ok", "rev": "1-a" }
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/changes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "changes": [], "checkpoint": "cp-1"
        })))
        .mount(&server)
        .await;

    let notifier = Arc::new(CapturingNotifier::default());
    let mem = MemStore::default();
    let engine = SyncEngine::new(
        vec![
            HubClient::new(format!("http://{dead_addr}"), "dev-token"),
            hub_client(&server),
        ],
        Arc::new(mem.clone()),
        Arc::new(mem),
    )
    .with_notifier(notifier.clone());

    engine
        .record_upsert("a.txt", 1, "text/plain", b"hi")
        .await
        .unwrap();

    engine.sync().await.unwrap();
    assert!(notifier.captured().is_empty());
}
