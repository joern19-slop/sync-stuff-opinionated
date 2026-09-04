//! Stage 2 end-to-end: exercise the Hub Sync API's three endpoints
//! (`GET /changes`, `POST /changes`, `GET /file/{path}`) against a real
//! CouchDB, not a mock. Requires Docker.
//!
//! Run explicitly: `cargo test --test hub_api_e2e -- --ignored --nocapture`
//!
//! Same version-risk caveat as `sync-core/tests/replication_e2e.rs` applies
//! to the `testcontainers` usage below (single-container case here, so
//! lower risk than the two-node network wiring there).

use std::collections::HashSet;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use hub_api::config::Config;
use serde_json::json;
use sync_core::{ChangesResponse, PushResult, PushStatus};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const DEVICE_TOKEN: &str = "test-device-token";

// Returns the hub URL, an HTTP client, and the running CouchDB container.
// The container is returned (rather than dropped) because dropping a
// `ContainerAsync` removes it - if we let it drop in `start_hub` the
// container would be gone before the test body makes its requests.
async fn start_hub() -> (String, reqwest::Client, ContainerAsync<GenericImage>) {
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
  // 127.0.0.1, not `localhost`: podman's IPv6 (`::1`) port forwarding drops
  // request bodies, so force IPv4.
  let couch_url = format!("http://127.0.0.1:{couch_port}");

  // The official couchdb image auto-finishes single-node setup when
  // COUCHDB_USER/COUCHDB_PASSWORD are set (the admin is asserted in its
  // preflight check *before* "Apache CouchDB has started" is logged, which
  // is what the container's wait condition above waits for). No manual
  // `_cluster_setup` call - re-issuing it against an already-setup node
  // restarts chttpd and breaks the next request.
  let http = reqwest::Client::new();

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

  (format!("http://{addr}"), http, couch)
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn push_then_pull_then_fetch_roundtrips_a_file() {
  let (hub_url, http, _couch) = start_hub().await;

  // Unauthenticated request is rejected.
  let resp = http.get(format!("{hub_url}/changes")).send().await.unwrap();
  assert_eq!(resp.status(), 401);

  // Push a new file.
  let push_body = json!([{
      "path": "notes/hello.txt",
      "deleted": false,
      "base_rev": null,
      "mtime": 1_700_000_000,
      "content_type": "text/plain",
      "content_base64": STANDARD.encode(b"hello, hub"),
  }]);
  let resp = http
    .post(format!("{hub_url}/changes"))
    .bearer_auth(DEVICE_TOKEN)
    .json(&push_body)
    .send()
    .await
    .unwrap();
  assert!(resp.status().is_success(), "push failed: {}", resp.status());
  let results: Vec<PushResult> = resp.json().await.unwrap();
  assert_eq!(results.len(), 1);
  assert_eq!(results[0].path, "notes/hello.txt");
  let PushStatus::Ok { rev } = &results[0].status else {
    panic!("expected Ok, got {:?}", results[0].status);
  };
  let first_rev = rev.clone();

  // Pulling changes surfaces the new path.
  let resp = http
    .get(format!("{hub_url}/changes"))
    .bearer_auth(DEVICE_TOKEN)
    .send()
    .await
    .unwrap();
  assert!(resp.status().is_success());
  let changes: ChangesResponse = resp.json().await.unwrap();
  assert!(changes
    .changes
    .iter()
    .any(|c| c.path == "notes/hello.txt" && !c.deleted));

  // Fetching the file returns exactly what was pushed.
  let resp = http
    .get(format!("{hub_url}/file/notes/hello.txt"))
    .bearer_auth(DEVICE_TOKEN)
    .send()
    .await
    .unwrap();
  assert!(resp.status().is_success());
  assert_eq!(resp.headers().get("x-file-rev").unwrap(), &first_rev);
  let body = resp.bytes().await.unwrap();
  assert_eq!(&body[..], b"hello, hub");

  // A push with a *bogus* base_rev (not in the revision tree at all) is
  // rejected - there is nothing to branch from. (A real-but-stale rev is
  // handled by branching + resolving, tested below.)
  let stale_push = json!([{
      "path": "notes/hello.txt",
      "deleted": false,
      "base_rev": "1-doesnotexist",
      "mtime": 1_700_000_100,
      "content_type": "text/plain",
      "content_base64": STANDARD.encode(b"stale write"),
  }]);
  let resp = http
    .post(format!("{hub_url}/changes"))
    .bearer_auth(DEVICE_TOKEN)
    .json(&stale_push)
    .send()
    .await
    .unwrap();
  let results: Vec<PushResult> = resp.json().await.unwrap();
  assert_eq!(results[0].status, PushStatus::Conflict);
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn deleting_a_file_removes_it_and_surfaces_as_a_tombstone_in_changes() {
  let (hub_url, http, _couch) = start_hub().await;

  let push_body = json!([{
      "path": "to-delete.txt",
      "deleted": false,
      "base_rev": null,
      "mtime": 1,
      "content_type": "text/plain",
      "content_base64": STANDARD.encode(b"temporary"),
  }]);
  let resp = http
    .post(format!("{hub_url}/changes"))
    .bearer_auth(DEVICE_TOKEN)
    .json(&push_body)
    .send()
    .await
    .unwrap();
  let results: Vec<PushResult> = resp.json().await.unwrap();
  let PushStatus::Ok { rev } = &results[0].status else {
    panic!("expected Ok, got {:?}", results[0].status);
  };

  let delete_body = json!([{
      "path": "to-delete.txt",
      "deleted": true,
      "base_rev": rev,
      "mtime": 2,
      "content_type": null,
      "content_base64": null,
  }]);
  let resp = http
    .post(format!("{hub_url}/changes"))
    .bearer_auth(DEVICE_TOKEN)
    .json(&delete_body)
    .send()
    .await
    .unwrap();
  let results: Vec<PushResult> = resp.json().await.unwrap();
  assert!(matches!(results[0].status, PushStatus::Ok { .. }));

  let resp = http
    .get(format!("{hub_url}/file/to-delete.txt"))
    .bearer_auth(DEVICE_TOKEN)
    .send()
    .await
    .unwrap();
  assert_eq!(resp.status(), 404);

  let resp = http
    .get(format!("{hub_url}/changes"))
    .bearer_auth(DEVICE_TOKEN)
    .send()
    .await
    .unwrap();
  let changes: ChangesResponse = resp.json().await.unwrap();
  assert!(changes
    .changes
    .iter()
    .any(|c| c.path == "to-delete.txt" && c.deleted));
}

async fn push_text(
  http: &reqwest::Client,
  hub_url: &str,
  path: &str,
  base_rev: Option<&str>,
  mtime: i64,
  content: &str,
) -> String {
  let body = json!([{
      "path": path,
      "deleted": false,
      "base_rev": base_rev,
      "mtime": mtime,
      "content_type": "text/plain",
      "content_base64": STANDARD.encode(content),
  }]);
  let resp = http
    .post(format!("{hub_url}/changes"))
    .bearer_auth(DEVICE_TOKEN)
    .json(&body)
    .send()
    .await
    .unwrap();
  let results: Vec<PushResult> = resp.json().await.unwrap();
  match &results[0].status {
    PushStatus::Ok { rev } => rev.clone(),
    other => panic!("expected Ok, got {other:?}"),
  }
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn stale_but_real_base_rev_branches_and_resolves() {
  let (hub_url, http, _couch) = start_hub().await;

  let rev1 = push_text(&http, &hub_url, "note.txt", None, 1, "a\nb\nc\nd\n").await;
  let _rev2 = push_text(&http, &hub_url, "note.txt", Some(&rev1), 2, "a\nB1\nc\nd\n").await;

  // This is a *real* but now-stale base_rev (rev1): it must not be
  // rejected. The hub branches it and merges with rev2.
  push_text(&http, &hub_url, "note.txt", Some(&rev1), 3, "a\nb\nc\nD1\n").await;

  let resp = http
    .get(format!("{hub_url}/file/note.txt"))
    .bearer_auth(DEVICE_TOKEN)
    .send()
    .await
    .unwrap();
  let body = resp.bytes().await.unwrap();
  assert_eq!(&body[..], b"a\nB1\nc\nD1\n");
}
