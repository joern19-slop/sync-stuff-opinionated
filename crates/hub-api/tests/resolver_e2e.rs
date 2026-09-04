//! Stage 5/6 e2e: exercise the hub-side conflict resolver against a real
//! CouchDB. Conflicts are manufactured with `_bulk_docs` (`new_edits:false`),
//! the same mechanism replication uses to build a branched revision tree.
//!
//! Run explicitly: `cargo test --test resolver_e2e -- --ignored --nocapture`

use base64::{engine::general_purpose::STANDARD, Engine as _};
use hub_api::resolver::{self, Outcome, ResolutionKind};
use sync_core::CouchClient;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const USER: &str = "hub";
const PASS: &str = "hub-password";
const DB: &str = "filesync";

async fn start_couch() -> (
    String,
    CouchClient,
    reqwest::Client,
    ContainerAsync<GenericImage>,
) {
    let _ = tracing_subscriber::fmt().try_init();

    let couch = GenericImage::new("couchdb", "3.3")
        .with_wait_for(WaitFor::message_on_stderr("Apache CouchDB has started"))
        .with_exposed_port(ContainerPort::Tcp(5984))
        .with_env_var("COUCHDB_USER", USER)
        .with_env_var("COUCHDB_PASSWORD", PASS)
        .start()
        .await
        .expect("start couchdb");
    let port = couch.get_host_port_ipv4(5984).await.expect("couch port");
    let url = format!("http://127.0.0.1:{port}");

    let client = CouchClient::new(&url, DB, USER, PASS);
    client.ensure_db().await.expect("ensure db");

    let http = reqwest::Client::new();
    (url, client, http, couch)
}

/// Writes a single revision directly into the revision tree (the same shape
/// replication builds). `rev` is the full "N-hash" rev; `parents` are the
/// ancestor *hashes* (immediate parent first). Content is attached inline.
#[allow(clippy::too_many_arguments)]
async fn put_rev(
    http: &reqwest::Client,
    url: &str,
    id: &str,
    rev: &str,
    parents: &[&str],
    deleted: bool,
    mtime: i64,
    content: Option<&[u8]>,
) {
    let hash = rev.split('-').nth(1).unwrap();
    let mut ids = vec![hash.to_string()];
    ids.extend(parents.iter().map(|s| s.to_string()));

    let mut doc = serde_json::json!({
        "_id": id,
        "_rev": rev,
        "_revisions": { "start": ids.len(), "ids": ids },
        "path": id,
        "mtime": mtime,
        "content_type": "text/plain",
    });
    if deleted {
        doc["_deleted"] = true.into();
    }
    if let Some(c) = content {
        doc["_attachments"] = serde_json::json!({
            "content": { "content_type": "text/plain", "data": STANDARD.encode(c) }
        });
    }

    let resp = http
        .post(format!("{url}/{DB}/_bulk_docs"))
        .basic_auth(USER, Some(PASS))
        .json(&serde_json::json!({ "new_edits": false, "docs": [doc] }))
        .send()
        .await
        .expect("bulk_docs");
    assert!(
        resp.status().is_success(),
        "bulk_docs failed: {}",
        resp.status()
    );
}

async fn conflicts_of(client: &CouchClient, path: &str) -> Vec<String> {
    let doc = client.get_doc(path).await.unwrap().expect("doc exists");
    doc.get("_conflicts")
        .and_then(|c| c.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

async fn content_of(client: &CouchClient, path: &str) -> Vec<u8> {
    client
        .get_attachment(path, "content")
        .await
        .unwrap()
        .to_vec()
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn edit_vs_edit_clean_merge_resolves_without_copy() {
    let (url, client, http, _couch) = start_couch().await;

    // base: "a b c d", A edits "b", B edits "d" (non-overlapping).
    put_rev(
        &http,
        &url,
        "note.txt",
        "1-11111111",
        &[],
        false,
        1,
        Some(b"a\nb\nc\nd\n"),
    )
    .await;
    put_rev(
        &http,
        &url,
        "note.txt",
        "2-22222222",
        &["11111111"],
        false,
        2,
        Some(b"a\nb1\nc\nd\n"),
    )
    .await;
    put_rev(
        &http,
        &url,
        "note.txt",
        "2-33333333",
        &["11111111"],
        false,
        3,
        Some(b"a\nb\nc\nd1\n"),
    )
    .await;

    assert_eq!(conflicts_of(&client, "note.txt").await.len(), 1);

    let outcome = resolver::resolve(&client, "note.txt").await.unwrap();
    let Outcome::Resolved(resolved) = outcome else {
        panic!("expected Resolved, got {outcome:?}");
    };
    assert_eq!(resolved.kind, ResolutionKind::Merged);
    assert!(resolved.conflict_copies.is_empty());

    // Both edits survive, conflict flag cleared.
    assert_eq!(content_of(&client, "note.txt").await, b"a\nb1\nc\nd1\n");
    assert!(conflicts_of(&client, "note.txt").await.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn edit_vs_edit_overlapping_edit_keeps_newer_and_copies_older() {
    let (url, client, http, _couch) = start_couch().await;

    // Both A and B edit line "b" differently -> merge conflict.
    put_rev(
        &http,
        &url,
        "note.txt",
        "1-11111111",
        &[],
        false,
        1,
        Some(b"a\nb\nc\n"),
    )
    .await;
    put_rev(
        &http,
        &url,
        "note.txt",
        "2-22222222",
        &["11111111"],
        false,
        2,
        Some(b"a\nB1\nc\n"),
    )
    .await;
    put_rev(
        &http,
        &url,
        "note.txt",
        "2-33333333",
        &["11111111"],
        false,
        3,
        Some(b"a\nB2\nc\n"),
    )
    .await;

    let outcome = resolver::resolve(&client, "note.txt").await.unwrap();
    let Outcome::Resolved(resolved) = outcome else {
        panic!("expected Resolved, got {outcome:?}");
    };
    assert_eq!(resolved.kind, ResolutionKind::KeptNewer);

    // Newer (mtime 3 -> "B2") becomes active; older ("B1") preserved.
    assert_eq!(content_of(&client, "note.txt").await, b"a\nB2\nc\n");
    assert_eq!(resolved.conflict_copies.len(), 1);
    let copy = &resolved.conflict_copies[0];
    assert!(copy.starts_with("note.txt.conflict-"));
    assert_eq!(content_of(&client, copy).await, b"a\nB1\nc\n");

    assert!(conflicts_of(&client, "note.txt").await.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn edit_vs_delete_edit_survives() {
    let (url, client, http, _couch) = start_couch().await;

    put_rev(
        &http,
        &url,
        "note.txt",
        "1-11111111",
        &[],
        false,
        1,
        Some(b"a\nb\nc\n"),
    )
    .await;
    put_rev(
        &http,
        &url,
        "note.txt",
        "2-22222222",
        &["11111111"],
        false,
        2,
        Some(b"a\nedited\nc\n"),
    )
    .await;
    put_rev(
        &http,
        &url,
        "note.txt",
        "2-33333333",
        &["11111111"],
        true,
        3,
        None,
    )
    .await;

    // CouchDB prefers the non-deleted leaf at equal generation, so the edit
    // is already the winner. Resolving must not resurrect a delete or lose
    // the edit - the file simply stays alive with the edited content.
    let outcome = resolver::resolve(&client, "note.txt").await.unwrap();
    assert!(matches!(
        outcome,
        Outcome::NoConflict | Outcome::Resolved(_)
    ));

    assert_eq!(content_of(&client, "note.txt").await, b"a\nedited\nc\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn unconflicted_doc_is_a_noop() {
    let (url, client, http, _couch) = start_couch().await;

    put_rev(
        &http,
        &url,
        "plain.txt",
        "1-11111111",
        &[],
        false,
        1,
        Some(b"hello\n"),
    )
    .await;

    let outcome = resolver::resolve(&client, "plain.txt").await.unwrap();
    assert_eq!(outcome, Outcome::NoConflict);
}
