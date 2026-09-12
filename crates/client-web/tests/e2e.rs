//! End-to-end: a real browser client against a real hub + CouchDB.
//!
//! Requires a hub on `http://127.0.0.1:8080` with device token `test-token`
//! (see `scripts/e2e-web.sh`). Gated behind the `e2e` feature so normal
//! `wasm-pack test` runs don't need a hub.
#![cfg(feature = "e2e")]

use base64::{Engine as _, engine::general_purpose::STANDARD};
use client_web::{WebSync, init};
use common::http_client;
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const HUB_URL: &str = "http://127.0.0.1:8080";
const TOKEN: &str = "test-token";
const PATH: &str = "calendars/e2e/local.ics";
const REMOTE_PATH: &str = "calendars/e2e/remote.ics";
const ICS: &[u8] =
  b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:local-1\r\nSUMMARY:local\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
const REMOTE_ICS: &[u8] =
  b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:remote-1\r\nSUMMARY:remote\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

async fn client() -> WebSync {
  init(vec![HUB_URL.to_string()], TOKEN.to_string())
    .await
    .unwrap()
}

/// Injects a file straight into the hub (bypassing the local client), so the
/// client's *pull* path can be tested against content it doesn't already have.
async fn push_to_hub(path: &str, content: &[u8]) {
  let resp = http_client()
    .post(format!("{HUB_URL}/changes"))
    .bearer_auth(TOKEN)
    .json(&serde_json::json!([{
        "path": path,
        "deleted": false,
        "base_rev": null,
        "mtime": 1,
        "content_type": "text/calendar",
        "content_base64": STANDARD.encode(content),
    }]))
    .send()
    .await
    .unwrap();
  assert!(
    resp.status().is_success(),
    "inject failed: {}",
    resp.status()
  );
}

/// Fetches a file directly from the hub (independent of local OPFS), returning
/// its status and bytes.
async fn fetch_from_hub(path: &str) -> (u16, Option<Vec<u8>>) {
  let resp = http_client()
    .get(format!("{HUB_URL}/file/{path}"))
    .bearer_auth(TOKEN)
    .send()
    .await
    .unwrap();
  let status = resp.status().as_u16();
  let body = if status == 200 {
    Some(resp.bytes().await.unwrap().to_vec())
  } else {
    None
  };
  (status, body)
}

#[wasm_bindgen_test]
async fn push_pull_delete_roundtrip_through_the_hub() {
  // Clean up any leftovers from a previous run.
  let a = client().await;
  let _ = a.delete_file(PATH, 1.0).await;
  let _ = a.delete_file(REMOTE_PATH, 1.0).await;
  let _ = a.sync().await;

  // Push: the client writes a file and syncs; the hub must actually have it.
  a.put_file(PATH, 1.0, "text/calendar", ICS).await.unwrap();
  a.sync().await.unwrap();
  let (status, body) = fetch_from_hub(PATH).await;
  assert_eq!(status, 200);
  assert_eq!(body.as_deref(), Some(ICS));

  // Pull: a file injected on the hub appears locally after a sync.
  push_to_hub(REMOTE_PATH, REMOTE_ICS).await;
  a.sync().await.unwrap();
  assert!(
    a.list_files()
      .await
      .unwrap()
      .contains(&REMOTE_PATH.to_string())
  );
  assert_eq!(
    a.read_file(REMOTE_PATH).await.unwrap(),
    Some(REMOTE_ICS.to_vec())
  );

  // Delete: a local delete propagates to the hub.
  a.delete_file(PATH, 2.0).await.unwrap();
  a.sync().await.unwrap();
  let (status, _) = fetch_from_hub(PATH).await;
  assert_eq!(status, 404);
}
