//! Fast tests for the HTTP layer: routing, auth, and response shaping,
//! with CouchDB itself replaced by a `wiremock` server. Unlike
//! `hub_api_e2e.rs` these need no Docker and run under plain `cargo test`.

use std::collections::HashSet;

use hub_api::config::Config;
use serde_json::json;
use sync_core::ChangesResponse;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const DEVICE_TOKEN: &str = "test-token";

async fn start_hub_against(couch: &MockServer) -> (String, reqwest::Client) {
    Mock::given(method("PUT"))
        .and(path("/filesync"))
        .respond_with(ResponseTemplate::new(201))
        .mount(couch)
        .await;

    let mut device_tokens = HashSet::new();
    device_tokens.insert(DEVICE_TOKEN.to_string());
    let cfg = Config {
        bind_addr: "127.0.0.1:0".to_string(),
        couch_url: couch.uri(),
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
    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), reqwest::Client::new())
}

#[tokio::test]
async fn requests_without_a_valid_bearer_token_are_rejected() {
    let couch = MockServer::start().await;
    let (hub_url, http) = start_hub_against(&couch).await;

    let resp = http.get(format!("{hub_url}/changes")).send().await.unwrap();
    assert_eq!(resp.status(), 401);

    let resp = http
        .get(format!("{hub_url}/changes"))
        .bearer_auth("wrong-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn get_changes_maps_couchdb_changes_feed_into_the_api_contract() {
    let couch = MockServer::start().await;
    let (hub_url, http) = start_hub_against(&couch).await;

    Mock::given(method("GET"))
        .and(path("/filesync/_changes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "notes/a.txt", "deleted": false, "changes": [{ "rev": "3-abc" }] },
                { "id": "_design/foo", "deleted": false, "changes": [{ "rev": "1-x" }] },
                { "id": "gone.txt", "deleted": true, "changes": [{ "rev": "2-def" }] }
            ],
            "last_seq": "17-xyz"
        })))
        .mount(&couch)
        .await;

    let resp = http
        .get(format!("{hub_url}/changes"))
        .bearer_auth(DEVICE_TOKEN)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let body: ChangesResponse = resp.json().await.unwrap();
    assert_eq!(body.checkpoint, "17-xyz");
    // The design doc must never surface as a file change.
    assert_eq!(body.changes.len(), 2);
    assert!(body
        .changes
        .iter()
        .any(|c| c.path == "notes/a.txt" && !c.deleted && c.rev == "3-abc"));
    assert!(body
        .changes
        .iter()
        .any(|c| c.path == "gone.txt" && c.deleted));
}

#[tokio::test]
async fn get_changes_longpoll_forwards_feed_and_timeout_and_reshapes() {
    let couch = MockServer::start().await;
    let (hub_url, http) = start_hub_against(&couch).await;

    Mock::given(method("GET"))
        .and(path("/filesync/_changes"))
        .and(query_param("feed", "longpoll"))
        .and(query_param("timeout", "25"))
        .and(query_param("since", "17-xyz"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "a.txt", "deleted": false, "changes": [{ "rev": "4-abc" }] },
                { "id": "_design/foo", "deleted": false, "changes": [{ "rev": "1-x" }] }
            ],
            "last_seq": "18-xyz"
        })))
        .mount(&couch)
        .await;

    let resp = http
        .get(format!("{hub_url}/changes/longpoll?since=17-xyz"))
        .bearer_auth(DEVICE_TOKEN)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let body: ChangesResponse = resp.json().await.unwrap();
    assert_eq!(body.checkpoint, "18-xyz");
    assert_eq!(body.changes.len(), 1);
    assert_eq!(body.changes[0].path, "a.txt");
}

#[tokio::test]
async fn get_file_for_a_missing_path_is_404() {
    let couch = MockServer::start().await;
    let (hub_url, http) = start_hub_against(&couch).await;

    Mock::given(method("GET"))
        .and(path("/filesync/missing.txt"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": "not_found", "reason": "missing"
        })))
        .mount(&couch)
        .await;

    let resp = http
        .get(format!("{hub_url}/file/missing.txt"))
        .bearer_auth(DEVICE_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}
