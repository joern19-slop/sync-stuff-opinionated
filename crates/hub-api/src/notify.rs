//! Outbound notifications for the hub: FCM wakeups to devices and Discord
//! webhook alerts for hub-level problems.
//!
//! Both are intentionally thin HTTP clients so they can be exercised against
//! a `wiremock` server in tests without a real FCM project or Discord webhook.

use reqwest::Client;
use serde_json::json;
use thiserror::Error;

/// Legacy FCM endpoint. Deprecated upstream in favour of the HTTP v1 API
/// (which needs OAuth2 service-account token exchange); for personal-use
/// scale the legacy server-key endpoint is the pragmatic choice and is the
/// one place to swap when migrating. Nothing else in the hub references FCM.
const FCM_LEGACY_ENDPOINT: &str = "https://fcm.googleapis.com/fcm/send";

#[derive(Debug, Error)]
pub enum NotifyError {
    #[error("http transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("notification service returned {status}: {body}")]
    Api { status: u16, body: String },
}

/// Sends silent (data-only) FCM wakeup messages. "Empty body" in the
/// architecture plan means "no user-visible notification payload" - FCM
/// still requires *something*, so we send a `data`-only message with
/// `content_available` (iOS) which wakes the app without alerting the user.
#[derive(Debug, Clone)]
pub struct FcmClient {
    http: Client,
    server_key: String,
    endpoint: String,
}

impl FcmClient {
    pub fn new(server_key: impl Into<String>) -> Self {
        Self {
            http: Client::new(),
            server_key: server_key.into(),
            endpoint: FCM_LEGACY_ENDPOINT.to_string(),
        }
    }

    /// Override the endpoint (test seam for `wiremock`).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    pub async fn send_wakeup(&self, tokens: &[String]) -> Result<(), NotifyError> {
        if tokens.is_empty() {
            return Ok(());
        }

        let body = json!({
            "registration_ids": tokens,
            "priority": "high",
            "content_available": true,
            "data": { "kind": "wake" },
        });

        let resp = self
            .http
            .post(&self.endpoint)
            .header("Authorization", format!("key={}", self.server_key))
            .json(&body)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(NotifyError::Api {
                status: resp.status().as_u16(),
                body: resp.text().await.unwrap_or_default(),
            })
        }
    }
}

/// Posts a text message to a Discord webhook URL.
#[derive(Debug, Clone)]
pub struct DiscordClient {
    http: Client,
    webhook_url: String,
}

impl DiscordClient {
    pub fn new(webhook_url: impl Into<String>) -> Self {
        Self {
            http: Client::new(),
            webhook_url: webhook_url.into(),
        }
    }

    pub async fn send(&self, content: &str) -> Result<(), NotifyError> {
        let resp = self
            .http
            .post(&self.webhook_url)
            .json(&json!({ "content": content }))
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(NotifyError::Api {
                status: resp.status().as_u16(),
                body: resp.text().await.unwrap_or_default(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn fcm_sends_authorized_wakeup_to_all_tokens() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/fcm/send"))
            .and(header("Authorization", "key=test-key"))
            .and(body_json(json!({
                "registration_ids": ["tok-a", "tok-b"],
                "priority": "high",
                "content_available": true,
                "data": { "kind": "wake" },
            })))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let fcm = FcmClient::new("test-key").with_endpoint(format!("{}/fcm/send", server.uri()));
        fcm.send_wakeup(&["tok-a".into(), "tok-b".into()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fcm_noops_on_empty_token_list_without_http() {
        let fcm = FcmClient::new("test-key").with_endpoint("http://127.0.0.1:0/fcm/send");
        fcm.send_wakeup(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn discord_posts_content_to_webhook() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hooks/test"))
            .and(body_json(json!({ "content": "hub alert!" })))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let discord = DiscordClient::new(format!("{}/hooks/test", server.uri()));
        discord.send("hub alert!").await.unwrap();
    }

    #[tokio::test]
    async fn non_success_status_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hooks/test"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let discord = DiscordClient::new(format!("{}/hooks/test", server.uri()));
        let err = discord.send("x").await.unwrap_err();
        assert!(matches!(err, NotifyError::Api { status: 500, .. }));
    }
}
