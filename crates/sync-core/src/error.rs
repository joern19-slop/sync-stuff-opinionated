use thiserror::Error;

#[derive(Debug, Error)]
pub enum CouchError {
  #[error("http transport error: {0}")]
  Transport(#[from] reqwest::Error),

  #[error("couchdb returned {status}: {body}")]
  Api { status: u16, body: String },

  #[error("invalid url: {0}")]
  BadUrl(String),

  #[error("revision conflict writing {0}")]
  RevConflict(String),

  #[error("unexpected response shape: {0}")]
  Decode(String),
}
