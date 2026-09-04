use std::collections::HashSet;
use crate::couch::CouchClient;

pub struct AppState {
  pub couch: CouchClient,
  pub device_tokens: HashSet<String>,
}
