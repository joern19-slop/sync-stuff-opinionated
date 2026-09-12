use crate::couch::CouchClient;
use std::collections::HashSet;

pub struct AppState {
  pub couch: CouchClient,
  pub device_tokens: HashSet<String>,
}
