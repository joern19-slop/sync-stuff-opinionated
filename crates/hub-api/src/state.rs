use std::collections::HashSet;
use sync_core::CouchClient;

pub struct AppState {
    pub couch: CouchClient,
    pub device_tokens: HashSet<String>,
}
