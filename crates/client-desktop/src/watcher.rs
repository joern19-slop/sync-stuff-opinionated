//! inotify watcher: recursively watches the sync directory and signals a
//! (non-blocking) "something changed" on any filesystem event.
//!
//! It doesn't try to classify events - the caller re-scans the tree on each
//! signal, and that scan is idempotent (it compares mtimes against sync
//! metadata). It *does* keep watches for new subdirectories current, because
//! inotify only reports events inside a directory it's explicitly watching.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};
use tokio::sync::mpsc::UnboundedSender;

const WATCH_MASK: WatchMask = WatchMask::CREATE
  .union(WatchMask::DELETE)
  .union(WatchMask::MODIFY)
  .union(WatchMask::CLOSE_WRITE)
  .union(WatchMask::MOVED_FROM)
  .union(WatchMask::MOVED_TO)
  .union(WatchMask::ATTRIB);

/// How long to wait before retrying a failed watch.
const RETRY_INTERVAL: Duration = Duration::from_secs(300);

/// Spawns a background thread that watches `root` recursively and sends `()`
/// on `changed` whenever anything under it changes.
pub fn spawn(root: PathBuf, changed: UnboundedSender<()>) -> std::io::Result<()> {
  std::thread::Builder::new()
    .name("inotify".to_string())
    .spawn(move || run(root, changed))?;
  Ok(())
}

/// Outer loop: (re)establish the watch, retrying on failure. A failed watch
/// is logged; a *repeated* failure also notifies the user, then it sleeps
/// `RETRY_INTERVAL` and tries again.
fn run(root: PathBuf, changed: UnboundedSender<()>) {
  let mut failures = 0u32;
  loop {
    if let Err(e) = watch(root.clone(), &changed) {
      failures += 1;
      tracing::error!(error = %e, "inotify watch failed");
      if failures > 1 {
        crate::notifier::show_notification("filesync", "File watcher failed; retrying");
      }
      std::thread::sleep(RETRY_INTERVAL);
    }
  }
}

fn watch(root: PathBuf, changed: &UnboundedSender<()>) -> std::io::Result<()> {
  let mut inotify = Inotify::init()?;

  let mut dirs: HashMap<WatchDescriptor, PathBuf> = HashMap::new();
  watch_tree(&mut inotify, &mut dirs, &root);

  let mut buf = [0u8; 4096];
  loop {
    let events = inotify.read_events_blocking(&mut buf)?;

    for event in events {
      if event.mask.contains(EventMask::IGNORED) {
        continue;
      }

      let Some(dir) = dirs.get(&event.wd).cloned() else {
        continue;
      };

      let Some(name) = event.name else {
        // Event on the watched directory itself (e.g. it was removed).
        let _ = changed.send(());
        continue;
      };

      let full = dir.join(name);
      let is_dir = event.mask.contains(EventMask::ISDIR);

      if is_dir
        && (event.mask.contains(EventMask::CREATE) || event.mask.contains(EventMask::MOVED_TO))
      {
        // A new subdirectory appeared: watch it (and its children)
        // so future changes inside it are seen.
        watch_tree(&mut inotify, &mut dirs, &full);
      }

      let _ = changed.send(());
    }
  }
}

fn watch_tree(inotify: &mut Inotify, dirs: &mut HashMap<WatchDescriptor, PathBuf>, root: &Path) {
  if let Ok(wd) = inotify.watches().add(root, WATCH_MASK) {
    dirs.insert(wd, root.to_path_buf());
  }
  if let Ok(entries) = std::fs::read_dir(root) {
    for entry in entries.flatten() {
      let path = entry.path();
      if path.is_dir() {
        watch_tree(inotify, dirs, &path);
      }
    }
  }
}
