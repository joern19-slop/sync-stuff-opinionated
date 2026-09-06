use std::collections::HashSet;

use threeway_merge::{merge_strings, MergeOptions};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeError {
  Conflict,
  NotText,
  Internal(String),
}

impl std::fmt::Display for MergeError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      MergeError::Conflict => write!(f, "merge conflict"),
      MergeError::NotText => write!(f, "content is not valid UTF-8 text"),
      MergeError::Internal(msg) => write!(f, "merge engine error: {msg}"),
    }
  }
}

impl std::error::Error for MergeError {}

fn to_text(bytes: &[u8]) -> Result<&str, MergeError> {
  std::str::from_utf8(bytes).map_err(|_| MergeError::NotText)
}

pub fn merge(base: &[u8], mine: &[u8], theirs: &[u8]) -> Result<Vec<u8>, MergeError> {
  let base = to_text(base)?;
  let mine = to_text(mine)?;
  let theirs = to_text(theirs)?;

  let result = merge_strings(base, mine, theirs, &MergeOptions::default())
    .map_err(|e| MergeError::Internal(e.to_string()))?;

  match result.has_conflicts() {
    true => Err(MergeError::Conflict),
    false => Ok(result.content.into_bytes()),
  }
}

/// Reconstructs a full "N-hash" rev string from a `_revisions` entry.
pub fn revision_at(start: u64, ids: &[String], index: usize) -> Option<String> {
  let gen = start.checked_sub(index as u64)?;
  Some(format!("{gen}-{}", ids.get(index)?))
}

/// Deepest common ancestor of two `_revisions` histories (newest first;
/// `ids[i]` = generation `start - i`), as a full "N-hash" rev, or `None` if
/// they share no history. Generations match across branches, so one side's
/// `start` suffices to reconstruct the rev.
pub fn common_ancestor(a_ids: &[String], b_start: u64, b_ids: &[String]) -> Option<String> {
  let a_set: HashSet<&str> = a_ids.iter().map(String::as_str).collect();
  for (i, id) in b_ids.iter().enumerate() {
    if a_set.contains(id.as_str()) {
      return revision_at(b_start, b_ids, i);
    }
  }
  None
}

#[cfg(test)]
mod tests {
  use super::*;

  fn s(v: &str) -> Vec<u8> {
    v.as_bytes().to_vec()
  }

  #[test]
  fn independent_edits_merge() {
    let base = s("a\nb\nc\nd\n");
    let mine = s("a\nb1\nc\nd\n");
    let theirs = s("a\nb\nc\nd1\n");
    assert_eq!(merge(&base, &mine, &theirs).unwrap(), s("a\nb1\nc\nd1\n"));
  }

  #[test]
  fn one_sided_edit_merges() {
    let base = s("a\nb\nc\n");
    let mine = s("a\nb\nc\n");
    let theirs = s("a\nB\nc\n");
    assert_eq!(merge(&base, &mine, &theirs).unwrap(), s("a\nB\nc\n"));
  }

  #[test]
  fn identical_edit_on_both_sides_is_not_a_conflict() {
    let base = s("a\nb\nc\n");
    let mine = s("a\nB\nc\n");
    let theirs = s("a\nB\nc\n");
    assert_eq!(merge(&base, &mine, &theirs).unwrap(), s("a\nB\nc\n"));
  }

  #[test]
  fn overlapping_conflicting_edits_error() {
    let base = s("a\nb\nc\n");
    let mine = s("a\nB1\nc\n");
    let theirs = s("a\nB2\nc\n");
    assert_eq!(
      merge(&base, &mine, &theirs).unwrap_err(),
      MergeError::Conflict
    );
  }

  #[test]
  fn edit_vs_delete_in_same_region_conflicts() {
    let base = s("a\nb\nc\n");
    let mine = s("a\nc\n"); // deleted b
    let theirs = s("a\nB\nc\n"); // edited b
    assert_eq!(
      merge(&base, &mine, &theirs).unwrap_err(),
      MergeError::Conflict
    );
  }

  #[test]
  fn insertions_at_same_spot_with_different_content_conflict() {
    let base = s("a\nc\n");
    let mine = s("a\nX\nc\n");
    let theirs = s("a\nY\nc\n");
    assert_eq!(
      merge(&base, &mine, &theirs).unwrap_err(),
      MergeError::Conflict
    );
  }

  #[test]
  fn insertions_at_same_spot_with_same_content_merge() {
    let base = s("a\nc\n");
    let mine = s("a\nX\nc\n");
    let theirs = s("a\nX\nc\n");
    assert_eq!(merge(&base, &mine, &theirs).unwrap(), s("a\nX\nc\n"));
  }

  #[test]
  fn trailing_newline_is_preserved() {
    let base = s("a\nb\n");
    let mine = s("a\nB\n");
    let theirs = s("a\nb\n");
    assert_eq!(merge(&base, &mine, &theirs).unwrap(), s("a\nB\n"));
  }

  #[test]
  fn no_trailing_newline_is_preserved() {
    let base = s("a\nb");
    let mine = s("a\nb");
    let theirs = s("a\nB");
    assert_eq!(merge(&base, &mine, &theirs).unwrap(), s("a\nB"));
  }

  #[test]
  fn non_utf8_is_not_text() {
    assert_eq!(
      merge(b"\xff\xfe", b"a", b"b").unwrap_err(),
      MergeError::NotText
    );
  }

  #[test]
  fn adjacent_line_edits_conflict_matching_git() {
    // Git's xdiff (and thus threeway_merge) treats edits to *adjacent*
    // lines as a conflict - there is no unchanged line separating the two
    // hunks. This is conservative (never loses data), just stricter than
    // a naive diff3.
    let base = s("a\nb\nc\n");
    let mine = s("a\nB1\nc\n");
    let theirs = s("a\nb\nC1\n");
    assert_eq!(
      merge(&base, &mine, &theirs).unwrap_err(),
      MergeError::Conflict
    );
  }

  #[test]
  fn common_ancestor_finds_shared_revision() {
    // a: gen2(shared "bbbb") -> gen1; b: gen3 -> gen2(shared "bbbb") -> gen1
    let a_ids = vec!["aaaa".to_string(), "bbbb".to_string()];
    let b_ids = vec!["cccc".to_string(), "bbbb".to_string()];
    // b.ids[1] == "bbbb" is shared, generation 2 (start 3 - index 1)
    assert_eq!(
      common_ancestor(&a_ids, 3, &b_ids),
      Some("2-bbbb".to_string())
    );
  }

  #[test]
  fn common_ancestor_none_when_no_shared_history() {
    let a_ids = vec!["aaaa".to_string()];
    let b_ids = vec!["bbbb".to_string()];
    assert_eq!(common_ancestor(&a_ids, 1, &b_ids), None);
  }
}
