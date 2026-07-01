use crate::config::Config;
use crate::index::DestIndex;
use crate::lister::ObjectMeta;
use std::collections::HashSet;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Action {
    SkipJournaled,
    SkipSameSize,
    Simple,
    Multipart,
}

/// Decide what to do: journal skip (instant) → size skip → simple/multipart copy.
pub fn plan(o: &ObjectMeta, idx: &DestIndex, done: &HashSet<String>, cfg: &Config) -> Action {
    if done.contains(&o.key) {
        return Action::SkipJournaled;
    }
    if cfg.size_only {
        let dst_key = cfg.dst_key(&o.key);
        if let Some(&dsize) = idx.get(&dst_key) {
            if dsize == o.size {
                return Action::SkipSameSize;
            }
        }
    }
    if o.size >= cfg.multipart_threshold {
        Action::Multipart
    } else {
        Action::Simple
    }
}
