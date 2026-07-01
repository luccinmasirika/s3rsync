use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

/// Load completed keys from the append-only journal (one key per line).
pub fn load(path: &Path) -> HashSet<String> {
    let mut set = HashSet::new();
    if let Ok(f) = std::fs::File::open(path) {
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            if !line.is_empty() {
                set.insert(line);
            }
        }
    }
    set
}

/// Spawn a single writer task; workers send completed keys, it batches + fsyncs.
/// Single writer = no interleaved partial lines, crash-safe to the last batch.
pub fn spawn_writer(path: PathBuf) -> UnboundedSender<String> {
    let (tx, mut rx) = unbounded_channel::<String>();
    tokio::spawn(async move {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path);
        let mut f = match f {
            Ok(f) => f,
            Err(e) => {
                eprintln!("journal disabled ({path:?}): {e}");
                // drain so senders don't block
                while rx.recv().await.is_some() {}
                return;
            }
        };
        let mut n = 0u32;
        while let Some(key) = rx.recv().await {
            if key.contains('\n') {
                continue; // keys with newlines: skip journaling (rare; just re-copies)
            }
            let _ = writeln!(f, "{key}");
            n += 1;
            if n % 200 == 0 {
                let _ = f.flush();
                let _ = f.sync_data();
            }
        }
        let _ = f.flush();
        let _ = f.sync_data();
    });
    tx
}
