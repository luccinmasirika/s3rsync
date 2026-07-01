use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Global token-bucket bandwidth limiter: acquire `n` byte-tokens before
/// uploading, refilling at `bps`. Object-granularity, caps average throughput.
pub struct RateLimiter {
    bps: f64,
    state: Mutex<(f64, Instant)>, // (available tokens, last refill)
}

impl RateLimiter {
    /// `mib_per_sec == 0` → unlimited (returns None, callers skip acquire).
    pub fn new(mib_per_sec: u64) -> Option<Self> {
        if mib_per_sec == 0 {
            return None;
        }
        let bps = mib_per_sec as f64 * 1_048_576.0;
        Some(Self {
            bps,
            state: Mutex::new((bps, Instant::now())),
        })
    }

    pub async fn acquire(&self, n: u64) {
        let n = n as f64;
        loop {
            let wait = {
                let mut s = self.state.lock().unwrap();
                let now = Instant::now();
                let elapsed = now.duration_since(s.1).as_secs_f64();
                s.1 = now;
                // Cap at ~1s burst, but never below n or a >bps object never fits.
                let cap = self.bps.max(n);
                s.0 = (s.0 + elapsed * self.bps).min(cap);
                if s.0 >= n {
                    s.0 -= n;
                    None
                } else {
                    Some((n - s.0) / self.bps)
                }
            };
            match wait {
                None => return,
                Some(secs) => tokio::time::sleep(Duration::from_secs_f64(secs)).await,
            }
        }
    }
}
