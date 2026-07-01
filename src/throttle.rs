use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// AIMD concurrency limiter over the small-object pool: shrink on server
/// throttling (SlowDown / broken pipe), grow after a calm window.
pub struct Throttle {
    sem: Arc<Semaphore>,
    cap: AtomicUsize,
    debt: AtomicUsize, // permits still owed by a pending decrease
    ceiling: usize,
    floor: usize,
    last_penalty_ms: AtomicU64,
    start: Instant,
    adjust: Mutex<()>, // serializes cap adjustments
}

const CALM_MS: u64 = 15_000;

impl Throttle {
    pub fn new(ceiling: usize) -> Arc<Self> {
        let ceiling = ceiling.max(1);
        let floor = (ceiling / 8).max(4).min(ceiling);
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(ceiling)),
            cap: AtomicUsize::new(ceiling),
            debt: AtomicUsize::new(0),
            ceiling,
            floor,
            last_penalty_ms: AtomicU64::new(0),
            start: Instant::now(),
            adjust: Mutex::new(()),
        })
    }

    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        // Pay down debt by forgetting permits as workers release them.
        while self.debt.load(Relaxed) > 0 {
            match self.sem.clone().try_acquire_owned() {
                Ok(p) => {
                    p.forget();
                    self.debt.fetch_sub(1, Relaxed);
                }
                Err(_) => break,
            }
        }
        permit
    }

    #[allow(dead_code)] // exposed for the TUI/reporter (current cap display)
    pub fn cap(&self) -> usize {
        self.cap.load(Relaxed)
    }

    /// Multiplicative decrease (-25%, floored) on sustained server throttling.
    pub fn penalize(&self) {
        let _g = self.adjust.lock().unwrap();
        self.mark();
        let cur = self.cap.load(Relaxed);
        if cur <= self.floor {
            return;
        }
        let target = cur.saturating_sub((cur / 4).max(1)).max(self.floor);
        let mut remove = cur - target;
        while remove > 0 {
            match self.sem.clone().try_acquire_owned() {
                Ok(p) => p.forget(),
                Err(_) => break,
            }
            remove -= 1;
        }
        // Permits in flight can't be forgotten now; owe them to acquire().
        if remove > 0 {
            self.debt.fetch_add(remove, Relaxed);
        }
        self.cap.store(target, Relaxed);
    }

    /// Additive increase (+1) after a calm window with no throttling.
    pub fn recover(&self) {
        let _g = self.adjust.lock().unwrap();
        // Don't grow while a decrease is still settling.
        if self.debt.load(Relaxed) > 0 {
            return;
        }
        let cur = self.cap.load(Relaxed);
        if cur < self.ceiling
            && self
                .elapsed_ms()
                .saturating_sub(self.last_penalty_ms.load(Relaxed))
                > CALM_MS
        {
            self.sem.add_permits(1);
            self.cap.store(cur + 1, Relaxed);
        }
    }

    fn mark(&self) {
        self.last_penalty_ms.store(self.elapsed_ms(), Relaxed);
    }
    fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}
