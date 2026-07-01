// Several fields/variants below are the contract for the (upcoming) ratatui TUI
// and aren't all read by the current text reporter yet.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum Phase {
    Start = 0,
    Dl = 1,
    Up = 2,
    Idle = 3,
    Done = 4,
}

pub enum Kind {
    Ok,
    RetryUp,
    ErrUp,
    ErrDl,
    Skip,
}

pub struct FeedLine {
    pub kind: Kind,
    pub path: String,
    pub note: String,
}

/// One live worker slot. A single worker writes its own slot; the TUI reads it.
pub struct WorkerSlot {
    pub phase: AtomicU8,
    pub cur_done: AtomicU64,
    pub cur_total: AtomicU64,
    pub key: Mutex<String>,
}

impl WorkerSlot {
    fn new() -> Self {
        Self {
            phase: AtomicU8::new(Phase::Start as u8),
            cur_done: AtomicU64::new(0),
            cur_total: AtomicU64::new(0),
            key: Mutex::new(String::new()),
        }
    }
    pub fn begin_dl(&self, key: &str, total: u64) {
        *self.key.lock().unwrap() = key.to_owned();
        self.cur_total.store(total, Relaxed);
        self.cur_done.store(0, Relaxed);
        self.phase.store(Phase::Dl as u8, Relaxed);
    }
    pub fn dl_progress(&self, done: u64) {
        self.cur_done.store(done, Relaxed);
    }
    pub fn begin_up(&self) {
        self.phase.store(Phase::Up as u8, Relaxed);
    }
    pub fn idle(&self) {
        self.phase.store(Phase::Idle as u8, Relaxed);
    }
    pub fn phase(&self) -> Phase {
        match self.phase.load(Relaxed) {
            1 => Phase::Dl,
            2 => Phase::Up,
            3 => Phase::Idle,
            4 => Phase::Done,
            _ => Phase::Start,
        }
    }
}

/// Shared metrics: cheap atomics for the hot path, sampled by the reporter/TUI.
pub struct Stats {
    // immutable job context
    pub src: String,
    pub dst: String,
    pub prefix: String,
    pub jobs: usize,
    pub total_files: AtomicU64,
    pub total_bytes: AtomicU64,
    pub skip: AtomicU64,
    pub start: Instant,

    // hot counters
    pub files_done: AtomicU64,
    pub bytes_done: AtomicU64,
    pub failed: AtomicU64,
    pub up_wire: AtomicU64,
    pub dl_wire: AtomicU64,
    pub inflight: AtomicU64,

    pub workers: Vec<WorkerSlot>,
    pub feed: Mutex<VecDeque<FeedLine>>,
    pub done: AtomicBool,
    pub token_expired: AtomicBool,
    pub shutdown: AtomicBool, // Ctrl-C requested → drain + flush, don't kill
    pub list_failed: AtomicBool, // listing errored → run is INCOMPLETE, fail loudly
}

impl Stats {
    pub fn new(src: String, dst: String, prefix: String, jobs: usize) -> Self {
        let mut workers = Vec::with_capacity(jobs);
        for _ in 0..jobs {
            workers.push(WorkerSlot::new());
        }
        Self {
            src,
            dst,
            prefix,
            jobs,
            total_files: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            skip: AtomicU64::new(0),
            start: Instant::now(),
            files_done: AtomicU64::new(0),
            bytes_done: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            up_wire: AtomicU64::new(0),
            dl_wire: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            workers,
            feed: Mutex::new(VecDeque::with_capacity(64)),
            done: AtomicBool::new(false),
            token_expired: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            list_failed: AtomicBool::new(false),
        }
    }

    // writer API (engine side)
    pub fn add_dl(&self, n: u64) {
        self.dl_wire.fetch_add(n, Relaxed);
    }
    pub fn add_up(&self, n: u64) {
        self.up_wire.fetch_add(n, Relaxed);
    }
    pub fn complete(&self, size: u64) {
        self.files_done.fetch_add(1, Relaxed);
        self.bytes_done.fetch_add(size, Relaxed);
    }
    pub fn skipped(&self) {
        self.skip.fetch_add(1, Relaxed);
    }
    pub fn fail(&self) {
        self.failed.fetch_add(1, Relaxed);
    }
    pub fn plan_add(&self, size: u64) {
        self.total_files.fetch_add(1, Relaxed);
        self.total_bytes.fetch_add(size, Relaxed);
    }
    pub fn push_feed(&self, kind: Kind, path: String, note: String) {
        let mut f = self.feed.lock().unwrap();
        if f.len() == 64 {
            f.pop_front();
        }
        f.push_back(FeedLine { kind, path, note });
    }
}
