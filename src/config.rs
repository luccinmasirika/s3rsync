use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

/// rsync for S3-compatible object storage: streaming, concurrent, resumable.
#[derive(Parser, Clone, Debug)]
#[command(name = "s3rsync", version)]
pub struct Config {
    /// Source S3 bucket (not needed with --size)
    #[arg(long, default_value = "")]
    pub src_bucket: String,

    /// Optional source key prefix (only migrate keys under it)
    #[arg(long, default_value = "")]
    pub src_prefix: String,

    /// Destination MinIO bucket (optionally bucket/prefix)
    #[arg(long)]
    pub dst_bucket: String,

    /// Concurrent SMALL-object transfers. Push this high; tokio runs hundreds
    /// cheaply where rclone caps at ~24.
    #[arg(long, default_value_t = 128)]
    pub transfers: usize,

    /// Concurrent BIG (multipart) objects, kept low to bound RAM.
    #[arg(long, default_value_t = 4)]
    pub big_transfers: usize,

    /// Concurrent parts per multipart object
    #[arg(long, default_value_t = 2)]
    pub part_concurrency: usize,

    /// Multipart part size (bytes)
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    pub part_size: usize,

    /// Objects >= this size use multipart (bytes)
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub multipart_threshold: u64,

    /// Skip objects already on dest with matching size
    #[arg(long, default_value_t = true)]
    pub size_only: bool,

    /// Simulate: list and plan, but transfer nothing
    #[arg(long)]
    pub dry_run: bool,

    /// Resume journal path (persistent disk)
    #[arg(long, default_value = ".s3rsync-journal")]
    pub journal: PathBuf,

    /// Max retries per request before giving up
    #[arg(long, default_value_t = 5)]
    pub max_retries: u32,

    /// Source AWS region
    #[arg(long, env = "AWS_REGION", default_value = "eu-central-1")]
    pub src_region: String,

    /// S3-compatible source endpoint (e.g. MinIO). Omit → real AWS via env creds.
    #[arg(long)]
    pub src_endpoint: Option<String>,

    /// Source access key (with --src-endpoint; else falls back to AWS_ACCESS_KEY_ID)
    #[arg(long)]
    pub src_access_key: Option<String>,

    /// Source secret key (with --src-endpoint; else falls back to AWS_SECRET_ACCESS_KEY)
    #[arg(long)]
    pub src_secret_key: Option<String>,

    /// Only copy keys matching this glob (repeatable). Empty = all.
    #[arg(long)]
    pub include: Vec<String>,

    /// Skip keys matching this glob (repeatable).
    #[arg(long)]
    pub exclude: Vec<String>,

    /// Verify each object's size on the dest after copy (extra HEAD per object).
    #[arg(long)]
    pub verify: bool,

    /// SYNC mode: after copying, DELETE dest objects absent from the source.
    /// Destructive, use with care.
    #[arg(long)]
    pub delete: bool,

    /// Cap total upload bandwidth (MiB/s, 0 = unlimited).
    #[arg(long, default_value_t = 0)]
    pub bwlimit: u64,

    /// Report the dest (MinIO) bucket size (object count + total bytes) and exit.
    #[arg(long)]
    pub size: bool,

    /// End-to-end integrity: send the source MD5 as Content-MD5 so MinIO rejects
    /// any corrupted single-part upload (multipart objects fall back to size).
    #[arg(long)]
    pub checksum: bool,
}

/// Compiled include/exclude globs, matched against the (prefix-relative) key.
pub struct Filter {
    include: Vec<glob::Pattern>,
    exclude: Vec<glob::Pattern>,
}

impl Filter {
    pub fn build(cfg: &Config) -> anyhow::Result<Self> {
        let compile = |v: &[String]| -> anyhow::Result<Vec<glob::Pattern>> {
            v.iter()
                .map(|p| glob::Pattern::new(p).map_err(anyhow::Error::from))
                .collect()
        };
        Ok(Self {
            include: compile(&cfg.include)?,
            exclude: compile(&cfg.exclude)?,
        })
    }
    pub fn accepts(&self, key: &str) -> bool {
        if !self.include.is_empty() && !self.include.iter().any(|p| p.matches(key)) {
            return false;
        }
        !self.exclude.iter().any(|p| p.matches(key))
    }
}

/// MinIO destination credentials, read from the environment / .env.
pub struct DestCreds {
    pub url: String,
    pub access: String,
    pub secret: String,
}

impl Config {
    /// Split `dst_bucket` (`bucket` or `bucket/prefix`) into (bucket, prefix).
    pub fn dst_parts(&self) -> (String, String) {
        let trimmed = self.dst_bucket.trim_end_matches('/');
        match trimmed.split_once('/') {
            Some((b, p)) => (b.to_string(), p.trim_matches('/').to_string()),
            None => (trimmed.to_string(), String::new()),
        }
    }

    /// Destination key for a source key, preserving structure: strip the
    /// source prefix, then re-root under the destination prefix.
    pub fn dst_key(&self, src_key: &str) -> String {
        let (_, dst_prefix) = self.dst_parts();
        let rel = if !self.src_prefix.is_empty() {
            src_key
                .strip_prefix(&format!("{}/", self.src_prefix.trim_matches('/')))
                .unwrap_or(src_key)
        } else {
            src_key
        };
        if dst_prefix.is_empty() {
            rel.to_string()
        } else {
            format!("{}/{}", dst_prefix, rel)
        }
    }

    /// Total worker slots across both pools (for stats sizing).
    pub fn total_workers(&self) -> usize {
        self.transfers + self.big_transfers
    }

    /// Peak in-flight body memory: only the multipart (big) pool holds part
    /// buffers; small transfers stream through ~a TLS record each.
    pub fn peak_body_bytes(&self) -> u64 {
        (self.big_transfers as u64)
            .saturating_mul(self.part_concurrency as u64)
            .saturating_mul(self.part_size as u64)
    }

    /// Reject configs that would blow the RAM budget on a small VM.
    pub fn validate_memory_budget(&self) -> Result<()> {
        if self.transfers == 0 || self.big_transfers == 0 {
            anyhow::bail!("--transfers and --big-transfers must be >= 1");
        }
        if self.part_size == 0 || self.part_concurrency == 0 {
            anyhow::bail!("--part-size and --part-concurrency must be >= 1");
        }
        const LIMIT: u64 = 1536 * 1024 * 1024; // 1.5 GiB
        let peak = self.peak_body_bytes();
        if peak > LIMIT {
            anyhow::bail!(
                "peak in-flight body memory {} MiB exceeds limit {} MiB, lower --transfers/--part-size",
                peak / 1024 / 1024,
                LIMIT / 1024 / 1024
            );
        }
        Ok(())
    }

    pub fn dest_creds() -> Result<DestCreds> {
        let _ = dotenvy::dotenv();
        Ok(DestCreds {
            url: std::env::var("MINIO_REMOTE_URL")
                .context("MINIO_REMOTE_URL missing (source your .env)")?,
            access: std::env::var("MINIO_REMOTE_ACCESS_KEY")
                .context("MINIO_REMOTE_ACCESS_KEY missing")?,
            secret: std::env::var("MINIO_REMOTE_SECRET_KEY")
                .context("MINIO_REMOTE_SECRET_KEY missing")?,
        })
    }
}
