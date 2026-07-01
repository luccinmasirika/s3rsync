use anyhow::Result;
use std::future::Future;
use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Disp {
    Retry,
    RateLimited,
    Fatal,
    TokenExpired,
}

/// Classify an error by its (chained) message. String-based so it works
/// uniformly across GetObject / PutObject / UploadPart error types.
pub fn classify(err: &anyhow::Error) -> Disp {
    let s = format!("{err:#}").to_lowercase();
    if s.contains("expiredtoken")
        || s.contains("token has expired")
        || s.contains("token expired")
        || s.contains("invalidtoken")
        || s.contains("requestexpired")
    {
        Disp::TokenExpired
    } else if s.contains("slowdown")
        || s.contains("reduce your request rate")
        || s.contains("unwritable")
        || s.contains("throttl")
        || s.contains(" 503")
        || s.contains("(503")
    {
        Disp::RateLimited
    } else if s.contains("accessdenied")
        || s.contains("access denied")
        || s.contains("nosuchkey")
        || s.contains("nosuchbucket")
        || s.contains("invalidaccesskeyid")
        || s.contains("signaturedoesnotmatch")
    {
        Disp::Fatal
    } else {
        // broken pipe, connection reset, timeout, dispatch, IO, 5xx, etc.
        Disp::Retry
    }
}

fn jitter(ceil_ms: u64) -> u64 {
    if ceil_ms == 0 {
        return 0;
    }
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    n % ceil_ms
}

/// Run `f` with full-jitter exponential backoff. Fatal / TokenExpired errors
/// return immediately (no point retrying). Returns the last error on exhaustion.
pub async fn run<F, Fut, T>(max: u32, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => match classify(&e) {
                Disp::Fatal | Disp::TokenExpired => return Err(e),
                d => {
                    if attempt >= max {
                        return Err(e);
                    }
                    let base = if d == Disp::RateLimited { 1000 } else { 200 };
                    let ceil = (base << attempt.min(6)).min(20_000);
                    tokio::time::sleep(Duration::from_millis(jitter(ceil).max(base / 4))).await;
                    attempt += 1;
                }
            },
        }
    }
}
