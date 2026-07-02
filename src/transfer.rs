use crate::config::Config;
use crate::lister::ObjectMeta;
use crate::multipart::multipart_copy;
use crate::rate::RateLimiter;
use crate::retry::{classify, Disp};
use crate::stats::{Kind, Stats};
use crate::throttle::Throttle;
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use base64::Engine;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

// A single-part ETag is a 32-hex MD5; turn it into a Content-MD5 header.
// Multipart ETags ("md5-N") aren't a plain MD5, so return None and let size
// checks cover integrity instead.
fn etag_to_content_md5(etag: &str) -> Option<String> {
    let s = etag.trim_matches('"');
    if s.len() != 32 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Small-object path: streaming GET→PUT (high-concurrency pool, AIMD-gated).
#[allow(clippy::too_many_arguments)]
pub async fn do_simple(
    o: ObjectMeta,
    src: &Client,
    dst: &Client,
    cfg: &Config,
    stats: &Arc<Stats>,
    throttle: &Arc<Throttle>,
    limiter: &Option<Arc<RateLimiter>>,
    wid: usize,
) -> bool {
    let _permit = throttle.acquire().await; // effective concurrency = adaptive cap
    if let Some(l) = limiter {
        l.acquire(o.size).await;
    }
    let res = simple_copy(src, dst, &o, cfg, stats, wid).await;
    if let Err(e) = &res {
        if classify(e) == Disp::RateLimited {
            throttle.penalize();
        }
    }
    let ok = res.is_ok();
    record(res, &o, stats);
    stats.workers[wid].idle();
    ok
}

/// Big-object path: multipart with ranged GETs (low-concurrency pool).
pub async fn do_multipart(
    o: ObjectMeta,
    src: &Client,
    dst: &Client,
    cfg: &Config,
    stats: &Arc<Stats>,
    limiter: &Option<Arc<RateLimiter>>,
    wid: usize,
) -> bool {
    if let Some(l) = limiter {
        l.acquire(o.size).await;
    }
    let res = multipart_copy(src, dst, &o, cfg, stats, wid).await;
    let ok = res.is_ok();
    record(res, &o, stats);
    stats.workers[wid].idle();
    ok
}

fn record(res: Result<()>, o: &ObjectMeta, stats: &Arc<Stats>) {
    match res {
        Ok(()) => {
            stats.complete(o.size);
            stats.push_feed(Kind::Ok, o.key.clone(), format!("{}K", o.size / 1024));
        }
        Err(e) => {
            stats.fail();
            if classify(&e) == Disp::TokenExpired {
                stats.token_expired.store(true, Relaxed);
            }
            let short = format!("{e:#}");
            let short = short.lines().next().unwrap_or("failed").to_string();
            stats.push_feed(Kind::ErrUp, o.key.clone(), short);
        }
    }
}

/// Streaming single-object copy: GET body piped straight into PUT (no full buffer).
async fn simple_copy(
    src: &Client,
    dst: &Client,
    o: &ObjectMeta,
    cfg: &Config,
    stats: &Arc<Stats>,
    wid: usize,
) -> Result<()> {
    let (dst_bucket, _) = cfg.dst_parts();
    let dst_key = cfg.dst_key(&o.key);
    let slot = &stats.workers[wid];

    slot.begin_dl(&o.key, o.size);
    crate::retry::run(cfg.max_retries, || async {
        let got = src
            .get_object()
            .bucket(&cfg.src_bucket)
            .key(&o.key)
            .send()
            .await
            .context("get_object")?;
        // carry over content headers and user metadata from the source
        let ct = got.content_type().map(String::from);
        let ce = got.content_encoding().map(String::from);
        let cc = got.cache_control().map(String::from);
        let cd = got.content_disposition().map(String::from);
        let meta = got.metadata().cloned();
        let etag = got.e_tag().map(String::from);
        let body = got.body;
        slot.begin_up();
        let mut put = dst
            .put_object()
            .bucket(&dst_bucket)
            .key(&dst_key)
            .content_length(o.size as i64)
            .set_content_type(ct)
            .set_content_encoding(ce)
            .set_cache_control(cc)
            .set_content_disposition(cd)
            .set_metadata(meta);
        if cfg.checksum {
            if let Some(md5) = etag.as_deref().and_then(etag_to_content_md5) {
                put = put.content_md5(md5); // MinIO validates it, end-to-end integrity
            }
        }
        put.body(body).send().await.context("put_object")?;
        Ok(())
    })
    .await?;

    if cfg.verify {
        verify_dest(dst, &dst_bucket, &dst_key, o.size).await?;
    }
    stats.add_dl(o.size);
    stats.add_up(o.size);
    Ok(())
}

/// Confirm the dest object exists with the expected size (post-copy integrity).
pub async fn verify_dest(dst: &Client, bucket: &str, key: &str, expected: u64) -> Result<()> {
    let h = dst
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("verify head_object")?;
    let got = h.content_length().unwrap_or(0).max(0) as u64;
    if got != expected {
        anyhow::bail!("verify failed: dest size {got} != source {expected}");
    }
    Ok(())
}
