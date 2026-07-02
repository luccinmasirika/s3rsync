mod clients;
mod config;
mod index;
mod journal;
mod lister;
mod multipart;
mod plan;
mod rate;
mod retry;
mod stats;
mod throttle;
mod transfer;

use anyhow::{Context, Result};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use clap::Parser;
use config::{Config, Filter};
use futures::StreamExt;
use lister::ObjectMeta;
use plan::Action;
use stats::Stats;
use std::collections::HashSet;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::parse();
    cfg.validate_memory_budget()?;
    if !cfg.size {
        eprintln!(
            "small-transfers={} big-transfers={} part_size={}MiB  peak body ~{}MiB",
            cfg.transfers,
            cfg.big_transfers,
            cfg.part_size / 1024 / 1024,
            cfg.peak_body_bytes() / 1024 / 1024
        );
    }

    let dc = Config::dest_creds()?;
    let dst = clients::dest_client(&dc);
    let (dst_bucket, dst_prefix) = cfg.dst_parts();

    // --size: list the MinIO bucket, report count + total bytes, exit.
    if cfg.size {
        eprintln!("listing {dst_bucket} …");
        let idx = index::build(&dst, &dst_bucket, &dst_prefix)
            .await
            .context("list dest")?;
        let total: u64 = idx.values().sum();
        let where_ = if dst_prefix.is_empty() {
            dst_bucket.clone()
        } else {
            format!("{dst_bucket}/{dst_prefix}")
        };
        println!(
            "{where_}: {} objects · {} ({total} bytes)",
            idx.len(),
            humansize::format_size(total, humansize::BINARY),
        );
        return Ok(());
    }

    if cfg.src_bucket.is_empty() {
        anyhow::bail!("--src-bucket is required (or use --size to report dest size)");
    }
    let src = clients::source_client(&cfg).await;
    dst.head_bucket()
        .bucket(&dst_bucket)
        .send()
        .await
        .with_context(|| format!("dest bucket `{dst_bucket}` missing or unreachable"))?;

    eprintln!("listing dest {dst_bucket} …");
    let idx = Arc::new(
        index::build(&dst, &dst_bucket, &dst_prefix)
            .await
            .context("list dest")?,
    );
    eprintln!("dest index: {} objects", idx.len());

    let stats = Arc::new(Stats::new(
        cfg.src_bucket.clone(),
        cfg.dst_bucket.clone(),
        cfg.src_prefix.clone(),
        cfg.total_workers(),
    ));
    let filter = Arc::new(Filter::build(&cfg).context("invalid --include/--exclude glob")?);

    // resume journal: completed keys skip instantly on re-run
    let done = Arc::new(journal::load(&cfg.journal));
    if !done.is_empty() {
        eprintln!("journal: {} keys already done (will skip)", done.len());
    }
    let jtx = journal::spawn_writer(cfg.journal.clone());

    // --delete: remember every source key (dest-key form) to find dest orphans.
    let src_seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let cfg = Arc::new(cfg);
    let reporter = tokio::spawn(reporter(stats.clone()));

    // Ctrl-C: stop dispatching, drain in-flight, flush journal.
    {
        let s = stats.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("\ninterrupted, draining in-flight transfers + flushing journal …");
                s.shutdown.store(true, Relaxed);
            }
        });
    }

    // Two decoupled pools: many small transfers, few big (multipart) ones.
    let (small_tx, small_rx) = async_channel::bounded::<ObjectMeta>(cfg.transfers * 4);
    let (big_tx, big_rx) = async_channel::bounded::<ObjectMeta>(cfg.big_transfers * 4);

    let limiter: Option<Arc<rate::RateLimiter>> = rate::RateLimiter::new(cfg.bwlimit).map(Arc::new);

    // AIMD throttle over the small pool; recovery ticker ramps concurrency back.
    let throttle = throttle::Throttle::new(cfg.transfers);
    {
        let t = throttle.clone();
        let done = stats.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(3));
            loop {
                iv.tick().await;
                t.recover();
                if done.done.load(Relaxed) {
                    break;
                }
            }
        });
    }

    let mut handles = Vec::new();
    for wid in 0..cfg.transfers {
        handles.push(tokio::spawn(consumer(
            small_rx.clone(),
            src.clone(),
            dst.clone(),
            cfg.clone(),
            stats.clone(),
            throttle.clone(),
            limiter.clone(),
            jtx.clone(),
            wid,
            false,
        )));
    }
    for i in 0..cfg.big_transfers {
        let wid = cfg.transfers + i;
        handles.push(tokio::spawn(consumer(
            big_rx.clone(),
            src.clone(),
            dst.clone(),
            cfg.clone(),
            stats.clone(),
            throttle.clone(),
            limiter.clone(),
            jtx.clone(),
            wid,
            true,
        )));
    }

    // Producer: list source, plan, route to the right pool (or skip / count).
    lister::list_source(src.clone(), cfg.src_bucket.clone(), cfg.src_prefix.clone())
        .for_each(|item| {
            let (idx, done, cfg, stats, filter, small_tx, big_tx, src_seen) = (
                idx.clone(),
                done.clone(),
                cfg.clone(),
                stats.clone(),
                filter.clone(),
                small_tx.clone(),
                big_tx.clone(),
                src_seen.clone(),
            );
            async move {
                if stats.shutdown.load(Relaxed) {
                    return; // Ctrl-C: stop dispatching, let in-flight drain
                }
                match item {
                    Ok(o) => {
                        if cfg.delete {
                            src_seen.lock().unwrap().insert(cfg.dst_key(&o.key));
                        }
                        if !filter.accepts(&o.key) {
                            return;
                        }
                        match plan::plan(&o, &idx, &done, &cfg) {
                            Action::SkipJournaled | Action::SkipSameSize => stats.skipped(),
                            Action::Simple => {
                                stats.plan_add(o.size);
                                if !cfg.dry_run {
                                    let _ = small_tx.send(o).await;
                                }
                            }
                            Action::Multipart => {
                                stats.plan_add(o.size);
                                if !cfg.dry_run {
                                    let _ = big_tx.send(o).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        stats.list_failed.store(true, Relaxed);
                        eprintln!("FATAL list error (listing INCOMPLETE): {e:#}");
                    }
                }
            }
        })
        .await;

    // Close channels → consumers drain then exit.
    drop(small_tx);
    drop(big_tx);
    for h in handles {
        let _ = h.await;
    }

    // All senders (consumer clones) gone; drop ours so the journal writer flushes.
    drop(jtx);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // --delete (sync): remove dest objects with no source counterpart.
    // Refuse on a partial listing, or we'd prune live objects as orphans.
    let listing_complete = !stats.shutdown.load(Relaxed) && !stats.list_failed.load(Relaxed);
    if cfg.delete && !listing_complete {
        eprintln!("delete: skipped (source listing incomplete, refusing to prune)");
    } else if cfg.delete {
        let orphans: Vec<String> = {
            let seen = src_seen.lock().unwrap();
            idx.keys().filter(|k| !seen.contains(*k)).cloned().collect()
        };
        if orphans.is_empty() {
            eprintln!("delete: no orphan objects on dest");
        } else if cfg.dry_run {
            eprintln!(
                "delete (dry-run): {} orphan objects would be removed",
                orphans.len()
            );
        } else {
            eprintln!(
                "delete: removing {} orphan objects from dest",
                orphans.len()
            );
            let mut removed = 0usize;
            for chunk in orphans.chunks(1000) {
                let mut ids = Vec::with_capacity(chunk.len());
                for k in chunk {
                    ids.push(ObjectIdentifier::builder().key(k).build()?);
                }
                let del = Delete::builder().set_objects(Some(ids)).build()?;
                match dst
                    .delete_objects()
                    .bucket(&dst_bucket)
                    .delete(del)
                    .send()
                    .await
                {
                    Ok(_) => removed += chunk.len(),
                    Err(e) => eprintln!("delete batch error: {e}"),
                }
            }
            eprintln!("delete: {removed} removed");
        }
    }

    stats.done.store(true, Relaxed);
    let _ = reporter.await;
    print_summary(&stats);

    if stats.list_failed.load(Relaxed) {
        eprintln!(
            "\nERROR: source listing was INCOMPLETE (page failed after retries). This run did\n       NOT see every object, do not treat it as complete. Re-run to finish."
        );
        std::process::exit(4);
    }
    if stats.token_expired.load(Relaxed) {
        eprintln!(
            "\nERROR: AWS session token expired. Refresh (`source ~/aws-session.env`) and re-run:\n       already-copied objects are skipped by size, so it resumes quickly."
        );
        std::process::exit(75);
    }
    if stats.failed.load(Relaxed) > 0 {
        std::process::exit(2);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn consumer(
    rx: async_channel::Receiver<ObjectMeta>,
    src: aws_sdk_s3::Client,
    dst: aws_sdk_s3::Client,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
    throttle: Arc<throttle::Throttle>,
    limiter: Option<Arc<rate::RateLimiter>>,
    jtx: tokio::sync::mpsc::UnboundedSender<String>,
    wid: usize,
    big: bool,
) {
    while let Ok(o) = rx.recv().await {
        let key = o.key.clone();
        let ok = if big {
            transfer::do_multipart(o, &src, &dst, &cfg, &stats, &limiter, wid).await
        } else {
            transfer::do_simple(o, &src, &dst, &cfg, &stats, &throttle, &limiter, wid).await
        };
        if ok {
            let _ = jtx.send(key);
        }
    }
}

/// Plain periodic progress (MVP). Replaced by the ratatui dashboard later.
async fn reporter(stats: Arc<Stats>) {
    let gb = 1_073_741_824.0;
    let mut iv = tokio::time::interval(Duration::from_secs(2));
    let (mut prev_up, mut prev_dl) = (0u64, 0u64);
    loop {
        iv.tick().await;
        let up = stats.up_wire.load(Relaxed);
        let dl = stats.dl_wire.load(Relaxed);
        let up_rate = (up.saturating_sub(prev_up)) as f64 / 2.0 / 1_048_576.0;
        let dl_rate = (dl.saturating_sub(prev_dl)) as f64 / 2.0 / 1_048_576.0;
        prev_up = up;
        prev_dl = dl;

        let df = stats.files_done.load(Relaxed);
        let tf = stats.total_files.load(Relaxed);
        let db = stats.bytes_done.load(Relaxed) as f64 / gb;
        let tb = stats.total_bytes.load(Relaxed) as f64 / gb;
        let el = stats.start.elapsed().as_secs().max(1);
        let fps = df as f64 / el as f64;
        eprintln!(
            "[t+{:02}:{:02}:{:02}] files {}/{}  skip {}  err {}  data {:.2}/{:.2}G  ↑{:.1} ↓{:.1} MB/s  {:.1} f/s",
            el / 3600,
            (el % 3600) / 60,
            el % 60,
            df,
            tf,
            stats.skip.load(Relaxed),
            stats.failed.load(Relaxed),
            db,
            tb,
            up_rate,
            dl_rate,
            fps,
        );
        if stats.done.load(Relaxed) {
            break;
        }
    }
}

fn print_summary(stats: &Stats) {
    let gb = 1_073_741_824.0;
    let el = stats.start.elapsed().as_secs().max(1);
    eprintln!(
        "\n[ DONE ] {} files · {:.2}G · skip {} · err {} · {}s · {:.1} f/s",
        stats.files_done.load(Relaxed),
        stats.bytes_done.load(Relaxed) as f64 / gb,
        stats.skip.load(Relaxed),
        stats.failed.load(Relaxed),
        el,
        stats.files_done.load(Relaxed) as f64 / el as f64,
    );
}
