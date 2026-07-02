use crate::config::Config;
use crate::lister::ObjectMeta;
use crate::stats::Stats;
use anyhow::{Context, Result};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;

/// Multipart copy: one ranged GET from source per part, streamed into an
/// UploadPart on the dest. Parts run up to `part_concurrency` at a time.
pub async fn multipart_copy(
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
    slot.begin_up();

    // one cheap head so we can copy the content headers onto the new object
    let head = src
        .head_object()
        .bucket(&cfg.src_bucket)
        .key(&o.key)
        .send()
        .await
        .ok();
    let mut cmu = dst
        .create_multipart_upload()
        .bucket(&dst_bucket)
        .key(&dst_key);
    if let Some(h) = &head {
        cmu = cmu
            .set_content_type(h.content_type().map(String::from))
            .set_content_encoding(h.content_encoding().map(String::from))
            .set_cache_control(h.cache_control().map(String::from))
            .set_content_disposition(h.content_disposition().map(String::from))
            .set_metadata(h.metadata().cloned());
    }
    let create = cmu.send().await.context("create_multipart_upload")?;
    let upload_id = create.upload_id().unwrap_or_default().to_string();

    // S3 caps uploads at 10k parts; grow part size to fit.
    let part_size = (cfg.part_size as u64).max(o.size.div_ceil(10_000));
    let n_parts = o.size.div_ceil(part_size);

    let result: Result<Vec<CompletedPart>> = stream::iter(0..n_parts)
        .map(|i| {
            let (src, dst) = (src.clone(), dst.clone());
            let (bucket_src, key_src) = (cfg.src_bucket.clone(), o.key.clone());
            let (bucket_dst, key_dst) = (dst_bucket.clone(), dst_key.clone());
            let upload_id = upload_id.clone();
            let stats = stats.clone();
            let total = o.size;
            let max_retries = cfg.max_retries;
            async move {
                let start = i * part_size;
                let end = ((i + 1) * part_size).min(total) - 1;
                let len = end - start + 1;
                let etag = crate::retry::run(max_retries, || async {
                    let got = src
                        .get_object()
                        .bucket(&bucket_src)
                        .key(&key_src)
                        .range(format!("bytes={start}-{end}"))
                        .send()
                        .await
                        .context("ranged get_object")?;
                    let up = dst
                        .upload_part()
                        .bucket(&bucket_dst)
                        .key(&key_dst)
                        .upload_id(&upload_id)
                        .part_number(i as i32 + 1)
                        .content_length(len as i64)
                        .body(got.body)
                        .send()
                        .await
                        .context("upload_part")?;
                    Ok(up.e_tag().map(|s| s.to_string()))
                })
                .await?;
                stats.add_dl(len);
                stats.add_up(len);
                anyhow::Ok(
                    CompletedPart::builder()
                        .part_number(i as i32 + 1)
                        .set_e_tag(etag)
                        .build(),
                )
            }
        })
        .buffer_unordered(cfg.part_concurrency)
        .try_collect()
        .await;

    match result {
        Ok(mut parts) => {
            parts.sort_by_key(|p| p.part_number().unwrap_or(0));
            dst.complete_multipart_upload()
                .bucket(&dst_bucket)
                .key(&dst_key)
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .context("complete_multipart_upload")?;
            if cfg.verify {
                crate::transfer::verify_dest(dst, &dst_bucket, &dst_key, o.size).await?;
            }
            Ok(())
        }
        Err(e) => {
            let _ = dst
                .abort_multipart_upload()
                .bucket(&dst_bucket)
                .key(&dst_key)
                .upload_id(&upload_id)
                .send()
                .await;
            Err(e)
        }
    }
}
