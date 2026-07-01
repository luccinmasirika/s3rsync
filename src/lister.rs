use anyhow::{Context, Result};
use futures::Stream;
use std::collections::VecDeque;

#[derive(Clone, Debug)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
}

/// Lazily stream every object under `prefix`, paginating by continuation token
/// with per-page retry (transient blips don't drop objects). If a page still
/// fails after retries, a single `Err` is yielded then the stream ends: the
/// caller MUST treat that as an incomplete listing and fail loudly, never silent.
pub fn list_source(
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
) -> impl Stream<Item = Result<ObjectMeta>> {
    let prefix_arg = if prefix.is_empty() {
        None
    } else {
        Some(format!("{}/", prefix.trim_matches('/')))
    };
    // next: Option<Option<String>>
    //   None            → no more pages, stop
    //   Some(None)      → fetch first page
    //   Some(Some(tok)) → fetch page after `tok`
    futures::stream::unfold(
        (
            client,
            bucket,
            prefix_arg,
            Some(None::<String>),
            VecDeque::<ObjectMeta>::new(),
        ),
        |(client, bucket, prefix, mut next, mut buf)| async move {
            loop {
                if let Some(m) = buf.pop_front() {
                    return Some((Ok(m), (client, bucket, prefix, next, buf)));
                }
                let token = match &next {
                    None => return None,
                    Some(t) => t.clone(),
                };
                let page = crate::retry::run(5, || async {
                    let mut req = client.list_objects_v2().bucket(&bucket).max_keys(1000);
                    if let Some(p) = &prefix {
                        req = req.prefix(p);
                    }
                    if let Some(t) = &token {
                        req = req.continuation_token(t);
                    }
                    req.send().await.context("list_objects_v2")
                })
                .await;

                match page {
                    Ok(page) => {
                        for o in page.contents() {
                            if let Some(k) = o.key() {
                                if k.is_empty() || k.ends_with('/') {
                                    continue;
                                }
                                buf.push_back(ObjectMeta {
                                    key: k.to_string(),
                                    size: o.size().unwrap_or(0).max(0) as u64,
                                });
                            }
                        }
                        next = page.next_continuation_token().map(|t| Some(t.to_string()));
                        // loop: drain buf, or fetch the next page if this one was empty
                    }
                    Err(e) => {
                        return Some((Err(e), (client, bucket, prefix, None, buf)));
                    }
                }
            }
        },
    )
}
