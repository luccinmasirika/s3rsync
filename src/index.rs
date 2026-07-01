use anyhow::Result;
use std::collections::HashMap;

/// Full listing of the destination into a `key -> size` map, built once.
/// Far cheaper than one HEAD per source object (~700 requests vs ~700k).
pub type DestIndex = HashMap<String, u64>;

pub async fn build(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> Result<DestIndex> {
    let mut idx = DestIndex::new();
    let mut req = client.list_objects_v2().bucket(bucket);
    if !prefix.is_empty() {
        req = req.prefix(format!("{}/", prefix.trim_matches('/')));
    }
    let mut pages = req.into_paginator().page_size(1000).send();
    while let Some(page) = pages.next().await {
        let page = page?;
        for o in page.contents() {
            if let Some(k) = o.key() {
                // skip directory markers
                if k.is_empty() || k.ends_with('/') {
                    continue;
                }
                idx.insert(k.to_string(), o.size().unwrap_or(0).max(0) as u64);
            }
        }
    }
    Ok(idx)
}
