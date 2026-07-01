use crate::config::{Config, DestCreds};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region, StalledStreamProtectionConfig};

/// Source client. With --src-endpoint it targets an S3-compatible server
/// (MinIO) using static creds + path-style; otherwise the default AWS
/// credential chain (AWS_ACCESS_KEY_ID / SECRET / SESSION_TOKEN, MFA-friendly).
pub async fn source_client(cfg: &Config) -> aws_sdk_s3::Client {
    if let Some(ep) = &cfg.src_endpoint {
        let access = cfg
            .src_access_key
            .clone()
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
            .unwrap_or_default();
        let secret = cfg
            .src_secret_key
            .clone()
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
            .unwrap_or_default();
        let creds = Credentials::new(access, secret, None, None, "src-static");
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(cfg.src_region.clone()))
            .endpoint_url(ep.clone())
            .force_path_style(true)
            .credentials_provider(creds)
            .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
            .build();
        return aws_sdk_s3::Client::from_conf(conf);
    }

    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(cfg.src_region.clone()))
        .load()
        .await;
    let conf = aws_sdk_s3::config::Builder::from(&shared)
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// Destination client: static creds + endpoint override + path-style (MinIO).
pub fn dest_client(dc: &DestCreds) -> aws_sdk_s3::Client {
    let creds = Credentials::new(
        dc.access.clone(),
        dc.secret.clone(),
        None,
        None,
        "minio-static",
    );
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(dc.url.clone())
        .force_path_style(true)
        .credentials_provider(creds)
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}
