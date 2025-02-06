use crate::fs::local::LocalFs;
use crate::fs::s3::S3Fs;
use anyhow::{anyhow, bail, ensure};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use url::Url;


pub mod local;
pub mod s3;


pub type FSRef = Arc<dyn Fs + Sync + Send>;

    
#[async_trait]
pub trait Fs {
    fn cd(&self, path: &str) -> FSRef;
    
    async fn ls(&self) -> anyhow::Result<Vec<String>>;

    async fn move_local(&self, local_src: &Path, dest: &str) -> anyhow::Result<()>;
    
    async fn delete(&self, path: &str) -> anyhow::Result<()>;
}


pub async fn create_fs(url: &str) -> anyhow::Result<FSRef> {
    match Url::parse(url) {
        Ok(u) => {
            if u.scheme() == "s3" {
                ensure!(!u.cannot_be_a_base(), "invalid s3 url - {}", url);

                let bucket = u.host_str().ok_or_else(|| {
                    anyhow!("bucket is missing in {}", url)
                })?;

                let mut config_loader = aws_config::from_env();
                if let Ok(s3_endpoint) = std::env::var("AWS_S3_ENDPOINT") {
                    config_loader = config_loader.endpoint_url(s3_endpoint);
                }
                let config = config_loader.load().await;
                let s3_client = aws_sdk_s3::Client::new(&config);
                
                let fs = S3Fs::new(s3_client, bucket.to_string()).cd(u.path());
                Ok(fs)
            } else {
                bail!("unsupported protocol - {}", u.scheme())
            }
        }
        Err(_) => {
            let path = Path::new(url);
            if path.is_absolute() || path.is_relative() {
                let fs = LocalFs::new(path);
                Ok(Arc::new(fs))
            } else {
                bail!("unsupported filesystem - {url}")
            }
        }
    }
}
