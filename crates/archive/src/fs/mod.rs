use crate::fs::local::LocalFs;
use crate::fs::s3::S3Fs;
use anyhow::{anyhow, ensure, Context};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use url::Url;


mod local;
mod s3;


pub type FSRef = Arc<dyn Fs>;

    
#[async_trait]
pub trait Fs {
    fn cd(&self, path: &str) -> FSRef;
    
    async fn ls(&self) -> anyhow::Result<Vec<String>>;

    async fn move_local(&self, local_src: &Path, dest: &str) -> anyhow::Result<()>;
    
    async fn delete(&self, path: &str) -> anyhow::Result<()>;
}


pub async fn create_fs(url: &str) -> anyhow::Result<FSRef> {
    let u = Url::parse(url)?;
    
    if u.scheme() == "s3" {
        ensure!(!u.cannot_be_a_base(), "invalid s3 url - {}", url);
        
        let bucket = u.host_str().ok_or_else(|| {
            anyhow!("bucket is missing in {}", url)   
        })?;
        
        let mut path = &u.path()[1..];
        if path.ends_with('/') {
            path = &path[0..path.len() - 1];
        }
        
        let mut config_loader = aws_config::from_env();
        if let Some(s3_endpoint) = std::env::var("AWS_S3_ENDPOINT").ok() {
            config_loader = config_loader.endpoint_url(s3_endpoint);
        }
        let config = config_loader.load().await;
        
        let s3_client = aws_sdk_s3::Client::new(&config);
        let fs = S3Fs::new(s3_client, bucket.to_string(), path.to_string());
        return Ok(Arc::new(fs))
    }
    
    if u.scheme() == "file" || u.scheme() == "" {
        let path = Path::new(u.path());
        let fs = LocalFs::new(path);
        return Ok(Arc::new(fs))
    }

    anyhow::bail!("unsupported protocol - {}", u.scheme())
}
