use crate::fs::{FSRef, Fs};
use anyhow::{ensure, Context};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;
use std::sync::Arc;


pub struct S3Fs {
    client: aws_sdk_s3::Client,
    bucket: String,
    path: Vec<String>
}


impl S3Fs {
    pub fn new(
        s3_client: aws_sdk_s3::Client,
        bucket: String
    ) -> Self {
        Self {
            client: s3_client,
            bucket,
            path: Vec::new()
        }
    }

    fn resolve(&self, mut path: &str) -> Vec<String> {
        let mut result = self.path.clone();
        
        if path.starts_with('/') {
            result.clear();
            path = &path[1..];
        }
        
        if path.ends_with('/') {
            path = &path[0..path.len() - 1];
        }

        for seg in path.split('/') {
            match seg {
                "." | "" => {},
                ".." => {
                    result.pop();
                },
                s => {
                    result.push(s.to_string());
                }
            }
        }

        result
    }
    
    fn resolve_key(&self, path: &str) -> String {
        let key_segments = self.resolve(path);

        let mut key = String::with_capacity(
            key_segments.len() + key_segments.iter().map(|s| s.len()).sum::<usize>() + 1
        );

        for seg in key_segments {
            key.push_str(&seg);
            key.push('/');
        }
        key.pop();
        
        key
    }

    fn resolve_item_key(&self, path: &str) -> anyhow::Result<String> {
        let key = self.resolve_key(path);
        ensure!(
            !key.is_empty(), 
            "'{}' resolves to a root path, not to an item", 
            path
        );
        Ok(key)
    }

    async fn upload_directory(&self, local_src: &Path, dest: &str) -> anyhow::Result<()> {
        let mut dir = tokio::fs::read_dir(local_src).await?;
        
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            let file_name = entry.file_name().into_string().unwrap();
            let dest_path = format!("{}/{}", dest, file_name);

            if path.is_dir() {
                Box::pin(self.upload_directory(&path, &dest_path)).await?;
            } else {
                let mut file = tokio::fs::File::open(&path).await?;
                let mut buffer = Vec::new();
                tokio::io::copy(&mut file, &mut buffer).await?;
                let byte_stream = ByteStream::from(buffer);

                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .body(byte_stream)
                    .key(&self.resolve_item_key(&dest_path)?)
                    .send()
                    .await?;
            }
        }

        Ok(())
    }
}


#[async_trait]
impl Fs for S3Fs {
    fn cd(&self, path: &str) -> FSRef {
        Arc::new(Self {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            path: self.resolve(path)
        })
    }

    async fn ls(&self) -> anyhow::Result<Vec<String>> {
        let mut prefix = self.resolve_key(".");
        
        if !self.path.is_empty() {
            prefix.push('/');
        }

        let mut stream = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .delimiter('/')
            .prefix(&prefix)
            .into_paginator()
            .send();

        let mut items = vec![];

        while let Some(result) = stream.next().await {
            let output = result?;

            for common_prefix in output.common_prefixes() {
                if let Some(common_prefix) = common_prefix.prefix() {
                    let dir = common_prefix
                        .strip_prefix(&prefix)
                        .context("unexpected file name")?
                        .strip_suffix("/")
                        .context("unexpected file name")?
                        .to_string();

                    items.push(dir);
                }
            }

            for object in output.contents() {
                if let Some(key) = object.key() {
                    let file = key
                        .strip_prefix(&prefix)
                        .context("unexpected file name")?
                        .to_string();
                    items.push(file);
                }
            }
        }

        Ok(items)
    }

    async fn move_local(&self, local_src: &Path, dest: &str) -> anyhow::Result<()> {
        let dest_key = self.resolve_item_key(dest)?;

        if local_src.is_dir() {
            self.upload_directory(local_src, &dest_key).await?;
            tokio::fs::remove_dir_all(local_src).await?;
        } else {
            let mut file = tokio::fs::File::open(local_src).await?;
            let mut buffer = Vec::new();
            tokio::io::copy(&mut file, &mut buffer).await?;
            let byte_stream = ByteStream::from(buffer);

            self.client
                .put_object()
                .bucket(&self.bucket)
                .body(byte_stream)
                .key(&dest_key)
                .send()
                .await?;
            tokio::fs::remove_file(local_src).await?;
        }

        Ok(())
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let mut prefix = self.resolve_item_key(path)?;
        prefix.push('/');

        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await?;
        
        ensure!(
            !output.is_truncated.unwrap_or(false),
            "too many items under '{}', can't delete that much",
            prefix
        );

        if let Some(contents) = output.contents {
            for object in contents {
                if let Some(key) = object.key() {
                    self.client
                        .delete_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await?;
                }
            }
        } else {
            prefix.pop();
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&prefix)
                .send()
                .await?;
        }

        Ok(())
    }
}