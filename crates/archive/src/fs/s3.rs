use crate::fs::{FSRef, Fs};
use anyhow::{ensure, Context};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;
use std::sync::Arc;


pub struct S3Fs {
    client: aws_sdk_s3::Client,
    bucket: String,
    root: String
}


impl S3Fs {
    pub fn new(
        s3_client: aws_sdk_s3::Client,
        bucket: String,
        root: String
    ) -> Self {
        Self {
            client: s3_client,
            bucket,
            root
        }
    }

    fn resolve(&self, path: &str) -> String {
        let mut segments: Vec<_> = if self.root.is_empty() {
            vec![]
        } else {
            self.root.split('/').collect()
        };
        
        let mut path = path;
        if path.starts_with('/') {
            segments.clear();
            path = &path[1..];
        }

        for seg in path.split('/') {
            match seg {
                "." => {},
                ".." => {
                    segments.pop();
                },
                s => {
                    segments.push(s);
                }
            }
        }

        let mut result = String::with_capacity(
            segments.len() + segments.iter().map(|s| s.len()).sum::<usize>() + 1
        );
        
        for seg in segments {
            result.push_str(seg);
            result.push('/');
        }
        
        result.pop();
        result
    }

    fn resolve_item_key(&self, path: &str) -> anyhow::Result<String> {
        let key = self.resolve(path);
        ensure!(!key.is_empty(), "'{}' resolves to a root path, not to an item", path);
        Ok(key)
    }
}


#[async_trait]
impl Fs for S3Fs {
    fn cd(&self, path: &str) -> FSRef {
        Arc::new(Self::new(
            self.client.clone(),
            self.bucket.clone(),
            self.resolve(path)
        ))
    }

    async fn ls(&self) -> anyhow::Result<Vec<String>> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .delimiter('/');
        
        if !self.root.is_empty() {
            let mut prefix = String::with_capacity(self.root.len() + 1);
            prefix.push_str(&self.root);
            prefix.push('/');
            req = req.prefix(prefix);
        }
        
        let mut stream = req.into_paginator().send();

        let mut items = vec![];

        while let Some(result) = stream.next().await {
            let output = result?;

            for common_prefix in output.common_prefixes() {
                if let Some(common_prefix) = common_prefix.prefix() {
                    let dir = common_prefix
                        .strip_prefix(&self.root)
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
                        .strip_prefix(&self.root)
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
        let byte_stream = ByteStream::from_path(local_src).await?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .body(byte_stream)
            .key(&dest_key)
            .send()
            .await?;

        tokio::fs::remove_file(local_src).await?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&self.resolve_item_key(path)?)
            .send()
            .await?;
        Ok(())
    }
}