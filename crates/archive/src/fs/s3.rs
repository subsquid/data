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

    async fn upload_directory(&self, local_src: &Path, dest: &str) -> anyhow::Result<()> {
        let mut dir = tokio::fs::read_dir(local_src).await?;
        
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            let file_name = entry.file_name().into_string().unwrap();
            let dest_path = format!("{}/{}", dest, file_name);

            if path.is_dir() {
                Box::pin(self.upload_directory(&path, &dest_path)).await?;
            } else {
                let byte_stream = ByteStream::from_path(&path).await?;
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
        Arc::new(Self::new(
            self.client.clone(),
            self.bucket.clone(),
            self.resolve(path)
        ))
    }

    async fn ls(&self) -> anyhow::Result<Vec<String>> {
        let mut prefix = self.root.clone();
        if !self.root.is_empty() {
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
            let byte_stream = ByteStream::from_path(local_src).await?;
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
        let key = self.resolve_item_key(path)?;

        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&key)
            .send()
            .await?;

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
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await?;
        }

        Ok(())
    }
}