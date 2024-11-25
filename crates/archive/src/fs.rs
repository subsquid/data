use std::env;
use std::fs;
use std::path::Path;

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use aws_sdk_s3::primitives::ByteStream;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use url::Url;
use futures::executor::block_on;


pub trait Fs {
    fn ls(&self, segments: &[&str]) -> anyhow::Result<Vec<String>>;

    fn delete(&self, loc: &str) -> anyhow::Result<()>;

    fn upload(&self, local_src: &str, dest: &str) -> anyhow::Result<()>;

    fn cd(&self, segments: &[&str]) -> Box<dyn Fs>;

    fn write_parquet<'a>(
        &self,
        dest: &str,
        batches: Box<dyn Iterator<Item = RecordBatch> + 'a>,
        schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> anyhow::Result<()>;
}


pub struct S3Fs {
    client: aws_sdk_s3::Client,
    bucket: String,
    root: String,
}


impl S3Fs {
    pub fn new(client: aws_sdk_s3::Client, bucket: String, root: String) -> Self {
        Self {
            client,
            bucket,
            root,
        }
    }

    fn rel_path(&self, segments: &[&str]) -> String {
        let mut path = self.root.clone();
        for segment in segments {
            if segment.starts_with('/') {
                path.clear();
                path.push_str(&segment[1..]);
            } else {
                if !path.is_empty() {
                    path.push('/');
                }
                path.push_str(segment);
            }
        }
        path
    }
}


impl Fs for S3Fs {
    fn ls(&self, segments: &[&str]) -> anyhow::Result<Vec<String>> {
        let mut prefix = self.rel_path(segments);
        if !prefix.is_empty() {
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
        while let Some(result) = block_on(stream.next()) {
            let output = result?;

            for common_prefix in output.common_prefixes() {
                if let Some(common_prefix) = common_prefix.prefix() {
                    let dir = common_prefix
                        .strip_prefix(&prefix)
                        .context("unexpected file name")?
                        .strip_suffix("/")
                        .context("unexpected file name")?;
                    items.push(dir.to_string());
                }
            }

            for object in output.contents() {
                if let Some(key) = object.key() {
                    let file = key.strip_prefix(&prefix).context("unexpected file name")?;
                    items.push(file.to_string());
                }
            }
        }

        Ok(items)
    }

    fn delete(&self, loc: &str) -> anyhow::Result<()> {
        let builder = self.client.delete_object().bucket(&self.bucket).key(loc);
        block_on(builder.send())?;
        Ok(())
    }

    fn upload(&self, local_src: &str, dest: &str) -> anyhow::Result<()> {
        let future = ByteStream::from_path(local_src);
        let byte_stream = block_on(future)?;
        let dest_path = self.rel_path(&[dest]);
        let builder = self.client
            .put_object()
            .bucket(&self.bucket)
            .body(byte_stream)
            .key(dest_path);
        block_on(builder.send())?;
        Ok(())
    }

    fn write_parquet<'a>(
        &self,
        dest: &str,
        batches: Box<dyn Iterator<Item = RecordBatch> + 'a>,
        schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let mut writer = ArrowWriter::try_new(&file, schema, props)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
        self.upload(file.path().to_str().unwrap(), dest)?;
        Ok(())
    }

    fn cd(&self, segments: &[&str]) -> Box<dyn Fs> {
        let root = self.rel_path(segments);
        Box::new(S3Fs::new(self.client.clone(), self.bucket.clone(), root))
    }
}


pub struct LocalFs {
    root: String,
}


impl LocalFs {
    pub fn new(root: String) -> LocalFs {
        LocalFs { root }
    }

    fn rel_path(&self, segments: &[&str]) -> String {
        let mut path = self.root.clone();
        for segment in segments {
            if segment.starts_with('/') {
                path.clear();
                path.push_str(&segment[1..]);
            } else {
                if !path.is_empty() {
                    path.push('/');
                }
                path.push_str(segment);
            }
        }
        path
    }
}


impl Fs for LocalFs {
    fn ls(&self, segments: &[&str]) -> anyhow::Result<Vec<String>> {
        let path = self.rel_path(segments);
        match fs::read_dir(path) {
            Ok(iter) => iter
                .map(|entry| {
                    let path = entry?.path();
                    let component = path.components().last().context("invalid component")?;
                    let name = component.as_os_str().to_str().context("invalid component name")?;
                    Ok(name.to_string())
                })
                .collect(),
            Err(_) => Ok(vec![]),
        }
    }

    fn cd(&self, segments: &[&str]) -> Box<dyn Fs> {
        let root = self.rel_path(segments);
        Box::new(LocalFs::new(root))
    }

    fn delete(&self, loc: &str) -> anyhow::Result<()> {
        let path = self.rel_path(&[loc]);
        let path = Path::new(&path);
        if path.is_dir() {
            fs::remove_dir_all(path)?;
        } else {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn upload(&self, _: &str, _: &str) -> anyhow::Result<()> {
        unreachable!()
    }

    fn write_parquet<'a>(
        &self,
        dest: &str,
        batches: Box<dyn Iterator<Item = RecordBatch> + 'a>,
        schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let mut writer = ArrowWriter::try_new(&file, schema, props)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
        let dest = self.rel_path(&[dest]);
        fs::rename(file.path(), dest)?;
        Ok(())
    }
}


fn is_valid_path(path: &str) -> bool {
    Path::new(path).is_absolute() || Path::new(path).is_relative()
}


pub fn create_fs(value: &str) -> anyhow::Result<Box<dyn Fs>> {
    match value.parse::<Url>() {
        Ok(url) => match url.scheme() {
            "s3" => {
                let mut config_loader = aws_config::from_env();
                let s3_endpoint = env::var("AWS_S3_ENDPOINT").ok();
                if let Some(s3_endpoint) = &s3_endpoint {
                    let endpoint = s3_endpoint
                        .parse::<String>()
                        .map_err(|_| anyhow::anyhow!("invalid s3-endpoint"))?;
                    config_loader = config_loader.endpoint_url(endpoint);
                }
                let config = block_on(config_loader.load());

                let client = aws_sdk_s3::Client::new(&config);
                let host = url.host_str().expect("invalid dataset host").to_string();
                let bucket = host + url.path();
                Ok(Box::new(S3Fs::new(client, bucket, "".to_string())))
            }
            _ => anyhow::bail!("unsupported url scheme - {}", url.scheme()),
        },
        Err(_) => {
            if is_valid_path(value) {
                Ok(Box::new(LocalFs::new(value.to_string())))
            } else {
                anyhow::bail!(format!("unsupported filesystem - {value}"))
            }
        }
    }
}
