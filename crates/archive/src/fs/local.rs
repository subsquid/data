use crate::fs::{FSRef, Fs};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;


pub struct LocalFs {
    root: PathBuf,
}


impl LocalFs {
    pub fn new(root: impl Into<PathBuf>) -> LocalFs {
        Self { root: root.into() }
    }
}


#[async_trait]
impl Fs for LocalFs {
    fn cd(&self, path: &str) -> FSRef {
        Arc::new(Self::new(
            self.root.join(path)
        ))
    }

    async fn ls(&self) -> anyhow::Result<Vec<String>> {
        match tokio::fs::read_dir(&self.root).await {
            Ok(mut read_dir) => {
                let mut result = Vec::new();
                while let Some(entry) = read_dir.next_entry().await? {
                    if let Some(name) = entry.file_name().to_str() {
                        result.push(name.to_string());
                    }
                }
                Ok(result)
            }
            Err(_) => Ok(vec![])
        }
    }

    async fn move_local(&self, local_src: &Path, dest: &str) -> anyhow::Result<()> {
        let dest = self.root.join(dest);
        if let Some(dir) = dest.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        tokio::fs::rename(local_src, dest).await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let path = self.root.join(path);
        if path.is_dir() && !path.is_symlink() {
            tokio::fs::remove_dir_all(path).await?;
        } else {
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }
}