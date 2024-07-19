use std::collections::HashMap;
use std::sync::Arc;

use axum::{Json, Router};
use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::response::Response;
use axum::routing::post;
use futures::{StreamExt, TryStreamExt};
use serde_json::{json, Value as JsonValue};
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

use error::ApiError;
use sqd_query::Query;
use sqd_storage::db::{Database, DatasetId};

use crate::api::push::DataPush;
use crate::api::query::{check_query_kind, StaticSnapshot, stream_data};
use crate::config::{Config, DatasetConfig};


mod error;
mod push;
mod query;


struct DatasetInfo {
    id: DatasetId,
    options: DatasetConfig
}


#[derive(Clone)]
pub struct Api {
    db: Arc<Database>,
    datasets: Arc<HashMap<DatasetId, Arc<DatasetInfo>>>
}


impl Api {
    pub fn new(db: Arc<Database>, config: &Config) -> Self {
        Self {
            db,
            datasets: Arc::new({
                let mut datasets = HashMap::new();
                for (&id, options) in config.datasets.iter() {
                    let info = Arc::new(DatasetInfo {
                        id,
                        options: options.clone()
                    });
                    datasets.insert(id, info.clone());
                    for alias in options.aliases.iter().copied() {
                        datasets.insert(alias, info.clone());
                    }
                }
                datasets
            })
        }
    }

    fn get_dataset_info(&self, name: &str) -> Result<Arc<DatasetInfo>, ApiError> {
        DatasetId::try_from(name)
            .ok()
            .and_then(|id| self.datasets.get(&id))
            .cloned()
            .ok_or_else(|| {
                ApiError::DatasetNotFound(name.to_string())
            })
    }

    pub fn get_status(&self, dataset_name: &str) -> Result<Json<JsonValue>, ApiError> {
        let info = self.get_dataset_info(dataset_name)?;

        let snapshot = self.db.get_snapshot();
        let first_chunk = snapshot.get_first_chunk(info.id)?;
        let last_chunk = snapshot.get_last_chunk(info.id)?;

        let response = json!({
            "id": info.id.as_str(),
            "kind": info.options.kind.as_str(),
            "firstBlock": first_chunk.map(|c| c.first_block()),
            "lastBlock": last_chunk.as_ref().map(|c| c.last_block()),
            "lastBlockHash": last_chunk.as_ref().map(|c| c.last_block_hash())
        });

        Ok(Json(response))
    }

    pub async fn push_data(&self, dataset_name: &str, req: Request) -> Result<&'static str, ApiError> {
        let info = self.get_dataset_info(dataset_name)?;

        let mut lines = StreamReader::new(
            req.into_body()
            .into_data_stream()
            .map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::Other, err)
            })
        ).lines();
        
        let mut writer = DataPush::new(self.db.clone(), info.id, info.options.kind);

        while let Some(line) = lines.next_line().await? {
            writer.push(&line)?;
        }
        
        writer.flush()?;

        Ok("OK")
    }

    pub async fn post_query(&self, dataset_name: &str, body: &Bytes) -> Result<Response, ApiError> {
        let info = self.get_dataset_info(dataset_name)?;

        let query = Query::from_json_bytes(body).map_err(|err| {
            ApiError::UserError(format!("invalid query: {}", err))
        })?;
        
        check_query_kind(info.options.kind, &query)?;

        let snapshot = StaticSnapshot::new(self.db.clone());

        let mut chunks = snapshot.list_chunks(
            info.id,
            query.first_block().unwrap_or(0),
            query.last_block()
        );

        let mut response = Response::builder()
            .status(200)
            .header("content-type", "text/plain");

        if let Some(first_chunk) = chunks.next().transpose()? {
            let plan = Arc::new(query.compile());

            let mut body_stream = stream_data(
                std::iter::once(Ok(first_chunk)).chain(chunks),
                plan
            );

            if let Some(bytes) = body_stream.next().await.transpose()? {
                response = response.header("content-encoding", "gzip");
                
                let body = Body::from_stream(
                    futures::stream::once(async { Ok(bytes) }).chain(body_stream)
                );
                
                return Ok(response.body(body)?)
            }
        }

        Ok(response.body("".into())?)
    }

    pub fn build_router(&self) -> Router {
        use axum::routing::get;
        use axum::extract::*;
        use axum::response::IntoResponse;

        async fn get_status(
            State(api): State<Api>,
            Path(dataset_name): Path<String>
        ) -> impl IntoResponse
        {
            api.get_status(&dataset_name)
        }

        async fn post_data(
            State(api): State<Api>,
            Path(dataset_name): Path<String>,
            req: Request
        ) -> impl IntoResponse
        {
            api.push_data(&dataset_name, req).await
        }

        async fn post_query(
            State(api): State<Api>,
            Path(dataset_name): Path<String>,
            body: Bytes
        ) -> impl IntoResponse
        {
            api.post_query(&dataset_name, &body).await
        }

        Router::new()
            .route("/:id/status", get(get_status))
            .route("/:id/data", post(post_data))
            .route("/:id/query", post(post_query))
            .with_state(self.clone())
    }
}