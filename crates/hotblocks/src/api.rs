use crate::cli::App;
use crate::errors::{BlockRangeMissing, Busy, QueryIsAboveTheHead, QueryKindMismatch, UnknownDataset};
use crate::query::QueryResponse;
use crate::types::RetentionStrategy;
use anyhow::bail;
use async_stream::try_stream;
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{BoxError, Extension, Json, Router};
use futures::TryStream;
use serde::Serialize;
use sqd_primitives::BlockRef;
use sqd_query::{Query, UnexpectedBaseBlock};
use sqd_storage::db::DatasetId;
use std::sync::Arc;

macro_rules! json_ok {
    ($json:expr) => {
        (StatusCode::OK, Json($json)).into_response()
    };
}


macro_rules! text {
    ($status:expr, $($arg:tt)+) => {
        ($status, format!($($arg)*)).into_response()
    };
}


type AppRef = Arc<App>;


pub fn build_api(app: App) -> Router {
    Router::new()
        .route("/", get(|| async { "Welcome to SQD hot block data service!" }))
        .route("/datasets/{id}/stream", post(stream))
        .route("/datasets/{id}/head", get(get_head))
        .route("/datasets/{id}/finalized-head", get(get_finalized_head))
        .route("/datasets/{id}/retention", get(get_retention).post(set_retention))
        .route("/datasets/{id}/status", get(get_status))
        .route("/metrics", get(get_metrics))
        .route("/rocksdb/stats", get(get_rocks_stats))
        .route("/rocksdb/prop/{cf}/{name}", get(get_rocks_prop))
        .layer(Extension(Arc::new(app)))
}


async fn stream(
    Extension(app): Extension<AppRef>,
    Path(dataset_id): Path<DatasetId>,
    Json(query): Json<Query>
) -> Response
{
    if let Err(err) = query.validate() {
        return text!(StatusCode::BAD_REQUEST, "{}", err)
    }

    match app.query_service.query(dataset_id, query).await {
        Ok(stream) => {
            let mut res = Response::builder()
                .status(200)
                .header("content-type", "text/plain")
                .header("content-encoding", "gzip");

            if let Some(head) = stream.finalized_head() {
                res = res.header("x-sqd-finalized-head-number", head.number);
                res = res.header("x-sqd-finalized-head-hash", head.hash.as_str());
            }

            let body = Body::from_stream(
                stream_query_response(stream)
            );

            res.body(body).unwrap()
        },
        Err(err) => error_to_response(err)
    }
}


fn stream_query_response(mut stream: QueryResponse) -> impl TryStream<Ok=Bytes, Error=BoxError> {
    try_stream! {
        while let Some(bytes) = stream.next_bytes().await? {
            yield bytes;
        }
    }
}


fn error_to_response(err: anyhow::Error) -> Response {
    if let Some(above_the_head) = err.downcast_ref::<QueryIsAboveTheHead>() {
        let mut res = Response::builder().status(204);
        if let Some(head) = above_the_head.finalized_head.as_ref() {
            res = res.header("x-sqd-finalized-head-number", head.number);
            res = res.header("x-sqd-finalized-head-hash", head.hash.as_str());
        }
        return res.body(Body::empty()).unwrap()
    }

    if let Some(fork) = err.downcast_ref::<UnexpectedBaseBlock>() {
        return (
            StatusCode::CONFLICT,
            Json(BaseBlockConflict {
                previous_blocks: &fork.prev_blocks
            })
        ).into_response()
    }

    let status_code = if err.is::<UnknownDataset>() {
        StatusCode::NOT_FOUND
    } else if err.is::<QueryKindMismatch>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<BlockRangeMissing>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<Busy>() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    let message = if status_code == StatusCode::INTERNAL_SERVER_ERROR {
        format!("{:?}", err)
    } else {
        format!("{}", err)
    };

    (status_code, message).into_response()
}


#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BaseBlockConflict<'a> {
    previous_blocks: &'a [BlockRef]
}



macro_rules! get_dataset {
    ($app:expr, $dataset_id:expr) => {
        match $app.data_service.get_dataset($dataset_id) {
            Ok(ds) => ds,
            Err(err) => return text!(StatusCode::NOT_FOUND, "{}", err)
        }
    };
}


async fn get_finalized_head(
    Extension(app): Extension<AppRef>,
    Path(dataset_id): Path<DatasetId>
) -> Response
{
    json_ok! {
        get_dataset!(app, dataset_id).get_finalized_head()
    }
}


async fn get_head(
    Extension(app): Extension<AppRef>,
    Path(dataset_id): Path<DatasetId>
) -> Response
{
    json_ok! {
        get_dataset!(app, dataset_id).get_head()
    }
}


async fn get_retention(
    Extension(app): Extension<AppRef>,
    Path(dataset_id): Path<DatasetId>
) -> Response
{
    json_ok! {
        get_dataset!(app, dataset_id).get_retention()
    }
}


async fn set_retention(
    Extension(app): Extension<AppRef>,
    Path(dataset_id): Path<DatasetId>,
    Json(strategy): Json<RetentionStrategy>
) -> Response
{
    let ds = get_dataset!(app, dataset_id);
    if app.api_controlled_datasets.contains(&dataset_id) {
        ds.retain(strategy);
        text!(StatusCode::OK, "OK")
    } else {
        text!(
            StatusCode::FORBIDDEN,
            "dataset '{}' is managed via config",
            dataset_id
        )
    }
}


async fn get_status(
    Extension(app): Extension<AppRef>,
    Path(dataset_id): Path<DatasetId>
) -> Response
{
    let ctl = get_dataset!(app, dataset_id);

    let read_status = || -> anyhow::Result<_> {
        let db = app.db.snapshot();

        let Some(label) = db.get_label(dataset_id)? else {
            bail!("dataset '{}' does not exist in the database", dataset_id)
        };

        let Some(first_chunk) = db.get_first_chunk(dataset_id)? else {
            return Ok(serde_json::json! {{
                "kind": label.kind(),
                "retentionStrategy": ctl.get_retention(),
                "data": null
            }})
        };

        let Some(last_chunk) = db.get_last_chunk(dataset_id)? else {
            bail!("inconsistent database read: the first chunk was found, but the last is not")
        };

        Ok(serde_json::json! {{
            "kind": label.kind(),
            "retentionStrategy": ctl.get_retention(),
            "data": {
                "firstBlock": first_chunk.first_block(),
                "lastBlock": last_chunk.last_block(),
                "lastBlockHash": last_chunk.last_block_hash(),
                "lastBlockTimestamp": last_chunk.last_block_time(),
                "finalizedHead": label.finalized_head()
            }
        }})
    };

    match read_status() {
        Ok(status) => json_ok!(status),
        Err(err) => text!(StatusCode::INTERNAL_SERVER_ERROR, "{:?}", err)
    }
}


async fn get_metrics(
    Extension(app): Extension<AppRef>
) -> Response
{
    let mut metrics = String::new();

    prometheus_client::encoding::text::encode(&mut metrics, &app.metrics_registry)
        .expect("String IO is infallible");

    metrics.into_response()
}


async fn get_rocks_stats(
    Extension(app): Extension<AppRef>
) -> Response
{
    if let Some(stats) = app.db.get_statistics() {
        stats.into_response()
    } else {
        text!(StatusCode::INTERNAL_SERVER_ERROR, "rocksdb stats are not enabled")
    }
}


async fn get_rocks_prop(
    Extension(app): Extension<AppRef>,
    Path((cf, name)): Path<(String, String)>
) -> Response
{
    match app.db.get_property(&cf, &name) {
        Ok(Some(s)) => s.into_response(),
        Ok(None) => text!(StatusCode::NOT_FOUND, "property not found"),
        Err(err) => text!(StatusCode::INTERNAL_SERVER_ERROR, "{}", err)
    }
}
