use async_stream::try_stream;
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{BoxError, Extension, Json, Router};
use futures::TryStream;
use serde::Serialize;
use sqd_node::{Node, Query, QueryResponse};
use sqd_primitives::BlockRef;
use sqd_storage::db::DatasetId;
use std::sync::Arc;


type NodeRef = Arc<Node>;


pub fn build_app(node: NodeRef) -> Router {
    Router::new()
        .route("/", get(|| async { "Welcome to SQD hot block data service!" }))
        .route("/datasets/{id}/stream", post(stream))
        .route("/datasets/{id}/finalized-head", get(get_finalized_head))
        .route("/datasets/{id}/head", get(get_head))
        .layer(Extension(node))
}


async fn stream(
    Extension(node): Extension<NodeRef>,
    Path(dataset_id): Path<DatasetId>,
    Json(query): Json<Query>
) -> Response
{
    if let Err(err) = query.validate() {
        return (StatusCode::BAD_REQUEST, format!("{}", err)).into_response()
    }

    match node.query(dataset_id, query).await {
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
    if let Some(above_the_head) = err.downcast_ref::<sqd_node::error::QueryIsAboveTheHead>() {
        let mut res = Response::builder().status(204);
        if let Some(head) = above_the_head.finalized_head.as_ref() {
            res = res.header("x-sqd-finalized-head-number", head.number);
            res = res.header("x-sqd-finalized-head-hash", head.hash.as_str());
        }
        return res.body(Body::empty()).unwrap()
    }
    
    if let Some(fork) = err.downcast_ref::<sqd_node::error::UnexpectedBaseBlock>() {
        return (
            StatusCode::CONFLICT, 
            Json(BaseBlockConflict {
                last_blocks: &fork.prev_blocks
            })
        ).into_response()
    }

    let status_code = if err.is::<sqd_node::error::UnknownDataset>() {
        StatusCode::NOT_FOUND
    } else if err.is::<sqd_node::error::QueryKindMismatch>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<sqd_node::error::BlockRangeMissing>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<sqd_node::error::Busy>() {
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
    last_blocks: &'a [BlockRef]
}


async fn get_finalized_head(
    Extension(node): Extension<NodeRef>,
    Path(dataset_id): Path<DatasetId>
) -> Response
{
    match node.get_finalized_head(dataset_id) {
        Ok(head) => (StatusCode::OK, Json(head)).into_response(),
        Err(err) => (StatusCode::NOT_FOUND, format!("{}", err)).into_response()
    }
}


async fn get_head(
    Extension(node): Extension<NodeRef>,
    Path(dataset_id): Path<DatasetId>
) -> Response
{
    match node.get_head(dataset_id) {
        Ok(head) => (StatusCode::OK, Json(head)).into_response(),
        Err(err) => (StatusCode::NOT_FOUND, format!("{}", err)).into_response()
    }
}
