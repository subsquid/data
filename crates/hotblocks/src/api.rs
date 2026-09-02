use std::{
    future::Future,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering}
    },
    time::Instant
};

use anyhow::bail;
use async_stream::try_stream;
use axum::{
    BoxError, Extension, Json, Router,
    body::{Body, Bytes},
    extract::{Path, Request},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post}
};
use futures::Stream;
use serde::Serialize;
use sqd_primitives::BlockRef;
use sqd_query::{Query, UnexpectedBaseBlock};
use sqd_storage::db::DatasetId;
use tower_http::request_id::{MakeRequestUuid, RequestId, SetRequestIdLayer};
use tracing::{Instrument, error};

use crate::{
    cli::App,
    dataset_controller::DatasetController,
    encoding::ContentEncoding,
    errors::{
        BlockItemIsNotAvailable, BlockRangeMissing, Busy, QueryIsAboveTheHead, QueryKindMismatch, QueryTaskPanicked,
        UnknownDataset, UnsupportedQuery
    },
    query::QueryResponse,
    types::{ClientId, RetentionStrategy}
};

const DEFAULT_CLIENT_ID: &str = "unknown";

#[derive(Clone, Copy, Debug)]
enum ErrorCode {
    MalformedRequest,
    UnsupportedQuery,
    KindMismatch,
    UnknownDataset,
    RangeUnavailable,
    ItemUnavailable,
    NotFound,
    NoData,
    Conflict,
    Overloaded,
    Internal,
    // Catch-all for error responses that don't set a specific code (e.g. admin endpoints).
    Unclassified
}

impl ErrorCode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedRequest => "MALFORMED_REQUEST",
            Self::UnsupportedQuery => "UNSUPPORTED_QUERY",
            Self::KindMismatch => "KIND_MISMATCH",
            Self::UnknownDataset => "UNKNOWN_DATASET",
            Self::RangeUnavailable => "RANGE_UNAVAILABLE",
            Self::ItemUnavailable => "ITEM_UNAVAILABLE",
            Self::NotFound => "NOT_FOUND",
            Self::NoData => "NO_DATA",
            Self::Conflict => "CONFLICT",
            Self::Overloaded => "OVERLOADED",
            Self::Internal => "INTERNAL",
            Self::Unclassified => "UNCLASSIFIED"
        }
    }
}

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

macro_rules! get_dataset {
    ($app:expr, $dataset_id:expr) => {
        match $app.data_service.get_dataset($dataset_id) {
            Ok(ds) => ds,
            Err(err) => return error_response(StatusCode::NOT_FOUND, ErrorCode::UnknownDataset, err.to_string())
        }
    };
}

type AppRef = Arc<App>;

pub fn build_api(app: App, shutting_down: Arc<AtomicBool>) -> Router {
    Router::new()
        .route("/", get(|| async { "Welcome to SQD hot block data service!" }))
        .route("/datasets/{id}/stream", post(stream))
        .route("/datasets/{id}/finalized-stream", post(finalized_stream))
        .route("/datasets/{id}/head", get(get_head))
        .route("/datasets/{id}/finalized-head", get(get_finalized_head))
        .route("/datasets/{id}/hashes/{hash}/block", get(get_block_by_hash))
        .route("/datasets/{id}/hashes/{hash}/transaction", get(get_transaction_by_hash))
        .route("/datasets/{id}/retention", get(get_retention).post(set_retention))
        .route("/datasets/{id}/status", get(get_status))
        .route("/datasets/{id}/metadata", get(get_metadata))
        .route("/metrics", get(get_metrics))
        .route("/rocksdb/stats", get(get_rocks_stats))
        .route("/rocksdb/prop/{cf}/{name}", get(get_rocks_prop))
        .fallback(handle_404)
        .layer(axum::middleware::from_fn(middleware))
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid::default()))
        .layer(Extension(Arc::new(app)))
        // Routed after the layers deliberately: axum leaves later routes unwrapped, and the
        // grace window's 503s are a rotation signal, not faults -- inside `middleware` they
        // would land in `http_status` as `error_class="Unclassified"` on every termination.
        .route("/ready", get(get_readiness).layer(Extension(shutting_down)))
}

/// Rotation gate, not liveness: 503 from the moment shutdown starts. Per-dataset
/// readability (LIV-5c) is not modelled -- serving is gated on full init today (GAP-7).
async fn get_readiness(Extension(shutting_down): Extension<Arc<AtomicBool>>) -> impl IntoResponse {
    if shutting_down.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Shutting down").into_response();
    }
    (StatusCode::OK, "Ready").into_response()
}

const HASH_MAX_LEN: usize = 256;

fn invalid_hash(hash: &str) -> bool {
    hash.is_empty() || hash.len() > HASH_MAX_LEN
}

#[cfg(test)]
mod hash_validation_tests {
    use super::*;

    #[test]
    fn invalid_hash_counts_utf8_bytes_at_the_256_byte_boundary() {
        assert!(invalid_hash(""));
        assert!(!invalid_hash(&"x".repeat(HASH_MAX_LEN)));
        assert!(!invalid_hash(&"é".repeat(HASH_MAX_LEN / 2)));
        assert!(invalid_hash(&"é".repeat(HASH_MAX_LEN / 2 + 1)));
    }
}

/// Names the replica that served a response. A Service load-balances across replicas behind
/// one ClusterIP, so a caller cannot otherwise tell them apart. The portal logs this and
/// strips every `x-internal-*` header before responding, so it never reaches a client.
const INSTANCE_HEADER: HeaderName = HeaderName::from_static("x-internal-hotblocks-instance");

/// An empty or unrepresentable name resolves to `unknown`, never to an empty value: the
/// portal reads this as a name or as `unknown`, and must never have to treat "" as either.
fn instance_header(hostname: Option<&str>) -> HeaderValue {
    hostname
        .filter(|name| !name.is_empty())
        .and_then(|name| HeaderValue::from_str(name).ok())
        .unwrap_or_else(|| HeaderValue::from_static("unknown"))
}

/// Kubernetes sets HOSTNAME to the pod name.
static INSTANCE: LazyLock<HeaderValue> = LazyLock::new(|| instance_header(std::env::var("HOSTNAME").ok().as_deref()));

#[cfg(test)]
mod instance_header_tests {
    use super::*;

    #[test]
    fn hostname_names_the_instance() {
        assert_eq!(instance_header(Some("hotblocks-db-0")), "hotblocks-db-0");
    }

    #[test]
    fn absent_empty_and_unrepresentable_all_collapse_to_unknown() {
        assert_eq!(instance_header(None), "unknown");
        assert_eq!(instance_header(Some("")), "unknown");
        assert_eq!(instance_header(Some("pod\nname")), "unknown");
    }
}

pub async fn middleware(mut req: Request, next: axum::middleware::Next) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let version = req.version();
    let start = Instant::now();

    let app = req.extensions().get::<Arc<App>>().expect("App extension should be set");

    let client_id = req
        .headers()
        .get("x-sqd-client-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| app.known_clients.contains(*v))
        .unwrap_or(DEFAULT_CLIENT_ID)
        .to_string();

    req.extensions_mut().insert(ClientId::new(client_id));

    let request_id = req
        .extensions()
        .get::<RequestId>()
        .expect("RequestId should be set by SetRequestIdLayer")
        .header_value()
        .to_str()
        .expect("Request ID should be a valid string");

    let span = tracing::span!(tracing::Level::INFO, "http_request", request_id);
    let mut response = next.run(req).instrument(span.clone()).await;
    let latency = start.elapsed();

    response.headers_mut().insert(INSTANCE_HEADER, INSTANCE.clone());

    let mut labels = response
        .extensions_mut()
        .remove::<Labels>()
        .map(|labels| labels.0)
        .unwrap_or(Vec::new());
    let status = response.status();
    let error_code = response.extensions_mut().remove::<ErrorCode>();
    // Every error keeps an error_class so none is invisible to error-rate alerting;
    // handlers that don't set a specific code fall back to Unclassified.
    if let Some(error_code) = error_code {
        labels.push(("error_class", error_code.as_str().to_owned()));
    } else if status.is_client_error() || status.is_server_error() {
        labels.push(("error_class", ErrorCode::Unclassified.as_str().to_owned()));
    }
    labels.push(("status", status.as_str().to_owned()));

    span.in_scope(|| {
        tracing::debug!(
            target: "http_request",
            method,
            path,
            ?version,
            status = %response.status(),
            ?latency,
            "HTTP request processed"
        );
    });

    crate::metrics::report_http_response(&labels, latency);

    response
}

#[derive(Clone)]
pub struct Labels(Vec<(&'static str, String)>);

pub struct ResponseWithMetadata {
    pub labels: Labels,
    pub response: Option<Response>
}

impl ResponseWithMetadata {
    fn new() -> Self {
        Self {
            labels: Labels(vec![]),
            response: None
        }
    }

    pub fn with_client_id(mut self, client_id: &ClientId) -> Self {
        self.labels.0.push(("client_id", client_id.as_str().to_owned()));
        self
    }

    pub fn with_dataset_id(mut self, id: DatasetId) -> Self {
        self.labels.0.push(("dataset_name", id.as_str().to_owned()));
        self
    }

    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.labels.0.push(("endpoint", endpoint.to_string()));
        self
    }

    pub fn with_response<F>(mut self, clause: F) -> Self
    where
        F: FnOnce() -> Response
    {
        self.response = Some(clause());
        self
    }
}

impl IntoResponse for ResponseWithMetadata {
    fn into_response(self) -> Response {
        let mut response = self.response.expect("response is mandatory method");
        response.extensions_mut().insert(self.labels);
        response
    }
}

async fn stream(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>,
    headers: HeaderMap,
    body: Bytes
) -> impl IntoResponse {
    let encoding = ContentEncoding::from_headers(&headers);
    let response = stream_internal(app, dataset_id, body, false, client_id.clone(), encoding).await;
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id)
        .with_endpoint("/stream")
        .with_response(|| response)
}

async fn finalized_stream(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>,
    headers: HeaderMap,
    body: Bytes
) -> impl IntoResponse {
    let encoding = ContentEncoding::from_headers(&headers);
    let response = stream_internal(app, dataset_id, body, true, client_id.clone(), encoding).await;
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id)
        .with_endpoint("/finalized_stream")
        .with_response(|| response)
}

async fn stream_internal(
    app: AppRef,
    dataset_id: DatasetId,
    body: Bytes,
    finalized: bool,
    client_id: ClientId,
    encoding: ContentEncoding
) -> Response {
    let dataset = get_dataset!(app, dataset_id);

    let query: Query = match Json::<Query>::from_bytes(&body) {
        Ok(Json(q)) => q,
        Err(rejection) => return error_response(rejection.status(), ErrorCode::MalformedRequest, rejection.body_text())
    };

    if let Err(err) = query.validate() {
        return error_response(StatusCode::BAD_REQUEST, ErrorCode::MalformedRequest, err.to_string());
    }

    let query_result = if finalized {
        app.query_service
            .query_finalized(&dataset, query, client_id, encoding)
            .await
    } else {
        app.query_service.query(&dataset, query, client_id, encoding).await
    };

    match query_result {
        Ok(stream) => {
            let mut res = Response::builder()
                .status(200)
                .header("content-type", "text/plain")
                .header("content-encoding", encoding.as_str())
                .header("vary", "Accept-Encoding");

            if let Some(finalized_head) = stream.finalized_head() {
                if finalized {
                    // For finalized stream, use the finalized head as the head
                    res = res.header("x-sqd-head-number", finalized_head.number);
                } else {
                    let head_block = finalized_head.number.max(dataset.get_head_block_number().unwrap_or(0));
                    res = res.header("x-sqd-head-number", head_block);
                }
                res = res.header("x-sqd-finalized-head-number", finalized_head.number);
                res = res.header("x-sqd-finalized-head-hash", finalized_head.hash.as_str());
            } else if let Some(head_block) = dataset.get_head_block_number() {
                res = res.header("x-sqd-head-number", head_block);
            }

            let body = Body::from_stream(stream_query_response(stream));

            res.body(body).unwrap()
        }
        Err(err) => error_to_response(err, &body)
    }
}

/// Pack source for [`stream_query_response`]; a trait so tests can script the panic
/// path without a live database.
trait DataPackSource: Send + 'static {
    fn next_pack(&mut self) -> impl Future<Output = anyhow::Result<Option<Bytes>>> + Send;
    fn finish_stream(&mut self) -> Bytes;
}

impl DataPackSource for QueryResponse {
    fn next_pack(&mut self) -> impl Future<Output = anyhow::Result<Option<Bytes>>> + Send {
        self.next_data_pack()
    }

    fn finish_stream(&mut self) -> Bytes {
        self.finish()
    }
}

fn stream_query_response<S: DataPackSource>(mut stream: S) -> impl Stream<Item = Result<Bytes, BoxError>> {
    try_stream! {
        while let Some(pack_result) = stream.next_pack().await.transpose() {
            match pack_result {
                Ok(bytes) => {
                    yield bytes;
                },
                Err(err) if stream_can_finish_cleanly(&err) => {
                    if !err.is::<Busy>() {
                        error!(err =? err, "terminating response stream due to query error");
                    }
                    // Partial data is never produced: the buffer is complete up to the last block.
                    yield stream.finish_stream();
                    return
                }
                Err(err) => {
                    // Panic dropped the runner mid-stream: no trailer to emit, so abort the
                    // body rather than close with a clean 200 that hides the missing data.
                    crate::metrics::report_query_worker_panic();
                    error!(err =? err, "aborting response stream after query worker panic");
                    Err::<(), _>(err)?;
                }
            }
        }
    }
}

/// A failed pack finishes as a valid partial 200, except a worker panic: that drops
/// the runner mid-stream (no trailer to emit), so such a stream must abort instead.
fn stream_can_finish_cleanly(err: &anyhow::Error) -> bool {
    !err.is::<QueryTaskPanicked>()
}

#[cfg(test)]
mod readiness_tests {
    use super::*;

    #[tokio::test]
    async fn ready_flips_to_503_once_shutdown_starts() {
        let shutting_down = Arc::new(AtomicBool::new(false));

        let res = get_readiness(Extension(shutting_down.clone())).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);

        shutting_down.store(true, Ordering::Relaxed);

        let res = get_readiness(Extension(shutting_down)).await.into_response();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[cfg(test)]
mod stream_query_response_tests {
    use std::collections::VecDeque;

    use futures::StreamExt;

    use super::*;

    const CHUNK: &[u8] = b"chunk-0";
    const FINISH: &[u8] = b"__finish__";

    /// `finish_stream` returns a sentinel so a test can tell a clean finish from an abort.
    struct ScriptedSource(VecDeque<anyhow::Result<Option<Bytes>>>);

    impl DataPackSource for ScriptedSource {
        fn next_pack(&mut self) -> impl Future<Output = anyhow::Result<Option<Bytes>>> + Send {
            let next = self.0.pop_front().unwrap_or(Ok(None));
            async move { next }
        }

        fn finish_stream(&mut self) -> Bytes {
            Bytes::from_static(FINISH)
        }
    }

    async fn drive(packs: Vec<anyhow::Result<Option<Bytes>>>) -> Vec<Result<Bytes, BoxError>> {
        stream_query_response(ScriptedSource(packs.into())).collect().await
    }

    #[tokio::test]
    async fn worker_panic_mid_stream_aborts_the_body() {
        let out = drive(vec![Ok(Some(Bytes::from_static(CHUNK))), Err(QueryTaskPanicked.into())]).await;

        assert!(matches!(out.first(), Some(Ok(b)) if b.as_ref() == CHUNK));
        assert!(
            out.last().is_some_and(|r| r.is_err()),
            "a worker panic must abort the stream"
        );
        assert!(
            !out.iter().any(|r| matches!(r, Ok(b) if b.as_ref() == FINISH)),
            "an aborted stream must not emit a clean finish"
        );
    }

    #[tokio::test]
    async fn recoverable_error_mid_stream_finishes_clean() {
        let out = drive(vec![Ok(Some(Bytes::from_static(CHUNK))), Err(Busy.into())]).await;

        assert!(
            out.iter().all(|r| r.is_ok()),
            "a recoverable error must not abort the stream"
        );
        assert!(
            out.iter().any(|r| matches!(r, Ok(b) if b.as_ref() == FINISH)),
            "a recoverable error must finish the buffered response"
        );
    }
}

fn error_to_response(err: anyhow::Error, body: &Bytes) -> Response {
    if let Some(above_the_head) = err.downcast_ref::<QueryIsAboveTheHead>() {
        let mut res = Response::builder().status(204);
        if let Some(head) = above_the_head.finalized_head.as_ref() {
            res = res.header("x-sqd-finalized-head-number", head.number);
            res = res.header("x-sqd-finalized-head-hash", head.hash.as_str());
        }
        return with_error_code(res.body(Body::empty()).unwrap(), ErrorCode::NoData);
    }

    if let Some(fork) = err.downcast_ref::<UnexpectedBaseBlock>() {
        let response = (
            StatusCode::CONFLICT,
            Json(BaseBlockConflict {
                previous_blocks: &fork.prev_blocks
            })
        )
            .into_response();
        return with_error_code(response, ErrorCode::Conflict);
    }

    let (status_code, error_code) = if err.is::<UnknownDataset>() {
        (StatusCode::NOT_FOUND, ErrorCode::UnknownDataset)
    } else if err.is::<UnsupportedQuery>() {
        (StatusCode::BAD_REQUEST, ErrorCode::UnsupportedQuery)
    } else if err.is::<QueryKindMismatch>() {
        (StatusCode::BAD_REQUEST, ErrorCode::KindMismatch)
    } else if err.is::<BlockRangeMissing>() {
        (StatusCode::BAD_REQUEST, ErrorCode::RangeUnavailable)
    } else if err.is::<BlockItemIsNotAvailable>() {
        (StatusCode::BAD_REQUEST, ErrorCode::ItemUnavailable)
    } else if err.is::<Busy>() {
        (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::Overloaded)
    } else {
        error!(
            err = ?err,
            query = %String::from_utf8_lossy(body),
            "unhandled error, returning 500"
        );
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            format!("{:?}", err)
        );
    };

    error_response(status_code, error_code, err.to_string())
}

fn error_response(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Response {
    // FIXME(GAP-36): expose `code` on the HTTP binding once a backwards-compatible
    // representation is chosen. sqd-portal forwards this body verbatim, so keep the
    // established text/plain response for now.
    let response = (status, message.into()).into_response();
    with_error_code(response, code)
}

fn with_error_code(mut response: Response, code: ErrorCode) -> Response {
    response.extensions_mut().insert(code);
    response
}

#[cfg(test)]
mod error_response_tests {
    use super::*;

    #[tokio::test]
    async fn classified_errors_keep_the_plain_text_wire_format() {
        let response = error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::UnsupportedQuery,
            "substrate queries are not supported"
        );

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response.extensions().get::<ErrorCode>().map(|code| code.as_str()),
            Some("UNSUPPORTED_QUERY")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "substrate queries are not supported");
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BaseBlockConflict<'a> {
    previous_blocks: &'a [BlockRef]
}

async fn get_finalized_head(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>
) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id.clone())
        .with_endpoint("/finalized_head")
        .with_response(|| {
            json_ok! {
                get_dataset!(app, dataset_id).get_finalized_head()
            }
        })
}

async fn get_head(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>
) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id.clone())
        .with_endpoint("/head")
        .with_response(|| {
            json_ok! {
                 get_dataset!(app, dataset_id).get_head()
            }
        })
}

async fn get_block_by_hash(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path((dataset_id, hash)): Path<(DatasetId, String)>
) -> impl IntoResponse {
    // Reject absurd lengths before touching the DB (real hashes are 64-88 bytes).
    if invalid_hash(&hash) {
        return ResponseWithMetadata::new()
            .with_client_id(&client_id)
            .with_dataset_id(dataset_id)
            .with_endpoint("/hashes/{hash}/block")
            .with_response(|| {
                error_response(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::MalformedRequest,
                    "invalid hash length"
                )
            });
    }

    let dataset = match app.data_service.get_dataset(dataset_id) {
        Ok(ds) => ds,
        Err(err) => {
            return ResponseWithMetadata::new()
                .with_client_id(&client_id)
                .with_dataset_id(dataset_id)
                .with_endpoint("/hashes/{hash}/block")
                .with_response(|| error_response(StatusCode::NOT_FOUND, ErrorCode::UnknownDataset, err.to_string()));
        }
    };

    let response = match dataset.get_block_by_hash(hash).await {
        Ok(Some(block_ref)) => json_ok!(block_ref),
        Ok(None) => error_response(StatusCode::NOT_FOUND, ErrorCode::NotFound, "block not found"),
        Err(err) => {
            error!(error = ?err, dataset_id = %dataset_id, "get_block_by_hash failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal, "internal error")
        }
    };

    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id)
        .with_endpoint("/hashes/{hash}/block")
        .with_response(|| response)
}

async fn get_transaction_by_hash(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path((dataset_id, hash)): Path<(DatasetId, String)>
) -> impl IntoResponse {
    // Reject absurd lengths before touching the DB (real hashes are 64-88 bytes).
    if invalid_hash(&hash) {
        return ResponseWithMetadata::new()
            .with_client_id(&client_id)
            .with_dataset_id(dataset_id)
            .with_endpoint("/hashes/{hash}/transaction")
            .with_response(|| {
                error_response(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::MalformedRequest,
                    "invalid hash length"
                )
            });
    }

    let dataset = match app.data_service.get_dataset(dataset_id) {
        Ok(ds) => ds,
        Err(err) => {
            return ResponseWithMetadata::new()
                .with_client_id(&client_id)
                .with_dataset_id(dataset_id)
                .with_endpoint("/hashes/{hash}/transaction")
                .with_response(|| error_response(StatusCode::NOT_FOUND, ErrorCode::UnknownDataset, err.to_string()));
        }
    };

    let response = match dataset.get_transaction_by_hash(hash).await {
        Ok(Some(transaction_ref)) => json_ok!(transaction_ref),
        Ok(None) => error_response(StatusCode::NOT_FOUND, ErrorCode::NotFound, "transaction not found"),
        Err(err) => {
            error!(error = ?err, dataset_id = %dataset_id, "get_transaction_by_hash failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal, "internal error")
        }
    };

    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id)
        .with_endpoint("/hashes/{hash}/transaction")
        .with_response(|| response)
}

async fn get_retention(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>
) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id.clone())
        .with_endpoint("/retention")
        .with_response(|| {
            json_ok! {
                get_dataset!(app, dataset_id).get_retention()
            }
        })
}

async fn set_retention(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>,
    Json(strategy): Json<RetentionStrategy>
) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id.clone())
        .with_endpoint("/retention")
        .with_response(|| {
            let ds = get_dataset!(app, dataset_id);
            if app.api_controlled_datasets.contains(&dataset_id) {
                ds.retain(strategy);
                text!(StatusCode::OK, "OK")
            } else {
                text!(
                    StatusCode::FORBIDDEN,
                    "dataset '{}' can't be managed via API",
                    dataset_id
                )
            }
        })
}

async fn get_status(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>
) -> impl IntoResponse {
    let read_status = |ctl: Arc<DatasetController>| -> anyhow::Result<_> {
        let db = app.db.snapshot();

        let Some(label) = db.get_label(dataset_id)? else {
            bail!("dataset '{}' does not exist in the database", dataset_id)
        };

        let Some(first_chunk) = db.get_first_chunk(dataset_id)? else {
            return Ok(serde_json::json! {{
                "kind": label.kind(),
                "retentionStrategy": ctl.get_retention(),
                "data": null
            }});
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

    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id.clone())
        .with_endpoint("/status")
        .with_response(|| {
            let ctl = get_dataset!(app, dataset_id);
            match read_status(ctl) {
                Ok(status) => json_ok!(status),
                Err(err) => text!(StatusCode::INTERNAL_SERVER_ERROR, "{:?}", err)
            }
        })
}

async fn get_metadata(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path(dataset_id): Path<DatasetId>
) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_dataset_id(dataset_id.clone())
        .with_endpoint("/metadata")
        .with_response(|| {
            get_dataset!(app, dataset_id);

            let db = app.db.snapshot();

            let first_chunk = match db.get_first_chunk(dataset_id) {
                Ok(chunk) => chunk,
                Err(err) => return text!(StatusCode::INTERNAL_SERVER_ERROR, "{:?}", err)
            };

            json_ok!(serde_json::json! {{
                "dataset": dataset_id,
                "aliases": [],
                "real_time": true,
                "start_block": first_chunk.map(|chunk| chunk.first_block()),
            }})
        })
}

async fn get_metrics(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>
) -> impl IntoResponse {
    let mut metrics = String::new();

    prometheus_client::encoding::text::encode(&mut metrics, &app.metrics_registry).expect("String IO is infallible");

    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_endpoint("/metrics")
        .with_response(|| metrics.into_response())
}

async fn get_rocks_stats(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>
) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_endpoint("/rocks_stats")
        .with_response(|| {
            if let Some(stats) = app.db.get_statistics() {
                stats.into_response()
            } else {
                text!(StatusCode::INTERNAL_SERVER_ERROR, "rocksdb stats are not enabled")
            }
        })
}

async fn get_rocks_prop(
    Extension(app): Extension<AppRef>,
    Extension(client_id): Extension<ClientId>,
    Path((cf, name)): Path<(String, String)>
) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_endpoint("/rocks_prop")
        .with_response(|| match app.db.get_property(&cf, &name) {
            Ok(Some(s)) => s.into_response(),
            Ok(None) => text!(StatusCode::NOT_FOUND, "property not found"),
            Err(err) => text!(StatusCode::INTERNAL_SERVER_ERROR, "{}", err)
        })
}

async fn handle_404(Extension(client_id): Extension<ClientId>, uri: Uri) -> impl IntoResponse {
    ResponseWithMetadata::new()
        .with_client_id(&client_id)
        .with_endpoint("404_fallback")
        .with_response(|| text!(StatusCode::NOT_FOUND, "Not found: {}", uri.path()))
}
