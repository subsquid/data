use axum::response::{IntoResponse, Response};
use axum::http::StatusCode;


pub enum ApiError {
    Internal(anyhow::Error),
    DatasetNotFound(String),
    UserError(String)
}


impl <'a> IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Internal(err) => {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("{:?}", err)
                ).into_response()
            },
            ApiError::DatasetNotFound(dataset_id) => {
                (
                    StatusCode::NOT_FOUND,
                    format!("unknown dataset - {}", dataset_id)
                ).into_response()
            },
            ApiError::UserError(msg) => {
                (
                    StatusCode::BAD_REQUEST,
                    msg
                ).into_response()
            }
        }
    }
}


impl <E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(err: E) -> Self {
        ApiError::Internal(err.into())
    }
}