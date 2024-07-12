use axum::response::{IntoResponse, Response};
use axum::http::StatusCode;


pub enum ApiError {
    Internal(anyhow::Error),
    DatasetNotFound(String),
}


impl <'a> IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Internal(err) => {
                (
                    StatusCode::INTERNAL_SERVER_ERROR, 
                    err.to_string()
                ).into_response()
            },
            ApiError::DatasetNotFound(dataset_id) => {
                (
                    StatusCode::NOT_FOUND, 
                    format!("unknown dataset - {}", dataset_id)
                ).into_response()
            }
        }
    }
}


impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError::Internal(err)
    }
}
