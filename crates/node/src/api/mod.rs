use std::collections::HashMap;
use std::sync::Arc;

use axum::{Json, Router};
use serde_json::{json, Value as JsonValue};

use error::ApiError;
use sqd_storage::db::{Database, DatasetId};

use crate::config::{Config, DatasetConfig};


mod error;


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

        let response = json!({
            "id": info.id.as_str(),
            "kind": info.options.kind.as_str()
        });

        Ok(Json(response))
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

        Router::new()
            .route("/:id/status", get(get_status))
            .with_state(self.clone())
    }
}