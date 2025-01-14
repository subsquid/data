use std::fmt::{Display, Formatter};


#[derive(Debug)]
pub struct QueryKindMismatch {
    pub query_kind: sqd_storage::db::DatasetKind,
    pub dataset_kind: sqd_storage::db::DatasetKind
}


impl Display for QueryKindMismatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} query was issued against {} dataset", self.query_kind, self.dataset_kind)
    }
}


impl std::error::Error for QueryKindMismatch {}