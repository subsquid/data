use std::fmt::{Display, Formatter};
pub use super::query::user_error::*;
pub use sqd_query::UnexpectedBaseBlock;


#[derive(Debug)]
pub struct Busy;


impl Display for Busy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "node is busy")
    }
}


impl std::error::Error for Busy {}