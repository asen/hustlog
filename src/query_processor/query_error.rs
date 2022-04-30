use std::error::Error;
use std::fmt;

use sqlparser::ast::BinaryOperator;

use crate::ParsedValue;

#[derive(Debug, Clone)]
pub struct QueryError(String);

impl QueryError {
    pub fn new(s: &str) -> QueryError {
        QueryError(s.to_string())
    }

    pub fn not_supported(what: &str) -> QueryError {
        QueryError(format!("Feature not supported {}", what))
    }

    pub fn not_impl(what: &str) -> QueryError {
        QueryError(format!("Feature not implemented yet {}", what))
    }

    pub fn unexpected(what: &str) -> QueryError {
        QueryError(format!("Unexpected error: {}", what))
    }

    pub fn incompatible_types(
        ltype: &ParsedValue,
        rtype: &ParsedValue,
        op: &BinaryOperator,
    ) -> QueryError {
        QueryError(format!(
            "Incompatible types for op: {:?} lval={:?} rval={:?}",
            op, ltype, rtype
        ))
    }
}
impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Query error: {}", self.0)
    }
}

impl Error for QueryError {}
