extern crate csv;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

// PG strip skips these files; output only when hs (or later) in list
pub use query::logical_expr;
pub use query::physical_expr;
pub mod attribute;
pub use attribute::Attribute;
pub use attribute::Constraint;
pub mod catalog;
pub mod commands;
pub mod error;
pub mod ids;
pub mod datatypes;
pub mod physical;
pub mod rwlatch;
pub mod table;
pub use table::TableSchema;
pub mod traits;
pub mod tuple;
pub use tuple::Tuple;
pub mod query;
pub mod util;
pub use util::common_test_util as testutil;

/// Page size in bytes
pub const PAGE_SIZE: usize = 4096;

// How many pages a buffer pool can hold
pub const PAGE_SLOTS: usize = 50;
// Maximum number of columns in a table
pub const MAX_COLUMNS: usize = 100;

// dir name of manager table
pub const MANAGERS_DIR_NAME: &str = "managers";

// dir name of manager table
pub const QUERY_CACHES_DIR_NAME: &str = "query_caches";

pub mod prelude {
    pub use crate::error::CrustyError;
    pub use crate::ids::Permissions;
    pub use crate::ids::{
        ColumnId, ContainerId, LogicalTimeStamp, Lsn, PageId, SlotId, StateType, TidType,
        TransactionId, ValueId,
    };

    pub use crate::datatypes::{DataType, Field};
    pub use crate::table::TableInfo;
    pub use crate::{table::TableSchema, tuple::Tuple};
}

pub use crate::error::{ConversionError, CrustyError};

pub use crate::datatypes::{DataType, Field};
pub use query::query_result::QueryResult;
pub use crate::query::operation::{AggOp, BinaryOp};

#[cfg(test)]
mod libtests {
    use super::*;
    use crate::{attribute::Attribute, testutil::*, tuple::Tuple};

    #[test]
    fn test_tuple_bytes() {
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple_bytes = tuple.to_bytes();
        let check_tuple: Tuple = Tuple::from_bytes(&tuple_bytes);
        assert_eq!(tuple, check_tuple);
    }

    #[test]
    fn test_decimal_field() {
        let d1 = "13.2";
        let dfield = Field::Decimal(132, 1);
        let dtype = DataType::Decimal(3, 1);
        let attr = Attribute::new("dec field".to_string(), dtype);
        let df = Field::from_str(d1, &attr).unwrap();
        assert_eq!(dfield, df);
        assert_eq!(df.to_string(), d1);
    }
}
