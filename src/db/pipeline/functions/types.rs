use crate::{FirestoreBoolExpr, FirestoreExpr};
use serde::Serialize;

/// A Firestore value type, as `is_type` tests for it.
///
/// `Number` stands for `Int32`, `Int64`, `Float64` and `Decimal128` together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FirestoreValueType {
    Null,
    Array,
    Boolean,
    Bytes,
    Timestamp,
    GeoPoint,
    Number,
    Int32,
    Int64,
    Float64,
    Decimal128,
    Map,
    Reference,
    String,
    Vector,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    BsonTimestamp,
}

impl FirestoreExpr {
    /// The name of this value's type, such as `int64`, as a string. The service's `type`, which
    /// is a Rust keyword.
    pub fn type_of(self) -> FirestoreExpr {
        Self::raw("type", [self])
    }

    /// True when this value is of the type `value_type`.
    pub fn is_type(self, value_type: FirestoreValueType) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("is_type", [self, value_type.into()]))
    }
}
