use crate::{FirestoreBoolExpr, FirestoreExpr};

/// A Firestore value type, as `is_type` tests for it.
///
/// `Number` stands for `Int32`, `Int64`, `Float64` and `Decimal128` together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl From<FirestoreValueType> for FirestoreExpr {
    fn from(value: FirestoreValueType) -> Self {
        FirestoreExpr::constant(match value {
            FirestoreValueType::Null => "null",
            FirestoreValueType::Array => "array",
            FirestoreValueType::Boolean => "boolean",
            FirestoreValueType::Bytes => "bytes",
            FirestoreValueType::Timestamp => "timestamp",
            FirestoreValueType::GeoPoint => "geo_point",
            FirestoreValueType::Number => "number",
            FirestoreValueType::Int32 => "int32",
            FirestoreValueType::Int64 => "int64",
            FirestoreValueType::Float64 => "float64",
            FirestoreValueType::Decimal128 => "decimal128",
            FirestoreValueType::Map => "map",
            FirestoreValueType::Reference => "reference",
            FirestoreValueType::String => "string",
            FirestoreValueType::Vector => "vector",
            FirestoreValueType::MaxKey => "max_key",
            FirestoreValueType::MinKey => "min_key",
            FirestoreValueType::ObjectId => "object_id",
            FirestoreValueType::Regex => "regex",
            FirestoreValueType::BsonTimestamp => "bson_timestamp",
        })
    }
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
