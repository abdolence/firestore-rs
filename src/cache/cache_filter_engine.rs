use crate::FirestoreQueryFilter;
use crate::*;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::Value;
use std::cmp::Ordering;

pub struct FirestoreCacheFilterEngine<'a> {
    filter: &'a FirestoreQueryFilter,
}

impl<'a> FirestoreCacheFilterEngine<'a> {
    pub fn new(filter: &'a FirestoreQueryFilter) -> Self {
        Self { filter }
    }

    pub fn matches_doc(&'a self, doc: &FirestoreDocument) -> bool {
        Self::matches_doc_filter(doc, self.filter)
    }

    pub fn matches_doc_filter(doc: &FirestoreDocument, filter: &FirestoreQueryFilter) -> bool {
        match filter {
            FirestoreQueryFilter::Composite(composite_filter) => match composite_filter.operator {
                FirestoreQueryFilterCompositeOperator::And => composite_filter
                    .for_all_filters
                    .iter()
                    .all(|filter| Self::matches_doc_filter(doc, filter)),

                FirestoreQueryFilterCompositeOperator::Or => composite_filter
                    .for_all_filters
                    .iter()
                    .any(|filter| Self::matches_doc_filter(doc, filter)),
            },
            FirestoreQueryFilter::Unary(unary_filter) => {
                Self::matches_doc_filter_unary(doc, unary_filter)
            }
            FirestoreQueryFilter::Compare(compare_filter) => {
                Self::matches_doc_filter_compare(doc, compare_filter)
            }
        }
    }

    pub fn matches_doc_filter_unary(
        doc: &FirestoreDocument,
        filter: &FirestoreQueryFilterUnary,
    ) -> bool {
        match filter {
            FirestoreQueryFilterUnary::IsNan(field_path) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .map(|field_value| match field_value {
                        gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(
                            double_value,
                        ) => double_value.is_nan(),
                        _ => false,
                    })
                    .unwrap_or(false)
            }
            FirestoreQueryFilterUnary::IsNotNan(field_path) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .map(|field_value| match field_value {
                        gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(
                            double_value,
                        ) => !double_value.is_nan(),
                        gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(_) => false,
                        _ => true,
                    })
                    .unwrap_or(false)
            }
            FirestoreQueryFilterUnary::IsNull(field_path) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .map(|field_value| {
                        matches!(
                            field_value,
                            gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(_)
                        )
                    })
                    .unwrap_or(false)
            }
            FirestoreQueryFilterUnary::IsNotNull(field_path) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .map(|field_value| {
                        !matches!(
                            field_value,
                            gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(_)
                        )
                    })
                    .unwrap_or(false)
            }
        }
    }

    pub fn matches_doc_filter_compare(
        doc: &FirestoreDocument,
        filter: &Option<FirestoreQueryFilterCompare>,
    ) -> bool {
        match filter {
            Some(FirestoreQueryFilterCompare::Equal(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(CompareOp::Equal, field_value, compare_with_value)
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::LessThan(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(CompareOp::LessThan, field_value, compare_with_value)
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::LessThanOrEqual(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(
                                    CompareOp::LessThanOrEqual,
                                    field_value,
                                    compare_with_value,
                                )
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::GreaterThan(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(
                                    CompareOp::GreaterThan,
                                    field_value,
                                    compare_with_value,
                                )
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::GreaterThanOrEqual(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(
                                    CompareOp::GreaterThanOrEqual,
                                    field_value,
                                    compare_with_value,
                                )
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::NotEqual(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(CompareOp::NotEqual, field_value, compare_with_value)
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::ArrayContains(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(
                                    CompareOp::ArrayContains,
                                    field_value,
                                    compare_with_value,
                                )
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::In(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(CompareOp::In, field_value, compare_with_value)
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::ArrayContainsAny(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(
                                    CompareOp::ArrayContainsAny,
                                    field_value,
                                    compare_with_value,
                                )
                            })
                    })
                    .unwrap_or(false)
            }
            Some(FirestoreQueryFilterCompare::NotIn(field_path, compare_with)) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .and_then(|field_value| {
                        compare_with
                            .value
                            .value_type
                            .as_ref()
                            .map(|compare_with_value| {
                                compare_values(CompareOp::NotIn, field_value, compare_with_value)
                            })
                    })
                    .unwrap_or(false)
            }
            None => true,
        }
    }
}

pub(super) enum CompareOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    ArrayContains,
    ArrayContainsAny,
    In,
    NotIn,
}

impl CompareOp {
    /// Whether a comparison operator holds for the outcome of comparing two values; `None` is
    /// values with no order, as with NaN.
    fn holds_for(&self, ordering: Option<Ordering>) -> bool {
        match self {
            Self::Equal => ordering == Some(Ordering::Equal),
            Self::NotEqual => ordering != Some(Ordering::Equal),
            Self::LessThan => ordering == Some(Ordering::Less),
            Self::LessThanOrEqual => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
            Self::GreaterThan => ordering == Some(Ordering::Greater),
            Self::GreaterThanOrEqual => {
                matches!(ordering, Some(Ordering::Greater | Ordering::Equal))
            }
            Self::ArrayContains | Self::ArrayContainsAny | Self::In | Self::NotIn => false,
        }
    }
}

/// Compares an integer with a double by value, as Firestore does: the integer is not rounded to
/// a double first, so integers above 2^53 keep their exact order against doubles.
fn compare_integer_to_double(integer: i64, double: f64) -> Option<Ordering> {
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if double.is_nan() {
        None
    } else if double >= TWO_POW_63 {
        Some(Ordering::Less)
    } else if double < -TWO_POW_63 {
        Some(Ordering::Greater)
    } else {
        // In range, so the truncation is exact; a tie there is decided by the fraction.
        let fraction = double.fract();
        Some(
            integer
                .cmp(&(double.trunc() as i64))
                .then(if fraction > 0.0 {
                    Ordering::Less
                } else if fraction < 0.0 {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }),
        )
    }
}

/// Equality of two values as Firestore applies it inside arrays and maps, where, unlike for a
/// filter on a field, null equals null and NaN equals NaN.
fn nested_values_equal(a: &ValueType, b: &ValueType) -> bool {
    match (a, b) {
        (ValueType::NullValue(_), ValueType::NullValue(_)) => true,
        (ValueType::DoubleValue(a_val), ValueType::DoubleValue(b_val))
            if a_val.is_nan() && b_val.is_nan() =>
        {
            true
        }
        (ValueType::ArrayValue(a_val), ValueType::ArrayValue(b_val)) => {
            a_val.values.len() == b_val.values.len()
                && a_val
                    .values
                    .iter()
                    .zip(&b_val.values)
                    .all(|(a_item, b_item)| nested_items_equal(a_item, b_item))
        }
        (ValueType::MapValue(a_val), ValueType::MapValue(b_val)) => {
            a_val.fields.len() == b_val.fields.len()
                && a_val.fields.iter().all(|(key, a_item)| {
                    b_val
                        .fields
                        .get(key)
                        .is_some_and(|b_item| nested_items_equal(a_item, b_item))
                })
        }
        _ => compare_values(CompareOp::Equal, a, b),
    }
}

fn nested_items_equal(a: &Value, b: &Value) -> bool {
    matches!(
        (&a.value_type, &b.value_type),
        (Some(a_val), Some(b_val)) if nested_values_equal(a_val, b_val)
    )
}

pub(super) fn compare_values(
    op: CompareOp,
    a: &gcloud_sdk::google::firestore::v1::value::ValueType,
    b: &gcloud_sdk::google::firestore::v1::value::ValueType,
) -> bool {
    match (op, a, b) {
        // handle BooleanValue
        (
            CompareOp::Equal,
            gcloud_sdk::google::firestore::v1::value::ValueType::BooleanValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::BooleanValue(b_val),
        ) => a_val == b_val,

        (
            CompareOp::NotEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::BooleanValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::BooleanValue(b_val),
        ) => a_val != b_val,

        // handle IntegerValue
        (
            CompareOp::Equal,
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(b_val),
        ) => a_val == b_val,

        (
            CompareOp::NotEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(b_val),
        ) => a_val != b_val,

        (
            CompareOp::LessThan,
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(b_val),
        ) => a_val < b_val,

        (
            CompareOp::LessThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(b_val),
        ) => a_val <= b_val,

        (
            CompareOp::GreaterThan,
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(b_val),
        ) => a_val > b_val,

        (
            CompareOp::GreaterThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(b_val),
        ) => a_val >= b_val,

        // For DoubleValue
        (
            CompareOp::Equal,
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(b_val),
        ) => a_val == b_val,

        (
            CompareOp::NotEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(b_val),
        ) => a_val != b_val,

        (
            CompareOp::LessThan,
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(b_val),
        ) => a_val < b_val,

        (
            CompareOp::LessThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(b_val),
        ) => a_val <= b_val,

        (
            CompareOp::GreaterThan,
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(b_val),
        ) => a_val > b_val,

        (
            CompareOp::GreaterThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(b_val),
        ) => a_val >= b_val,

        // For TimestampValue, assumes it's a numerical timestamp; if it's a string or date type, adjust accordingly
        (
            CompareOp::Equal,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => a_val == b_val,

        (
            CompareOp::NotEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => a_val != b_val,

        (
            CompareOp::LessThan,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => (a_val.seconds, a_val.nanos) < (b_val.seconds, b_val.nanos),

        (
            CompareOp::LessThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => (a_val.seconds, a_val.nanos) <= (b_val.seconds, b_val.nanos),

        (
            CompareOp::GreaterThan,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => (a_val.seconds, a_val.nanos) > (b_val.seconds, b_val.nanos),

        (
            CompareOp::GreaterThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => (a_val.seconds, a_val.nanos) >= (b_val.seconds, b_val.nanos),

        // For StringType only Equal, NotEqual operations make sense in a general context
        (
            CompareOp::Equal,
            gcloud_sdk::google::firestore::v1::value::ValueType::StringValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::StringValue(b_val),
        ) => a_val == b_val,

        (
            CompareOp::NotEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::StringValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::StringValue(b_val),
        ) => a_val != b_val,

        (CompareOp::Equal, a_val @ ValueType::ArrayValue(_), b_val @ ValueType::ArrayValue(_)) => {
            nested_values_equal(a_val, b_val)
        }

        (CompareOp::Equal, a_val @ ValueType::MapValue(_), b_val @ ValueType::MapValue(_)) => {
            nested_values_equal(a_val, b_val)
        }

        (CompareOp::Equal, ValueType::BytesValue(a_val), ValueType::BytesValue(b_val)) => {
            a_val == b_val
        }

        (CompareOp::Equal, ValueType::ReferenceValue(a_val), ValueType::ReferenceValue(b_val)) => {
            a_val == b_val
        }

        (CompareOp::Equal, ValueType::GeoPointValue(a_val), ValueType::GeoPointValue(b_val)) => {
            a_val == b_val
        }

        //  Array Operation
        (
            CompareOp::ArrayContains,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(a_val),
            b_val,
        ) => a_val
            .values
            .iter()
            .flat_map(|v| &v.value_type)
            .any(|a_val| compare_values(CompareOp::Equal, a_val, b_val)),

        (
            CompareOp::ArrayContainsAny,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(b_val),
        ) => a_val
            .values
            .iter()
            .flat_map(|v| &v.value_type)
            .any(|a_val| {
                b_val
                    .values
                    .iter()
                    .flat_map(|v| &v.value_type)
                    .any(|b_val| compare_values(CompareOp::Equal, a_val, b_val))
            }),

        (
            CompareOp::In,
            a_val,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(b_val),
        ) => b_val
            .values
            .iter()
            .flat_map(|v| &v.value_type)
            .any(|b_val| compare_values(CompareOp::Equal, a_val, b_val)),

        // Firestore returns nothing for a `not-in` list that holds a null.
        (CompareOp::NotIn, _, ValueType::ArrayValue(b_val))
            if b_val
                .values
                .iter()
                .any(|v| matches!(v.value_type, Some(ValueType::NullValue(_)))) =>
        {
            false
        }

        // Firestore leaves out documents whose field is null, as it does those without the field.
        (
            CompareOp::NotIn,
            gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(_),
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(_),
        ) => false,

        (
            CompareOp::NotIn,
            a_val,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(b_val),
        ) => !b_val
            .values
            .iter()
            .flat_map(|v| &v.value_type)
            .any(|b_val| compare_values(CompareOp::Equal, a_val, b_val)),

        // Integers and doubles are one number type to Firestore: `1` equals `1.0`.
        (
            op,
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(integer),
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(double),
        ) => op.holds_for(compare_integer_to_double(*integer, *double)),

        (
            op,
            gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(double),
            gcloud_sdk::google::firestore::v1::value::ValueType::IntegerValue(integer),
        ) => op.holds_for(compare_integer_to_double(*integer, *double).map(Ordering::reverse)),

        // Whatever its type, an existing field that is not null matches `!=` when it differs from
        // a value that is not null.
        (CompareOp::NotEqual, a_val, b_val) => {
            !matches!(a_val, ValueType::NullValue(_))
                && !matches!(b_val, ValueType::NullValue(_))
                && !compare_values(CompareOp::Equal, a_val, b_val)
        }

        // Any other combinations result in false
        _ => false,
    }
}
