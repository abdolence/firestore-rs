use crate::FirestoreQueryFilter;
use crate::*;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::{ArrayValue, MapValue, Value};
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

    /// The fields the filter compares with an inequality, which Firestore orders a query on when
    /// it names no ordering of its own.
    pub fn inequality_fields(filter: &FirestoreQueryFilter) -> Vec<&str> {
        match filter {
            FirestoreQueryFilter::Composite(composite) => composite
                .for_all_filters
                .iter()
                .flat_map(Self::inequality_fields)
                .collect(),
            FirestoreQueryFilter::Unary(
                FirestoreQueryFilterUnary::IsNotNan(field)
                | FirestoreQueryFilterUnary::IsNotNull(field),
            )
            | FirestoreQueryFilter::Compare(Some(
                FirestoreQueryFilterCompare::LessThan(field, _)
                | FirestoreQueryFilterCompare::LessThanOrEqual(field, _)
                | FirestoreQueryFilterCompare::GreaterThan(field, _)
                | FirestoreQueryFilterCompare::GreaterThanOrEqual(field, _)
                | FirestoreQueryFilterCompare::NotEqual(field, _)
                | FirestoreQueryFilterCompare::NotIn(field, _),
            )) => vec![field],
            FirestoreQueryFilter::Unary(_) | FirestoreQueryFilter::Compare(_) => vec![],
        }
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

#[derive(Clone, Copy)]
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
    pub(super) fn holds_for(&self, ordering: Ordering) -> bool {
        match self {
            Self::Equal => ordering.is_eq(),
            Self::NotEqual => ordering.is_ne(),
            Self::LessThan => ordering.is_lt(),
            Self::LessThanOrEqual => ordering.is_le(),
            Self::GreaterThan => ordering.is_gt(),
            Self::GreaterThanOrEqual => ordering.is_ge(),
            Self::ArrayContains | Self::ArrayContainsAny | Self::In | Self::NotIn => false,
        }
    }

    pub(super) fn is_range(&self) -> bool {
        matches!(
            self,
            Self::LessThan | Self::LessThanOrEqual | Self::GreaterThan | Self::GreaterThanOrEqual
        )
    }
}

/// The types of a value in the order Firestore sorts them. NaN is a number, the lowest one.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ValueTypeRank {
    Null,
    Boolean,
    Number,
    Timestamp,
    String,
    Bytes,
    Reference,
    GeoPoint,
    Array,
    Vector,
    Map,
}

impl From<&ValueType> for ValueTypeRank {
    fn from(value: &ValueType) -> Self {
        match value {
            ValueType::NullValue(_) => Self::Null,
            ValueType::BooleanValue(_) => Self::Boolean,
            ValueType::IntegerValue(_) | ValueType::DoubleValue(_) => Self::Number,
            ValueType::TimestampValue(_) => Self::Timestamp,
            ValueType::StringValue(_) => Self::String,
            ValueType::BytesValue(_) => Self::Bytes,
            ValueType::ReferenceValue(_) => Self::Reference,
            ValueType::GeoPointValue(_) => Self::GeoPoint,
            ValueType::ArrayValue(_) => Self::Array,
            ValueType::MapValue(map) if is_vector(map) => Self::Vector,
            // Pipeline expressions: a stored document never holds one, so their place is
            // arbitrary.
            ValueType::MapValue(_)
            | ValueType::FieldReferenceValue(_)
            | ValueType::VariableReferenceValue(_)
            | ValueType::FunctionValue(_)
            | ValueType::PipelineValue(_) => Self::Map,
        }
    }
}

pub(super) fn is_nan(value: &ValueType) -> bool {
    matches!(value, ValueType::DoubleValue(double) if double.is_nan())
}

/// A vector is a map with a marker field; the components are in its `value` field.
fn is_vector(map: &MapValue) -> bool {
    matches!(
        map.fields.get("__type__").and_then(|marker| marker.value_type.as_ref()),
        Some(ValueType::StringValue(marker)) if marker == "__vector__"
    )
}

fn vector_components(map: &MapValue) -> &[Value] {
    match map
        .fields
        .get("value")
        .and_then(|value| value.value_type.as_ref())
    {
        Some(ValueType::ArrayValue(array)) => &array.values,
        _ => &[],
    }
}

/// The order of two doubles, with NaN equal to itself and below every other double, and `-0.0`
/// equal to `0.0`.
fn compare_doubles(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b)
        .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan()).reverse())
}

// Exact rather than through `f64`: an integer above 2^53 does not survive the conversion.
fn compare_integer_to_double(integer: i64, double: f64) -> Ordering {
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if double.is_nan() {
        Ordering::Greater
    } else if double >= TWO_POW_63 {
        Ordering::Less
    } else if double < -TWO_POW_63 {
        Ordering::Greater
    } else {
        // In range, so the truncation is exact; a tie there is decided by the fraction.
        let fraction = double.fract();
        integer
            .cmp(&(double.trunc() as i64))
            .then(if fraction > 0.0 {
                Ordering::Less
            } else if fraction < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            })
    }
}

fn compare_nested(a: &Value, b: &Value) -> Ordering {
    match (&a.value_type, &b.value_type) {
        (Some(a), Some(b)) => value_order(a, b),
        (a, b) => a.is_some().cmp(&b.is_some()),
    }
}

fn compare_elements(a: &[Value], b: &[Value]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| compare_nested(a, b))
        .find(|ordering| ordering.is_ne())
        .unwrap_or(Ordering::Equal)
}

fn compare_maps(a: &MapValue, b: &MapValue) -> Ordering {
    fn sorted(map: &MapValue) -> Vec<(&String, &Value)> {
        let mut entries: Vec<_> = map.fields.iter().collect();
        entries.sort_by_key(|(key, _)| *key);
        entries
    }
    let (a_entries, b_entries) = (sorted(a), sorted(b));
    a_entries
        .iter()
        .zip(&b_entries)
        .map(|((a_key, a_value), (b_key, b_value))| {
            a_key
                .cmp(b_key)
                .then_with(|| compare_nested(a_value, b_value))
        })
        .find(|ordering| ordering.is_ne())
        .unwrap_or_else(|| a_entries.len().cmp(&b_entries.len()))
}

/// Firestore's total order over values: by type first, then within the type: numbers by value,
/// strings by their UTF-8 bytes, references by path segment, geo points by latitude then
/// longitude, arrays by element and then length, vectors by length and then component, maps by
/// their key-sorted entries and then size.
pub(crate) fn value_order(a: &ValueType, b: &ValueType) -> Ordering {
    match (a, b) {
        (ValueType::NullValue(_), ValueType::NullValue(_)) => Ordering::Equal,
        (ValueType::BooleanValue(a), ValueType::BooleanValue(b)) => a.cmp(b),
        (ValueType::IntegerValue(a), ValueType::IntegerValue(b)) => a.cmp(b),
        (ValueType::DoubleValue(a), ValueType::DoubleValue(b)) => compare_doubles(*a, *b),
        (ValueType::IntegerValue(integer), ValueType::DoubleValue(double)) => {
            compare_integer_to_double(*integer, *double)
        }
        (ValueType::DoubleValue(double), ValueType::IntegerValue(integer)) => {
            compare_integer_to_double(*integer, *double).reverse()
        }
        (ValueType::TimestampValue(a), ValueType::TimestampValue(b)) => {
            (a.seconds, a.nanos).cmp(&(b.seconds, b.nanos))
        }
        (ValueType::StringValue(a), ValueType::StringValue(b)) => a.cmp(b),
        (ValueType::BytesValue(a), ValueType::BytesValue(b)) => a.cmp(b),
        (ValueType::ReferenceValue(a), ValueType::ReferenceValue(b)) => {
            a.split('/').cmp(b.split('/'))
        }
        (ValueType::GeoPointValue(a), ValueType::GeoPointValue(b)) => {
            compare_doubles(a.latitude, b.latitude)
                .then_with(|| compare_doubles(a.longitude, b.longitude))
        }
        (ValueType::ArrayValue(a), ValueType::ArrayValue(b)) => {
            compare_elements(&a.values, &b.values).then_with(|| a.values.len().cmp(&b.values.len()))
        }
        (ValueType::MapValue(a), ValueType::MapValue(b)) if is_vector(a) && is_vector(b) => {
            let (a, b) = (vector_components(a), vector_components(b));
            a.len().cmp(&b.len()).then_with(|| compare_elements(a, b))
        }
        (ValueType::MapValue(a), ValueType::MapValue(b)) if !is_vector(a) && !is_vector(b) => {
            compare_maps(a, b)
        }
        _ => ValueTypeRank::from(a).cmp(&ValueTypeRank::from(b)),
    }
}

/// Whether a filter of `op` on a field holding `a` matches `b`. A field that is null matches no
/// comparison, and neither does a null `b`: nulls are queried with the `IsNull` filter.
pub(super) fn compare_values(op: CompareOp, a: &ValueType, b: &ValueType) -> bool {
    let contains = |items: &ArrayValue, value: &ValueType| {
        items
            .values
            .iter()
            .flat_map(|item| &item.value_type)
            .any(|item| compare_values(CompareOp::Equal, item, value))
    };

    match (op, a, b) {
        (CompareOp::ArrayContains, ValueType::ArrayValue(items), value) => contains(items, value),

        (CompareOp::ArrayContainsAny, ValueType::ArrayValue(items), ValueType::ArrayValue(any)) => {
            any.values
                .iter()
                .flat_map(|value| &value.value_type)
                .any(|value| contains(items, value))
        }

        (CompareOp::In, value, ValueType::ArrayValue(listed)) => contains(listed, value),

        // Firestore returns nothing for a `not-in` list that holds a null.
        (CompareOp::NotIn, _, ValueType::ArrayValue(listed))
            if listed
                .values
                .iter()
                .any(|value| matches!(value.value_type, Some(ValueType::NullValue(_)))) =>
        {
            false
        }

        // Firestore leaves out documents whose field is null, as it does those without the field.
        (CompareOp::NotIn, ValueType::NullValue(_), ValueType::ArrayValue(_)) => false,

        (CompareOp::NotIn, value, ValueType::ArrayValue(listed)) => !contains(listed, value),

        (_, ValueType::NullValue(_), _) | (_, _, ValueType::NullValue(_)) => false,

        // NaN is ordered for sorting, but a filter treats it as equal to nothing, itself included,
        // and as in no range.
        (op, a, b) if is_nan(a) || is_nan(b) => matches!(op, CompareOp::NotEqual),

        // A range filter matches only values of the type it compares with.
        (op, a, b) if op.is_range() && ValueTypeRank::from(a) != ValueTypeRank::from(b) => false,

        (op, a, b) => op.holds_for(value_order(a, b)),
    }
}
