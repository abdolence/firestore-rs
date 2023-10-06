use crate::FirestoreQueryFilter;
use crate::*;

pub struct FirestoreCacheFilterEngine<'a> {
    filter: &'a FirestoreQueryFilter,
}

impl<'a> FirestoreCacheFilterEngine<'a> {
    pub fn new(filter: &'a FirestoreQueryFilter) -> Self {
        Self { filter }
    }

    pub fn matches_doc(&'a self, doc: &FirestoreDocument) -> bool {
        Self::matches_doc_filter(doc, &self.filter)
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
                        _ => true,
                    })
                    .unwrap_or(true)
            }
            FirestoreQueryFilterUnary::IsNull(field_path) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .map(|field_value| match field_value {
                        gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(_) => true,
                        _ => false,
                    })
                    .unwrap_or(true)
            }
            FirestoreQueryFilterUnary::IsNotNull(field_path) => {
                firestore_doc_get_field_by_path(doc, field_path)
                    .map(|field_value| match field_value {
                        gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(_) => false,
                        _ => true,
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

enum CompareOp {
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

fn compare_values(
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
        ) => a_val.seconds < b_val.seconds && a_val.nanos < b_val.nanos,

        (
            CompareOp::LessThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => a_val.seconds <= b_val.seconds && a_val.nanos <= b_val.nanos,

        (
            CompareOp::GreaterThan,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => a_val.seconds > b_val.seconds && a_val.nanos > b_val.nanos,

        (
            CompareOp::GreaterThanOrEqual,
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(b_val),
        ) => a_val.seconds >= b_val.seconds && a_val.nanos >= b_val.nanos,

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

        //  Array Operation
        (
            CompareOp::ArrayContains,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(b_val),
        ) => a_val
            .values
            .iter()
            .map(|v| &v.value_type)
            .flatten()
            .any(|a_val| {
                b_val
                    .values
                    .iter()
                    .map(|v| &v.value_type)
                    .flatten()
                    .all(|b_val| compare_values(CompareOp::Equal, a_val, b_val))
            }),

        (
            CompareOp::ArrayContainsAny,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(a_val),
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(b_val),
        ) => a_val
            .values
            .iter()
            .map(|v| &v.value_type)
            .flatten()
            .any(|a_val| {
                b_val
                    .values
                    .iter()
                    .map(|v| &v.value_type)
                    .flatten()
                    .any(|b_val| compare_values(CompareOp::Equal, a_val, b_val))
            }),

        (
            CompareOp::In,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(a_val),
            b_val,
        ) => a_val
            .values
            .iter()
            .map(|v| &v.value_type)
            .flatten()
            .any(|a_val| compare_values(CompareOp::Equal, a_val, b_val)),

        (
            CompareOp::NotIn,
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(a_val),
            b_val,
        ) => a_val
            .values
            .iter()
            .map(|v| &v.value_type)
            .flatten()
            .any(|a_val| !compare_values(CompareOp::Equal, a_val, b_val)),

        // Any other combinations result in false
        _ => false,
    }
}
