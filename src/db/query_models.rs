#![allow(clippy::derive_partial_eq_without_eq)] // Since we may not be able to implement Eq for the changes coming from Firestore protos

use crate::FirestoreValue;
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::Builder;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryCollection {
    Single(String),
    Group(Vec<String>),
}

impl ToString for FirestoreQueryCollection {
    fn to_string(&self) -> String {
        match self {
            FirestoreQueryCollection::Single(single) => single.to_string(),
            FirestoreQueryCollection::Group(group) => group.join(","),
        }
    }
}

impl From<&str> for FirestoreQueryCollection {
    fn from(collection_id_str: &str) -> Self {
        FirestoreQueryCollection::Single(collection_id_str.to_string())
    }
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreQueryParams {
    pub parent: Option<String>,
    pub collection_id: FirestoreQueryCollection,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub order_by: Option<Vec<FirestoreQueryOrder>>,
    pub filter: Option<FirestoreQueryFilter>,
    pub all_descendants: Option<bool>,
    pub return_only_fields: Option<Vec<String>>,
    pub start_at: Option<FirestoreQueryCursor>,
    pub end_at: Option<FirestoreQueryCursor>,
}

impl From<FirestoreQueryParams> for StructuredQuery {
    fn from(params: FirestoreQueryParams) -> Self {
        let query_filter = params.filter.map(|f| f.into());

        StructuredQuery {
            select: params.return_only_fields.map(|select_only_fields| {
                structured_query::Projection {
                    fields: select_only_fields
                        .into_iter()
                        .map(|field_name| structured_query::FieldReference {
                            field_path: field_name,
                        })
                        .collect(),
                }
            }),
            start_at: params.start_at.map(|start_at| start_at.into()),
            end_at: params.end_at.map(|end_at| end_at.into()),
            limit: params.limit.map(|x| x as i32),
            offset: params.offset.map(|x| x as i32).unwrap_or(0),
            order_by: params
                .order_by
                .map(|po| po.into_iter().map(|fo| fo.into()).collect())
                .unwrap_or_default(),
            from: match params.collection_id {
                FirestoreQueryCollection::Single(collection_id) => {
                    vec![structured_query::CollectionSelector {
                        collection_id,
                        all_descendants: params.all_descendants.unwrap_or(false),
                    }]
                }
                FirestoreQueryCollection::Group(collection_ids) => collection_ids
                    .into_iter()
                    .map(|collection_id| structured_query::CollectionSelector {
                        collection_id,
                        all_descendants: params.all_descendants.unwrap_or(false),
                    })
                    .collect(),
            },
            r#where: query_filter,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreQueryFilter {
    Composite(FirestoreQueryFilterComposite),
    Unary(FirestoreQueryFilterUnary),
    Compare(Option<FirestoreQueryFilterCompare>),
}

impl From<FirestoreQueryFilter> for structured_query::Filter {
    fn from(filter: FirestoreQueryFilter) -> Self {
        let filter_type = match filter {
            FirestoreQueryFilter::Compare(comp) => comp.map(|cmp| {
                structured_query::filter::FilterType::FieldFilter(match cmp {
                    FirestoreQueryFilterCompare::Equal(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::Equal.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::NotEqual(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::NotEqual.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::In(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::In.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::NotIn(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::NotIn.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::ArrayContains(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::ArrayContains.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::ArrayContainsAny(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::ArrayContainsAny.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::LessThan(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::LessThan.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::LessThanOrEqual(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::LessThanOrEqual.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::GreaterThan(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::GreaterThan.into(),
                            value: Some(fvalue.value),
                        }
                    }
                    FirestoreQueryFilterCompare::GreaterThanOrEqual(field_name, fvalue) => {
                        structured_query::FieldFilter {
                            field: Some(structured_query::FieldReference {
                                field_path: field_name,
                            }),
                            op: structured_query::field_filter::Operator::GreaterThanOrEqual.into(),
                            value: Some(fvalue.value),
                        }
                    }
                })
            }),
            FirestoreQueryFilter::Composite(composite) => {
                Some(structured_query::filter::FilterType::CompositeFilter(
                    structured_query::CompositeFilter {
                        op: (Into::<structured_query::composite_filter::Operator>::into(
                            composite.operator,
                        ))
                        .into(),
                        filters: composite
                            .for_all_filters
                            .into_iter()
                            .map(structured_query::Filter::from)
                            .filter(|filter| filter.filter_type.is_some())
                            .collect(),
                    },
                ))
            }
            FirestoreQueryFilter::Unary(unary) => match unary {
                FirestoreQueryFilterUnary::IsNan(field_name) => {
                    Some(structured_query::filter::FilterType::UnaryFilter(
                        structured_query::UnaryFilter {
                            op: structured_query::unary_filter::Operator::IsNan.into(),
                            operand_type: Some(structured_query::unary_filter::OperandType::Field(
                                structured_query::FieldReference {
                                    field_path: field_name,
                                },
                            )),
                        },
                    ))
                }
                FirestoreQueryFilterUnary::IsNull(field_name) => {
                    Some(structured_query::filter::FilterType::UnaryFilter(
                        structured_query::UnaryFilter {
                            op: structured_query::unary_filter::Operator::IsNull.into(),
                            operand_type: Some(structured_query::unary_filter::OperandType::Field(
                                structured_query::FieldReference {
                                    field_path: field_name,
                                },
                            )),
                        },
                    ))
                }
                FirestoreQueryFilterUnary::IsNotNan(field_name) => {
                    Some(structured_query::filter::FilterType::UnaryFilter(
                        structured_query::UnaryFilter {
                            op: structured_query::unary_filter::Operator::IsNotNan.into(),
                            operand_type: Some(structured_query::unary_filter::OperandType::Field(
                                structured_query::FieldReference {
                                    field_path: field_name,
                                },
                            )),
                        },
                    ))
                }
                FirestoreQueryFilterUnary::IsNotNull(field_name) => {
                    Some(structured_query::filter::FilterType::UnaryFilter(
                        structured_query::UnaryFilter {
                            op: structured_query::unary_filter::Operator::IsNotNull.into(),
                            operand_type: Some(structured_query::unary_filter::OperandType::Field(
                                structured_query::FieldReference {
                                    field_path: field_name,
                                },
                            )),
                        },
                    ))
                }
            },
        };

        structured_query::Filter { filter_type }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreQueryOrder {
    pub field_name: String,
    pub direction: FirestoreQueryDirection,
}

impl FirestoreQueryOrder {
    pub fn to_string_format(&self) -> String {
        format!("{} {}", self.field_name, self.direction.to_string())
    }
}

impl<S> From<(S, FirestoreQueryDirection)> for FirestoreQueryOrder
where
    S: AsRef<str>,
{
    fn from(field_order: (S, FirestoreQueryDirection)) -> Self {
        FirestoreQueryOrder::new(field_order.0.as_ref().to_string(), field_order.1)
    }
}

impl From<FirestoreQueryOrder> for structured_query::Order {
    fn from(order: FirestoreQueryOrder) -> Self {
        structured_query::Order {
            field: Some(structured_query::FieldReference {
                field_path: order.field_name,
            }),
            direction: (match order.direction {
                FirestoreQueryDirection::Ascending => structured_query::Direction::Ascending.into(),
                FirestoreQueryDirection::Descending => {
                    structured_query::Direction::Descending.into()
                }
            }),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryDirection {
    Ascending,
    Descending,
}

impl ToString for FirestoreQueryDirection {
    fn to_string(&self) -> String {
        match self {
            FirestoreQueryDirection::Ascending => "asc".to_string(),
            FirestoreQueryDirection::Descending => "desc".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreQueryFilterComposite {
    pub for_all_filters: Vec<FirestoreQueryFilter>,
    pub operator: FirestoreQueryFilterCompositeOperator,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryFilterCompositeOperator {
    And,
    Or,
}

impl From<FirestoreQueryFilterCompositeOperator> for structured_query::composite_filter::Operator {
    fn from(operator: FirestoreQueryFilterCompositeOperator) -> Self {
        match operator {
            FirestoreQueryFilterCompositeOperator::And => {
                structured_query::composite_filter::Operator::And
            }
            FirestoreQueryFilterCompositeOperator::Or => {
                structured_query::composite_filter::Operator::Or
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryFilterUnary {
    IsNan(String),
    IsNull(String),
    IsNotNan(String),
    IsNotNull(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreQueryFilterCompare {
    LessThan(String, FirestoreValue),
    LessThanOrEqual(String, FirestoreValue),
    GreaterThan(String, FirestoreValue),
    GreaterThanOrEqual(String, FirestoreValue),
    Equal(String, FirestoreValue),
    NotEqual(String, FirestoreValue),
    ArrayContains(String, FirestoreValue),
    In(String, FirestoreValue),
    ArrayContainsAny(String, FirestoreValue),
    NotIn(String, FirestoreValue),
}

#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreQueryCursor {
    BeforeValue(Vec<FirestoreValue>),
    AfterValue(Vec<FirestoreValue>),
}

impl From<FirestoreQueryCursor> for gcloud_sdk::google::firestore::v1::Cursor {
    fn from(cursor: FirestoreQueryCursor) -> Self {
        match cursor {
            FirestoreQueryCursor::BeforeValue(values) => {
                gcloud_sdk::google::firestore::v1::Cursor {
                    values: values.into_iter().map(|value| value.value).collect(),
                    before: true,
                }
            }
            FirestoreQueryCursor::AfterValue(values) => gcloud_sdk::google::firestore::v1::Cursor {
                values: values.into_iter().map(|value| value.value).collect(),
                before: false,
            },
        }
    }
}

impl From<gcloud_sdk::google::firestore::v1::Cursor> for FirestoreQueryCursor {
    fn from(cursor: gcloud_sdk::google::firestore::v1::Cursor) -> Self {
        let firestore_values = cursor
            .values
            .into_iter()
            .map(FirestoreValue::from)
            .collect();
        if cursor.before {
            FirestoreQueryCursor::BeforeValue(firestore_values)
        } else {
            FirestoreQueryCursor::AfterValue(firestore_values)
        }
    }
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestorePartitionQueryParams {
    pub query_params: FirestoreQueryParams,
    pub partition_count: u32,
    pub page_size: u32,
    pub page_token: Option<String>,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestorePartition {
    pub start_at: Option<FirestoreQueryCursor>,
    pub end_at: Option<FirestoreQueryCursor>,
}
