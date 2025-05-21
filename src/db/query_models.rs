// Allow derive_partial_eq_without_eq because some of these types wrap generated gRPC types
// that might not implement Eq, or their Eq implementation might change.
#![allow(clippy::derive_partial_eq_without_eq)]

use crate::errors::{
    FirestoreError, FirestoreInvalidParametersError, FirestoreInvalidParametersPublicDetails,
};
use crate::{FirestoreValue, FirestoreVector};
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::Builder;

/// Specifies the target collection(s) for a Firestore query.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryCollection {
    /// Queries a single collection identified by its ID.
    Single(String),
    /// Performs a collection group query across all collections with the specified ID(s).
    /// While Firestore gRPC supports multiple collection IDs here, typically a collection group query
    /// targets all collections with *one* specific ID.
    Group(Vec<String>),
}

#[allow(clippy::to_string_trait_impl)]
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

/// Parameters for constructing and executing a Firestore query.
///
/// This struct encapsulates all configurable aspects of a query, such as the
/// target collection, filters, ordering, limits, offsets, cursors, and projections.
/// It is used by the fluent API and direct query methods to define the query to be sent to Firestore.
///
/// # Examples
///
/// ```rust
/// use firestore::*;
///
/// let params = FirestoreQueryParams::new(
///     "my-collection".into(), // Target collection
/// )
/// .with_filter(Some(FirestoreQueryFilter::Compare(Some(
///     FirestoreQueryFilterCompare::Equal(
///         "status".to_string(),
///         "active".into(),
///     ),
/// ))))
/// .with_order_by(Some(vec![FirestoreQueryOrder::new(
///     "createdAt".to_string(),
///     FirestoreQueryDirection::Descending,
/// )]))
/// .with_limit(Some(10));
///
/// // These params can then be used with FirestoreDb methods or fluent builders.
/// ```
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreQueryParams {
    /// The parent resource path. For top-level collections, this is typically
    /// the database path (e.g., "projects/my-project/databases/(default)/documents").
    /// For sub-collections, it's the path to the parent document.
    /// If `None`, the query is assumed to be on a top-level collection relative to the
    /// `FirestoreDb`'s document path.
    pub parent: Option<String>,

    /// The ID of the collection or collection group to query.
    pub collection_id: FirestoreQueryCollection,

    /// The maximum number of results to return.
    pub limit: Option<u32>,

    /// The number of results to skip.
    pub offset: Option<u32>,

    /// A list of fields and directions to order the results by.
    pub order_by: Option<Vec<FirestoreQueryOrder>>,

    /// The filter to apply to the query.
    pub filter: Option<FirestoreQueryFilter>,

    /// If `true`, the query will search all collections located anywhere in the
    /// database under the `parent` path (if specified) that have the given
    /// `collection_id`. This is used for collection group queries.
    /// Defaults to `false` if not set, meaning only direct children collections are queried.
    pub all_descendants: Option<bool>,

    /// If set, only these fields will be returned in the query results (projection).
    /// If `None`, all fields are returned.
    pub return_only_fields: Option<Vec<String>>,

    /// A cursor to define the starting point of the query.
    pub start_at: Option<FirestoreQueryCursor>,

    /// A cursor to define the ending point of the query.
    pub end_at: Option<FirestoreQueryCursor>,

    /// Options for requesting an explanation of the query execution plan.
    pub explain_options: Option<FirestoreExplainOptions>,

    /// Options for performing a vector similarity search (find nearest neighbors).
    pub find_nearest: Option<FirestoreFindNearestOptions>,
}

impl TryFrom<FirestoreQueryParams> for StructuredQuery {
    type Error = FirestoreError;

    fn try_from(params: FirestoreQueryParams) -> Result<Self, Self::Error> {
        let query_filter = params.filter.map(|f| f.into());

        Ok(StructuredQuery {
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
            find_nearest: params
                .find_nearest
                .map(|find_nearest| find_nearest.try_into())
                .transpose()?,
            r#where: query_filter,
        })
    }
}

/// Represents a filter condition for a Firestore query.
///
/// Filters are used to narrow down the documents returned by a query based on
/// conditions applied to their fields.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreQueryFilter {
    /// A composite filter that combines multiple sub-filters using an operator (AND/OR).
    Composite(FirestoreQueryFilterComposite),
    /// A unary filter that applies an operation to a single field (e.g., IS NULL, IS NAN).
    Unary(FirestoreQueryFilterUnary),
    /// A field filter that compares a field to a value (e.g., equality, greater than).
    /// The `Option` allows for representing an effectively empty or no-op filter,
    /// which can be useful in dynamic filter construction.
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

/// Specifies an ordering for query results based on a field.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreQueryOrder {
    /// The path to the field to order by (e.g., "name", "address.city").
    pub field_name: String,
    /// The direction of the ordering (ascending or descending).
    pub direction: FirestoreQueryDirection,
}

impl FirestoreQueryOrder {
    /// Returns a string representation of the order, e.g., "fieldName asc".
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

/// The direction for ordering query results.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryDirection {
    /// Sort results in ascending order.
    Ascending,
    /// Sort results in descending order.
    Descending,
}

#[allow(clippy::to_string_trait_impl)]
impl ToString for FirestoreQueryDirection {
    fn to_string(&self) -> String {
        match self {
            FirestoreQueryDirection::Ascending => "asc".to_string(),
            FirestoreQueryDirection::Descending => "desc".to_string(),
        }
    }
}

/// A composite filter that combines multiple [`FirestoreQueryFilter`]s.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreQueryFilterComposite {
    /// The list of sub-filters to combine.
    pub for_all_filters: Vec<FirestoreQueryFilter>,
    /// The operator used to combine the sub-filters (AND/OR).
    pub operator: FirestoreQueryFilterCompositeOperator,
}

/// The operator for combining filters in a [`FirestoreQueryFilterComposite`].
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryFilterCompositeOperator {
    /// Logical AND: all sub-filters must be true.
    And,
    /// Logical OR: at least one sub-filter must be true.
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

/// A unary filter that applies an operation to a single field.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreQueryFilterUnary {
    /// Checks if a field's value is NaN (Not a Number).
    /// The string argument is the field path.
    IsNan(String),
    /// Checks if a field's value is NULL.
    /// The string argument is the field path.
    IsNull(String),
    /// Checks if a field's value is not NaN.
    /// The string argument is the field path.
    IsNotNan(String),
    /// Checks if a field's value is not NULL.
    /// The string argument is the field path.
    IsNotNull(String),
}

/// A field filter that compares a field to a value using a specific operator.
/// The first `String` argument in each variant is the field path.
/// The `FirestoreValue` is the value to compare against.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreQueryFilterCompare {
    /// Field is less than the value.
    LessThan(String, FirestoreValue),
    /// Field is less than or equal to the value.
    LessThanOrEqual(String, FirestoreValue),
    /// Field is greater than the value.
    GreaterThan(String, FirestoreValue),
    /// Field is greater than or equal to the value.
    GreaterThanOrEqual(String, FirestoreValue),
    /// Field is equal to the value.
    Equal(String, FirestoreValue),
    /// Field is not equal to the value.
    NotEqual(String, FirestoreValue),
    /// Field (which must be an array) contains the value.
    ArrayContains(String, FirestoreValue),
    /// Field's value is IN the given array value. The `FirestoreValue` should be an array.
    In(String, FirestoreValue),
    /// Field (which must be an array) contains any of the values in the given array value.
    /// The `FirestoreValue` should be an array.
    ArrayContainsAny(String, FirestoreValue),
    /// Field's value is NOT IN the given array value. The `FirestoreValue` should be an array.
    NotIn(String, FirestoreValue),
}

/// Represents a cursor for paginating query results.
///
/// Cursors define a starting or ending point for a query based on the values
/// of the fields being ordered by.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreQueryCursor {
    /// Starts the query results before the document that has these field values.
    /// The `Vec<FirestoreValue>` corresponds to the values of the ordered fields.
    BeforeValue(Vec<FirestoreValue>),
    /// Starts the query results after the document that has these field values.
    /// The `Vec<FirestoreValue>` corresponds to the values of the ordered fields.
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

/// Parameters for a partitioned query.
///
/// Partitioned queries allow you to divide a large query into smaller, parallelizable chunks.
/// This is useful for exporting data or performing large-scale data processing.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestorePartitionQueryParams {
    /// The base query parameters to partition.
    pub query_params: FirestoreQueryParams,
    /// The desired number of partitions to return. Must be a positive integer.
    pub partition_count: u32,
    /// The maximum number of partitions to return in this call, used for paging.
    /// Must be a positive integer.
    pub page_size: u32,
    /// A page token from a previous `PartitionQuery` response to retrieve the next set of partitions.
    pub page_token: Option<String>,
}

/// Represents a single partition of a query.
///
/// Each partition defines a range of the original query using `start_at` and `end_at` cursors.
/// Executing a query with these cursors will yield the documents for that specific partition.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestorePartition {
    /// The cursor indicating the start of this partition.
    pub start_at: Option<FirestoreQueryCursor>,
    /// The cursor indicating the end of this partition.
    pub end_at: Option<FirestoreQueryCursor>,
}

/// Options for requesting query execution analysis from Firestore.
///
/// When `analyze` is true, Firestore will return detailed information about
/// how the query was executed, including index usage and performance metrics.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreExplainOptions {
    /// If `true`, Firestore will analyze the query and return execution details.
    /// Defaults to `false` if not specified.
    pub analyze: Option<bool>,
}

impl TryFrom<&FirestoreExplainOptions> for gcloud_sdk::google::firestore::v1::ExplainOptions {
    type Error = FirestoreError;
    fn try_from(explain_options: &FirestoreExplainOptions) -> Result<Self, Self::Error> {
        Ok(ExplainOptions {
            analyze: explain_options.analyze.unwrap_or(false),
        })
    }
}

/// Options for performing a vector similarity search (find nearest neighbors).
///
/// This is used to find documents whose vector field is closest to a given query vector.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreFindNearestOptions {
    /// The path to the vector field in your documents to search against.
    pub field_name: String,
    /// The query vector to find nearest neighbors for.
    pub query_vector: FirestoreVector,
    /// The distance measure to use for comparing vectors.
    pub distance_measure: FirestoreFindNearestDistanceMeasure,
    /// The maximum number of nearest neighbors to return.
    pub neighbors_limit: u32,
    /// An optional field name to store the calculated distance in the query results.
    /// If provided, each returned document will include this field with the distance value.
    pub distance_result_field: Option<String>,
    /// An optional threshold for the distance. Only neighbors within this distance
    /// will be returned.
    pub distance_threshold: Option<f64>,
}

impl TryFrom<FirestoreFindNearestOptions>
    for gcloud_sdk::google::firestore::v1::structured_query::FindNearest
{
    type Error = FirestoreError;

    fn try_from(options: FirestoreFindNearestOptions) -> Result<Self, Self::Error> {
        Ok(structured_query::FindNearest {
            vector_field: Some(structured_query::FieldReference {
                field_path: options.field_name,
            }),
            query_vector: Some(Into::<FirestoreValue>::into(options.query_vector).value),
            distance_measure: {
                let distance_measure: structured_query::find_nearest::DistanceMeasure =
                    options.distance_measure.try_into()?;
                distance_measure.into()
            },
            limit: Some(options.neighbors_limit.try_into().map_err(|e| {
                FirestoreError::InvalidParametersError(FirestoreInvalidParametersError::new(
                    FirestoreInvalidParametersPublicDetails::new(
                        "neighbors_limit".to_string(),
                        format!(
                            "Invalid value for neighbors_limit: {}. Maximum allowed value is {}. Error: {}",
                            options.neighbors_limit,
                            i32::MAX,
                            e
                        ),
                    ),
                ))
            })?),
            distance_result_field: options.distance_result_field.unwrap_or_default(),
            distance_threshold: options.distance_threshold,
        })
    }
}

/// Specifies the distance measure for vector similarity searches.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreFindNearestDistanceMeasure {
    /// Euclidean distance.
    Euclidean,
    /// Cosine similarity (measures the cosine of the angle between two vectors).
    Cosine,
    /// Dot product distance.
    DotProduct,
}

impl TryFrom<FirestoreFindNearestDistanceMeasure>
    for structured_query::find_nearest::DistanceMeasure
{
    type Error = FirestoreError;

    fn try_from(measure: FirestoreFindNearestDistanceMeasure) -> Result<Self, Self::Error> {
        match measure {
            FirestoreFindNearestDistanceMeasure::Euclidean => {
                Ok(structured_query::find_nearest::DistanceMeasure::Euclidean)
            }
            FirestoreFindNearestDistanceMeasure::Cosine => {
                Ok(structured_query::find_nearest::DistanceMeasure::Cosine)
            }
            FirestoreFindNearestDistanceMeasure::DotProduct => {
                Ok(structured_query::find_nearest::DistanceMeasure::DotProduct)
            }
        }
    }
}
