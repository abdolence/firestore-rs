//! Builder for constructing aggregation queries in Firestore.
//!
//! This module provides a fluent API to define aggregations like `COUNT`, `SUM`, and `AVG`
//! to be performed over a set of documents matching a query.
//!
//! The main entry point is [`FirestoreAggregationBuilder`], passed into the closure given to
//! `.aggregate()` on a select builder.
//!
//! [`FirestoreAggregationBuilder`]: crate::select_aggregation_builder::FirestoreAggregationBuilder

use crate::{
    FirestoreAggregation, FirestoreAggregationOperator, FirestoreAggregationOperatorAvg,
    FirestoreAggregationOperatorCount, FirestoreAggregationOperatorSum,
};

/// A builder for constructing a list of aggregations to apply to a query.
///
/// This builder is used within a select/query operation to specify one or more
/// aggregations to be computed by Firestore.
pub struct FirestoreAggregationBuilder {}

impl FirestoreAggregationBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Collects `aggregation_field_expr` - typically built with
    /// [`FirestoreAggregationBuilder::field`] and its chained methods - into the aggregations for
    /// a query, dropping `None` entries.
    #[inline]
    pub fn fields<I>(&self, aggregation_field_expr: I) -> Vec<FirestoreAggregation>
    where
        I: IntoIterator,
        I::Item: FirestoreAggregationExpr,
    {
        aggregation_field_expr
            .into_iter()
            .filter_map(|filter| filter.build_aggregation())
            .collect()
    }

    /// Assigns `field_name` as the alias under which this aggregation's result is returned, then
    /// call `count`, `sum` or `avg` to pick the operator.
    #[inline]
    pub fn field<S>(&self, field_name: S) -> FirestoreAggregationFieldExpr
    where
        S: AsRef<str>,
    {
        FirestoreAggregationFieldExpr::new(field_name.as_ref().to_string())
    }
}

/// A trait for types that can be converted into a [`FirestoreAggregation`].
///
/// This is used by [`FirestoreAggregationBuilder::fields()`] to allow various ways
/// of defining aggregations, including optional ones.
pub trait FirestoreAggregationExpr {
    /// Builds the [`FirestoreAggregation`].
    /// Returns `None` if the expression represents an empty or no-op aggregation.
    fn build_aggregation(self) -> Option<FirestoreAggregation>;
}

/// Represents a specific alias targeted for an aggregation operation.
///
/// This struct provides methods to define the actual aggregation to be performed
/// (e.g., count, sum, avg) and associate it with the alias.
pub struct FirestoreAggregationFieldExpr {
    field_name: String, // This is the alias for the aggregation result
}

impl FirestoreAggregationFieldExpr {
    pub(crate) fn new(field_name: String) -> Self {
        Self { field_name }
    }

    /// Specifies a "count" aggregation.
    ///
    /// Counts the number of documents matching the query. The result is returned
    /// under the alias specified by `field_name`.
    #[inline]
    pub fn count(self) -> Option<FirestoreAggregation> {
        Some(FirestoreAggregation::new(self.field_name).with_operator(
            FirestoreAggregationOperator::Count(FirestoreAggregationOperatorCount::new()),
        ))
    }

    /// Specifies a "count up to" aggregation.
    ///
    /// Counts the number of documents matching the query, up to a specified limit.
    /// This can be more efficient than a full count if only an approximate count or
    /// a capped count is needed.
    #[inline]
    pub fn count_up_to(self, up_to: usize) -> Option<FirestoreAggregation> {
        Some(FirestoreAggregation::new(self.field_name).with_operator(
            FirestoreAggregationOperator::Count(
                FirestoreAggregationOperatorCount::new().with_up_to(up_to),
            ),
        ))
    }

    /// Specifies a "sum" aggregation.
    ///
    /// Calculates the sum of the values of a specific numeric field across all
    /// documents matching the query. The result is returned under the alias
    /// specified by `field_name`.
    #[inline]
    pub fn sum<S>(self, sum_on_field_name: S) -> Option<FirestoreAggregation>
    where
        S: AsRef<str>,
    {
        Some(FirestoreAggregation::new(self.field_name).with_operator(
            FirestoreAggregationOperator::Sum(FirestoreAggregationOperatorSum::new(
                sum_on_field_name.as_ref().to_string(),
            )),
        ))
    }

    /// Specifies an "average" (avg) aggregation.
    ///
    /// Calculates the average of the values of a specific numeric field across all
    /// documents matching the query. The result is returned under the alias
    /// specified by `field_name`.
    #[inline]
    pub fn avg<S>(self, avg_on_field_name: S) -> Option<FirestoreAggregation>
    where
        S: AsRef<str>,
    {
        Some(FirestoreAggregation::new(self.field_name).with_operator(
            FirestoreAggregationOperator::Avg(FirestoreAggregationOperatorAvg::new(
                avg_on_field_name.as_ref().to_string(),
            )),
        ))
    }
}

impl FirestoreAggregationExpr for FirestoreAggregation {
    #[inline]
    fn build_aggregation(self) -> Option<FirestoreAggregation> {
        Some(self)
    }
}

// Allows using Option<FirestoreAggregation> in the fields array,
// filtering out None values.
impl<F> FirestoreAggregationExpr for Option<F>
where
    F: FirestoreAggregationExpr,
{
    #[inline]
    fn build_aggregation(self) -> Option<FirestoreAggregation> {
        self.and_then(|expr| expr.build_aggregation())
    }
}
