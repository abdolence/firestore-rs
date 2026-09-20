//! Builder for constructing complex filter conditions for Firestore queries.
//!
//! This module provides a fluent API to define filters that can be applied to
//! select/query operations. It supports:
//! - Simple field comparisons (e.g., equality, greater than).
//! - Unary filters (e.g., IS NULL, IS NAN).
//! - Composite filters (AND, OR) to combine multiple conditions.
//!
//! The main entry point is [`FirestoreQueryFilterBuilder`], passed into the closure given to
//! `.filter()` on a select builder.
//!
//! [`FirestoreQueryFilterBuilder`]: crate::select_filter_builder::FirestoreQueryFilterBuilder

use crate::{
    FirestoreQueryFilter, FirestoreQueryFilterCompare, FirestoreQueryFilterComposite,
    FirestoreQueryFilterCompositeOperator, FirestoreQueryFilterUnary, FirestoreValue,
};

/// A builder for constructing Firestore query filters.
///
/// This builder is used to create [`FirestoreQueryFilter`] instances, which can then
/// be applied to select/query operations. It provides methods for creating
/// composite filters (`for_all`, `for_any`) and for targeting specific fields
/// to apply comparison or unary operators.

#[derive(Clone, Debug)]
pub struct FirestoreQueryFilterBuilder;

impl FirestoreQueryFilterBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Internal helper to build a composite filter (AND or OR).
    ///
    /// If only one valid filter expression is provided, it's returned directly without
    /// being wrapped in a composite filter. If no valid expressions are provided,
    /// `None` is returned.
    #[inline]
    fn build_filter_with_op<I>(
        &self,
        filter_expressions: I,
        op: FirestoreQueryFilterCompositeOperator,
    ) -> Option<FirestoreQueryFilter>
    where
        I: IntoIterator,
        I::Item: FirestoreQueryFilterExpr,
    {
        let mut filters: Vec<FirestoreQueryFilter> = filter_expressions
            .into_iter()
            .filter_map(|filter| filter.build_filter())
            .collect();

        if filters.is_empty() {
            None
        } else if filters.len() == 1 {
            filters.pop()
        } else {
            Some(FirestoreQueryFilter::Composite(
                FirestoreQueryFilterComposite::new(filters, op),
            ))
        }
    }

    /// Combines `filter_expressions` so every one of them must match (logical AND).
    ///
    /// Returns the single filter unwrapped if only one expression is valid, or `None` if none
    /// are.
    #[inline]
    pub fn for_all<I>(&self, filter_expressions: I) -> Option<FirestoreQueryFilter>
    where
        I: IntoIterator,
        I::Item: FirestoreQueryFilterExpr,
    {
        self.build_filter_with_op(
            filter_expressions,
            FirestoreQueryFilterCompositeOperator::And,
        )
    }

    /// Combines `filter_expressions` so at least one of them must match (logical OR).
    ///
    /// Returns the single filter unwrapped if only one expression is valid, or `None` if none
    /// are.
    #[inline]
    pub fn for_any<I>(&self, filter_expressions: I) -> Option<FirestoreQueryFilter>
    where
        I: IntoIterator,
        I::Item: FirestoreQueryFilterExpr,
    {
        self.build_filter_with_op(
            filter_expressions,
            FirestoreQueryFilterCompositeOperator::Or,
        )
    }

    /// Targets `field_name` for a comparison or unary filter.
    #[inline]
    pub fn field<S>(&self, field_name: S) -> FirestoreQueryFilterFieldExpr
    where
        S: AsRef<str>,
    {
        FirestoreQueryFilterFieldExpr::new(field_name.as_ref().to_string())
    }
}

/// A trait for types that can be converted into a [`FirestoreQueryFilter`].
///
/// This is used by [`FirestoreQueryFilterBuilder`] methods like `for_all` and `for_any`
/// to allow various ways of defining filter conditions, including optional ones.
pub trait FirestoreQueryFilterExpr {
    /// Builds the [`FirestoreQueryFilter`].
    /// Returns `None` if the expression represents an empty or no-op filter.
    fn build_filter(self) -> Option<FirestoreQueryFilter>;
}

/// Represents a specific field targeted for a filter condition.
///
/// This struct provides methods to define the comparison or unary operator
/// to be applied to the field.
pub struct FirestoreQueryFilterFieldExpr {
    field_name: String,
}

impl FirestoreQueryFilterFieldExpr {
    pub(crate) fn new(field_name: String) -> Self {
        Self { field_name }
    }

    /// Creates an "equal to" filter (e.g., `field == value`).
    /// Alias for [`equal()`](#method.equal).
    #[inline]
    pub fn eq<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        self.equal(value)
    }

    /// Creates a "not equal to" filter (e.g., `field != value`).
    /// Alias for [`not_equal()`](#method.not_equal).
    #[inline]
    pub fn neq<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        self.not_equal(value)
    }

    /// Creates an "equal to" filter (e.g., `field == value`).
    #[inline]
    pub fn equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::Equal(self.field_name, value.into()),
        )))
    }

    /// Creates a "not equal to" filter (e.g., `field != value`).
    #[inline]
    pub fn not_equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::NotEqual(self.field_name, value.into()),
        )))
    }

    /// Creates a "less than" filter (e.g., `field < value`).
    #[inline]
    pub fn less_than<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::LessThan(self.field_name, value.into()),
        )))
    }

    /// Creates a "less than or equal to" filter (e.g., `field <= value`).
    #[inline]
    pub fn less_than_or_equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::LessThanOrEqual(self.field_name, value.into()),
        )))
    }

    /// Creates a "greater than" filter (e.g., `field > value`).
    #[inline]
    pub fn greater_than<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::GreaterThan(self.field_name, value.into()),
        )))
    }

    /// Creates a "greater than or equal to" filter (e.g., `field >= value`).
    #[inline]
    pub fn greater_than_or_equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::GreaterThanOrEqual(self.field_name, value.into()),
        )))
    }

    /// Creates an "in" filter (e.g., `field IN [value1, value2, ...]`).
    /// The provided `value` should be an array [`FirestoreValue`].
    #[inline]
    pub fn is_in<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::In(self.field_name, value.into()),
        )))
    }

    /// Creates a "not in" filter (e.g., `field NOT IN [value1, value2, ...]`).
    /// The provided `value` should be an array [`FirestoreValue`].
    #[inline]
    pub fn is_not_in<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::NotIn(self.field_name, value.into()),
        )))
    }

    /// Creates an "array-contains" filter (e.g., `field array-contains value`).
    /// Checks if an array field contains the given value.
    #[inline]
    pub fn array_contains<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContains(self.field_name, value.into()),
        )))
    }

    /// Creates an "array-contains-any" filter: does the array field contain any of `value`.
    /// `value` should be an array [`FirestoreValue`].
    #[inline]
    pub fn array_contains_any<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContainsAny(self.field_name, value.into()),
        )))
    }

    /// Creates an "is NaN" filter. Checks if a numeric field is NaN (Not a Number).
    #[inline]
    pub fn is_nan(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNan(self.field_name),
        ))
    }

    /// Creates an "is not NaN" filter. Checks if a numeric field is not NaN.
    #[inline]
    pub fn is_not_nan(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNotNan(self.field_name),
        ))
    }

    /// Creates an "is null" filter. Checks if a field is null.
    #[inline]
    pub fn is_null(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNull(self.field_name),
        ))
    }

    /// Creates an "is not null" filter. Checks if a field is not null.
    #[inline]
    pub fn is_not_null(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNotNull(self.field_name),
        ))
    }
}

impl FirestoreQueryFilterExpr for FirestoreQueryFilter {
    #[inline]
    fn build_filter(self) -> Option<FirestoreQueryFilter> {
        Some(self)
    }
}

// Allows using Option<FirestoreQueryFilter> in the for_all/for_any arrays,
// filtering out None values.
impl<F> FirestoreQueryFilterExpr for Option<F>
where
    F: FirestoreQueryFilterExpr,
{
    #[inline]
    fn build_filter(self) -> Option<FirestoreQueryFilter> {
        self.and_then(|expr| expr.build_filter())
    }
}
