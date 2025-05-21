//! Builder for constructing complex filter conditions for Firestore queries.
//!
//! This module provides a fluent API to define filters that can be applied to
//! select/query operations. It supports:
//! - Simple field comparisons (e.g., equality, greater than).
//! - Unary filters (e.g., IS NULL, IS NAN).
//! - Composite filters (AND, OR) to combine multiple conditions.
//!
//! The main entry point is [`FirestoreQueryFilterBuilder`], which is typically
//! accessed within a closure passed to the `.filter()` method of a query builder
//! (e.g., [`FirestoreSelectDocBuilder::filter()`](crate::FirestoreSelectDocBuilder::filter)).

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
    /// Creates a new `FirestoreQueryFilterBuilder`.
    /// This is typically not called directly but provided within a `.filter()` closure.
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

    /// Creates a composite filter where all provided filter expressions must be true (logical AND).
    ///
    /// # Arguments
    /// * `filter_expressions`: An iterator of items that implement [`FirestoreQueryFilterExpr`].
    ///
    /// # Returns
    /// An `Option<FirestoreQueryFilter>` representing the AND-combined filter.
    /// Returns `None` if `filter_expressions` is empty or contains only `None` expressions.
    /// Returns the single filter directly if only one valid expression is provided.
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

    /// Creates a composite filter where at least one of the provided filter expressions must be true (logical OR).
    ///
    /// # Arguments
    /// * `filter_expressions`: An iterator of items that implement [`FirestoreQueryFilterExpr`].
    ///
    /// # Returns
    /// An `Option<FirestoreQueryFilter>` representing the OR-combined filter.
    /// Returns `None` if `filter_expressions` is empty or contains only `None` expressions.
    /// Returns the single filter directly if only one valid expression is provided.
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

    /// Specifies a document field to apply a filter condition to.
    ///
    /// # Arguments
    /// * `field_name`: The dot-separated path to the field.
    ///
    /// # Returns
    /// A [`FirestoreQueryFilterFieldExpr`] to specify the comparison or unary operator.
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
    /// Creates a new `FirestoreQueryFilterFieldExpr` for the given field name.
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
    /// The provided `value` should be a [`FirestoreValue::ArrayValue`].
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
    /// The provided `value` should be a [`FirestoreValue::ArrayValue`].
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

    /// Creates an "array-contains-any" filter (e.g., `field array-contains-any [value1, value2, ...]`).
    /// Checks if an array field contains any of the values in the provided array.
    /// The provided `value` should be a [`FirestoreValue::ArrayValue`].
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
