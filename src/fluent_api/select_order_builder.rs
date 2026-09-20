//! Builder for specifying the ordering of Firestore query and listing results.
//!
//! This module provides a fluent API to define, per field, the direction results should be
//! sorted in.
//!
//! The main entry point is [`FirestoreQueryOrderBuilder`], passed into the closure given to
//! `.order()` on a select or listing builder.
//!
//! [`FirestoreQueryOrderBuilder`]: crate::select_order_builder::FirestoreQueryOrderBuilder

use crate::{FirestoreQueryDirection, FirestoreQueryOrder};

/// A builder for constructing a list of orderings to apply to a query or listing.
pub struct FirestoreQueryOrderBuilder {}

impl FirestoreQueryOrderBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Collects `order_field_expr` - typically built with [`FirestoreQueryOrderBuilder::field`]
    /// and its chained methods - into the ordering for a query or listing, dropping `None`
    /// entries.
    #[inline]
    pub fn fields<I>(&self, order_field_expr: I) -> Vec<FirestoreQueryOrder>
    where
        I: IntoIterator,
        I::Item: FirestoreQueryOrderExpr,
    {
        order_field_expr
            .into_iter()
            .filter_map(|order| order.build_order())
            .collect()
    }

    /// Targets `field_name` for an ordering.
    #[inline]
    pub fn field<S>(&self, field_name: S) -> FirestoreQueryOrderFieldExpr
    where
        S: AsRef<str>,
    {
        FirestoreQueryOrderFieldExpr::new(field_name.as_ref().to_string())
    }
}

/// A trait for types that can be converted into a [`FirestoreQueryOrder`].
///
/// This is used by [`FirestoreQueryOrderBuilder::fields()`] to allow various ways of defining
/// orderings, including optional ones.
pub trait FirestoreQueryOrderExpr {
    /// Builds the [`FirestoreQueryOrder`].
    /// Returns `None` if the expression represents no ordering.
    fn build_order(self) -> Option<FirestoreQueryOrder>;
}

/// Represents a specific field targeted for an ordering.
///
/// This struct provides methods to define the direction to sort the field by.
pub struct FirestoreQueryOrderFieldExpr {
    field_name: String,
}

impl FirestoreQueryOrderFieldExpr {
    pub(crate) fn new(field_name: String) -> Self {
        Self { field_name }
    }

    /// Orders by this field ascending.
    /// Alias for [`ascending()`](#method.ascending).
    #[inline]
    pub fn asc(self) -> Option<FirestoreQueryOrder> {
        self.ascending()
    }

    /// Orders by this field descending.
    /// Alias for [`descending()`](#method.descending).
    #[inline]
    pub fn desc(self) -> Option<FirestoreQueryOrder> {
        self.descending()
    }

    /// Orders by this field ascending.
    #[inline]
    pub fn ascending(self) -> Option<FirestoreQueryOrder> {
        self.direction(FirestoreQueryDirection::Ascending)
    }

    /// Orders by this field descending.
    #[inline]
    pub fn descending(self) -> Option<FirestoreQueryOrder> {
        self.direction(FirestoreQueryDirection::Descending)
    }

    /// Orders by this field using a direction chosen at runtime.
    #[inline]
    pub fn direction(self, direction: FirestoreQueryDirection) -> Option<FirestoreQueryOrder> {
        Some(FirestoreQueryOrder::new(self.field_name, direction))
    }
}

impl FirestoreQueryOrderExpr for FirestoreQueryOrder {
    #[inline]
    fn build_order(self) -> Option<FirestoreQueryOrder> {
        Some(self)
    }
}

// Allows using Option<FirestoreQueryOrder> in the fields array,
// filtering out None values.
impl<F> FirestoreQueryOrderExpr for Option<F>
where
    F: FirestoreQueryOrderExpr,
{
    #[inline]
    fn build_order(self) -> Option<FirestoreQueryOrder> {
        self.and_then(|expr| expr.build_order())
    }
}
