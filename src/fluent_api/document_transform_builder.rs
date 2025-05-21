//! Builder for specifying field transformations in Firestore update operations.
//!
//! This module provides a fluent API to define transformations that should be
//! applied to document fields atomically on the server-side. These transformations
//! are used with update operations.
//!
//! Examples of transformations include:
//! - Setting a field to the server's timestamp.
//! - Incrementing a numeric field.
//! - Adding or removing elements from an array field.
//!
//! The main entry point is [`FirestoreTransformBuilder`], which is typically
//! accessed via a method on an update builder (e.g., [`FirestoreUpdateSetBuilder::transforms()`](crate::FirestoreUpdateSetBuilder::transforms)).

use crate::{
    FirestoreFieldTransform, FirestoreFieldTransformType, FirestoreTransformServerValue,
    FirestoreValue,
};

/// A builder for constructing a list of field transformations.
///
/// This builder is used within an update operation to specify atomic, server-side
/// modifications to document fields.
pub struct FirestoreTransformBuilder {}

impl FirestoreTransformBuilder {
    /// Creates a new `FirestoreTransformBuilder`.
    /// This is typically not called directly but obtained from an update builder.
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Builds a `Vec` of [`FirestoreFieldTransform`] from a collection of transform expressions.
    ///
    /// This method takes an iterator of items that implement [`FirestoreTransformExpr`]
    /// (typically created using [`FirestoreTransformBuilder::field()`] and its chained methods)
    /// and collects them into a vector of transformations.
    ///
    /// `Option<FirestoreFieldTransform>` expressions are filtered, so `None` values are ignored.
    ///
    /// # Arguments
    /// * `transform_field_expr`: An iterator of transform expressions.
    ///
    /// # Returns
    /// A `Vec<FirestoreFieldTransform>` ready to be used in an update operation.
    #[inline]
    pub fn fields<I>(&self, transform_field_expr: I) -> Vec<FirestoreFieldTransform>
    where
        I: IntoIterator,
        I::Item: FirestoreTransformExpr,
    {
        transform_field_expr
            .into_iter()
            .filter_map(|filter| filter.build_transform())
            .collect()
    }

    /// Specifies a field to apply a transformation to.
    ///
    /// # Arguments
    /// * `field_name`: The dot-separated path to the field.
    ///
    /// # Returns
    /// A [`FirestoreTransformFieldExpr`] to specify the type of transformation.
    #[inline]
    pub fn field<S>(&self, field_name: S) -> FirestoreTransformFieldExpr
    where
        S: AsRef<str>,
    {
        FirestoreTransformFieldExpr::new(field_name.as_ref().to_string())
    }
}

/// A trait for types that can be converted into a [`FirestoreFieldTransform`].
///
/// This is used by [`FirestoreTransformBuilder::fields()`] to allow various ways
/// of defining transformations, including optional ones.
pub trait FirestoreTransformExpr {
    /// Builds the [`FirestoreFieldTransform`].
    /// Returns `None` if the expression represents an empty or no-op transform.
    fn build_transform(self) -> Option<FirestoreFieldTransform>;
}

/// Represents a specific field targeted for a transformation.
///
/// This struct provides methods to define the actual transformation to be applied
/// to the field (e.g., increment, set to server value).
pub struct FirestoreTransformFieldExpr {
    field_name: String,
}

impl FirestoreTransformFieldExpr {
    /// Creates a new `FirestoreTransformFieldExpr` for the given field name.
    pub(crate) fn new(field_name: String) -> Self {
        Self { field_name }
    }

    /// Specifies an "increment" transformation.
    ///
    /// Atomically increments the numeric value of the field by the given value.
    /// The value must be an integer or a double.
    ///
    /// # Arguments
    /// * `value`: The value to increment by, convertible to [`FirestoreValue`].
    ///
    /// # Returns
    /// An `Option<FirestoreFieldTransform>` representing this transformation.
    #[inline]
    pub fn increment<V>(self, value: V) -> Option<FirestoreFieldTransform>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreFieldTransform::new(
            self.field_name,
            FirestoreFieldTransformType::Increment(value.into()),
        ))
    }

    /// Specifies a "maximum" transformation.
    ///
    /// Atomically sets the field to the maximum of its current value and the given value.
    ///
    /// # Arguments
    /// * `value`: The value to compare with, convertible to [`FirestoreValue`].
    ///
    /// # Returns
    /// An `Option<FirestoreFieldTransform>` representing this transformation.
    #[inline]
    pub fn maximum<V>(self, value: V) -> Option<FirestoreFieldTransform>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreFieldTransform::new(
            self.field_name,
            FirestoreFieldTransformType::Maximum(value.into()),
        ))
    }

    /// Specifies a "minimum" transformation.
    ///
    /// Atomically sets the field to the minimum of its current value and the given value.
    ///
    /// # Arguments
    /// * `value`: The value to compare with, convertible to [`FirestoreValue`].
    ///
    /// # Returns
    /// An `Option<FirestoreFieldTransform>` representing this transformation.
    #[inline]
    pub fn minimum<V>(self, value: V) -> Option<FirestoreFieldTransform>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreFieldTransform::new(
            self.field_name,
            FirestoreFieldTransformType::Minimum(value.into()),
        ))
    }

    /// Specifies a "set to server value" transformation.
    ///
    /// Sets the field to a server-generated value, most commonly the request timestamp.
    ///
    /// # Arguments
    /// * `value`: The [`FirestoreTransformServerValue`] to set (e.g., `RequestTime`).
    ///
    /// # Returns
    /// An `Option<FirestoreFieldTransform>` representing this transformation.
    #[inline]
    pub fn server_value(
        self,
        value: FirestoreTransformServerValue,
    ) -> Option<FirestoreFieldTransform> {
        Some(FirestoreFieldTransform::new(
            self.field_name,
            FirestoreFieldTransformType::SetToServerValue(value),
        ))
    }

    /// Specifies an "append missing elements" transformation for an array field.
    ///
    /// Atomically adds elements to the end of an array field, but only if they are
    /// not already present in the array.
    ///
    /// # Arguments
    /// * `values`: An iterator of items convertible to [`FirestoreValue`] to append.
    ///
    /// # Returns
    /// An `Option<FirestoreFieldTransform>` representing this transformation.
    #[inline]
    pub fn append_missing_elements<I>(self, values: I) -> Option<FirestoreFieldTransform>
    where
        I: IntoIterator,
        I::Item: Into<FirestoreValue>,
    {
        Some(FirestoreFieldTransform::new(
            self.field_name,
            FirestoreFieldTransformType::AppendMissingElements(
                values.into_iter().map(|m| m.into()).collect(),
            ),
        ))
    }

    /// Specifies a "remove all from array" transformation for an array field.
    ///
    /// Atomically removes all instances of the given elements from an array field.
    ///
    /// # Arguments
    /// * `values`: An iterator of items convertible to [`FirestoreValue`] to remove.
    ///
    /// # Returns
    /// An `Option<FirestoreFieldTransform>` representing this transformation.
    #[inline]
    pub fn remove_all_from_array<I>(self, values: I) -> Option<FirestoreFieldTransform>
    where
        I: IntoIterator,
        I::Item: Into<FirestoreValue>,
    {
        Some(FirestoreFieldTransform::new(
            self.field_name,
            FirestoreFieldTransformType::RemoveAllFromArray(
                values.into_iter().map(|m| m.into()).collect(),
            ),
        ))
    }
}

impl FirestoreTransformExpr for FirestoreFieldTransform {
    #[inline]
    fn build_transform(self) -> Option<FirestoreFieldTransform> {
        Some(self)
    }
}

// Allows using Option<FirestoreFieldTransform> in the fields array,
// filtering out None values.
impl<F> FirestoreTransformExpr for Option<F>
where
    F: FirestoreTransformExpr,
{
    #[inline]
    fn build_transform(self) -> Option<FirestoreFieldTransform> {
        self.and_then(|expr| expr.build_transform())
    }
}
