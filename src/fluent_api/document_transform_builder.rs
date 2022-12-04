use crate::{
    FirestoreFieldTransform, FirestoreFieldTransformType, FirestoreTransformServerValue,
    FirestoreValue,
};

pub struct FirestoreTransformBuilder {}

impl FirestoreTransformBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

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

    #[inline]
    pub fn field<S>(&self, field_name: S) -> FirestoreTransformFieldExpr
    where
        S: AsRef<str>,
    {
        FirestoreTransformFieldExpr::new(field_name.as_ref().to_string())
    }
}

pub trait FirestoreTransformExpr {
    fn build_transform(self) -> Option<FirestoreFieldTransform>;
}

pub struct FirestoreTransformFieldExpr {
    field_name: String,
}

impl FirestoreTransformFieldExpr {
    pub(crate) fn new(field_name: String) -> Self {
        Self { field_name }
    }

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

impl<F> FirestoreTransformExpr for Option<F>
where
    F: FirestoreTransformExpr,
{
    #[inline]
    fn build_transform(self) -> Option<FirestoreFieldTransform> {
        self.and_then(|expr| expr.build_transform())
    }
}
