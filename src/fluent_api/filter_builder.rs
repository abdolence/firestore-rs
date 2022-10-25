use crate::{
    FirestoreQueryFilter, FirestoreQueryFilterCompare, FirestoreQueryFilterComposite,
    FirestoreQueryFilterUnary, FirestoreValue,
};

#[derive(Clone, Debug)]
pub struct FirestoreQueryFilterBuilder;

impl FirestoreQueryFilterBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub fn for_all<I>(&self, filter_expressions: I) -> Option<FirestoreQueryFilter>
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
                FirestoreQueryFilterComposite::new(filters),
            ))
        }
    }

    pub fn field<S>(&self, field_name: S) -> FirestoreQueryFilterFieldExpr
    where
        S: AsRef<str>,
    {
        FirestoreQueryFilterFieldExpr::new(field_name.as_ref().to_string())
    }
}

pub trait FirestoreQueryFilterExpr {
    fn build_filter(self) -> Option<FirestoreQueryFilter>;
}

pub struct FirestoreQueryFilterFieldExpr {
    field_name: String,
}

impl FirestoreQueryFilterFieldExpr {
    pub(crate) fn new(field_name: String) -> Self {
        Self { field_name }
    }

    #[inline]
    pub fn eq<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        self.equal(value)
    }

    #[inline]
    pub fn neq<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        self.not_equal(value)
    }

    #[inline]
    pub fn equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::Equal(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn not_equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::NotEqual(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn less_than<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::LessThan(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn less_than_or_equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::LessThanOrEqual(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn greater_than<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::GreaterThan(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn greater_than_or_equal<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::GreaterThanOrEqual(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn intersect<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::In(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn not_intersect<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::NotIn(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn array_contains<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContains(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn array_contains_any<V>(self, value: V) -> Option<FirestoreQueryFilter>
    where
        V: Into<FirestoreValue>,
    {
        Some(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContainsAny(self.field_name, value.into()),
        )))
    }

    #[inline]
    pub fn is_nan(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNan(self.field_name),
        ))
    }

    #[inline]
    pub fn is_not_nan(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNotNan(self.field_name),
        ))
    }

    #[inline]
    pub fn is_null(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNull(self.field_name),
        ))
    }

    #[inline]
    pub fn is_not_null(self) -> Option<FirestoreQueryFilter> {
        Some(FirestoreQueryFilter::Unary(
            FirestoreQueryFilterUnary::IsNotNull(self.field_name),
        ))
    }
}

impl FirestoreQueryFilterExpr for FirestoreQueryFilter {
    fn build_filter(self) -> Option<FirestoreQueryFilter> {
        Some(self)
    }
}

impl<F> FirestoreQueryFilterExpr for Option<F>
where
    F: FirestoreQueryFilterExpr,
{
    fn build_filter(self) -> Option<FirestoreQueryFilter> {
        self.and_then(|expr| expr.build_filter())
    }
}
