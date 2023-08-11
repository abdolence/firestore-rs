use crate::{
    FirestoreAggregation, FirestoreAggregationOperator, FirestoreAggregationOperatorAvg,
    FirestoreAggregationOperatorCount, FirestoreAggregationOperatorSum,
};

pub struct FirestoreAggregationBuilder {}

impl FirestoreAggregationBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

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

    #[inline]
    pub fn field<S>(&self, field_name: S) -> FirestoreAggregationFieldExpr
    where
        S: AsRef<str>,
    {
        FirestoreAggregationFieldExpr::new(field_name.as_ref().to_string())
    }
}

pub trait FirestoreAggregationExpr {
    fn build_aggregation(self) -> Option<FirestoreAggregation>;
}

pub struct FirestoreAggregationFieldExpr {
    field_name: String,
}

impl FirestoreAggregationFieldExpr {
    pub(crate) fn new(field_name: String) -> Self {
        Self { field_name }
    }

    #[inline]
    pub fn count(self) -> Option<FirestoreAggregation> {
        Some(FirestoreAggregation::new(self.field_name).with_operator(
            FirestoreAggregationOperator::Count(FirestoreAggregationOperatorCount::new()),
        ))
    }

    #[inline]
    pub fn count_up_to(self, up_to: usize) -> Option<FirestoreAggregation> {
        Some(FirestoreAggregation::new(self.field_name).with_operator(
            FirestoreAggregationOperator::Count(
                FirestoreAggregationOperatorCount::new().with_up_to(up_to),
            ),
        ))
    }

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

impl<F> FirestoreAggregationExpr for Option<F>
where
    F: FirestoreAggregationExpr,
{
    #[inline]
    fn build_aggregation(self) -> Option<FirestoreAggregation> {
        self.and_then(|expr| expr.build_aggregation())
    }
}
