use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{FirestoreBoolExpr, FirestoreExpr, FirestoreValue, FirestoreValueType};

/// The argument of the `filter` closure of the fluent pipeline API, built like the filter of a
/// Core query.
///
/// Every condition built here is an `Option`, as in Core, so a list may mix plain and optional
/// conditions: [`for_all`](Self::for_all) and [`for_any`](Self::for_any) drop the `None`s. Every
/// other closure of a pipeline takes the value builder, [`FirestorePipelineExprBuilder`], and
/// its conditions are plain. [`values`](Self::values) reaches that builder from here.
#[derive(Debug, Clone, Copy)]
pub struct FirestorePipelineFilterBuilder;

impl FirestorePipelineFilterBuilder {
    /// A field of the current document, by its path such as `path!(Book::title)`, to compare.
    pub fn field<S: AsRef<str>>(&self, path: S) -> FirestorePipelineFilterFieldExpr {
        FirestorePipelineFilterFieldExpr(FirestoreExpr::field(path.as_ref()))
    }

    /// A computed value to compare, such as `FirestoreExpr::field("title").to_lower()`.
    pub fn expr(&self, expr: FirestoreExpr) -> FirestorePipelineFilterFieldExpr {
        FirestorePipelineFilterFieldExpr(expr)
    }

    /// A plain condition, such as a [`not`](FirestorePipelineExprBuilder::not) or a raw function,
    /// as an item of [`for_all`](Self::for_all) and [`for_any`](Self::for_any).
    pub fn condition(&self, condition: FirestoreBoolExpr) -> Option<FirestoreBoolExpr> {
        Some(condition)
    }

    /// A constant value, to compare a field against. A value passed to a comparison becomes a
    /// constant without this.
    pub fn constant<V: Into<FirestoreValue>>(&self, value: V) -> FirestoreExpr {
        self.values().constant(value)
    }

    /// The variable `name`, defined by the `define` stage of the enclosing pipeline, to compare a
    /// field of a subquery against.
    pub fn variable<S: Into<String>>(&self, name: S) -> FirestoreExpr {
        self.values().variable(name)
    }

    /// The value builder of the other pipeline closures, for what this builder has no method
    /// for.
    pub fn values(&self) -> FirestorePipelineExprBuilder {
        FirestorePipelineExprBuilder
    }

    /// True when every condition present is. The items are all conditions or all `Option`s of
    /// one, and a `None` drops out. A single condition is returned as it is, and none gives
    /// `None`, which `filter` takes as no filter at all.
    pub fn for_all<I>(&self, conditions: I) -> Option<FirestoreBoolExpr>
    where
        I: IntoIterator,
        I::Item: FirestorePipelineConditionExpr,
    {
        self.connect_present("and", conditions)
    }

    /// True when any condition present is. The items are all conditions or all `Option`s of
    /// one, and a `None` drops out. A single condition is returned as it is, and none gives
    /// `None`, which `filter` takes as no filter at all.
    pub fn for_any<I>(&self, conditions: I) -> Option<FirestoreBoolExpr>
    where
        I: IntoIterator,
        I::Item: FirestorePipelineConditionExpr,
    {
        self.connect_present("or", conditions)
    }

    fn connect_present<I>(&self, function: &str, conditions: I) -> Option<FirestoreBoolExpr>
    where
        I: IntoIterator,
        I::Item: FirestorePipelineConditionExpr,
    {
        let present: Vec<FirestoreBoolExpr> = conditions
            .into_iter()
            .filter_map(FirestorePipelineConditionExpr::build_condition)
            .collect();
        (!present.is_empty()).then(|| self.values().connect(function, present))
    }
}

/// A condition, or an `Option` of one, as the list forms `for_all` and `for_any` take them.
pub trait FirestorePipelineConditionExpr {
    /// The condition, or `None` for an item that stands for no condition.
    fn build_condition(self) -> Option<FirestoreBoolExpr>;
}

impl FirestorePipelineConditionExpr for FirestoreBoolExpr {
    fn build_condition(self) -> Option<FirestoreBoolExpr> {
        Some(self)
    }
}

impl<C: FirestorePipelineConditionExpr> FirestorePipelineConditionExpr for Option<C> {
    fn build_condition(self) -> Option<FirestoreBoolExpr> {
        self.and_then(FirestorePipelineConditionExpr::build_condition)
    }
}

/// The expression a condition of the `filter` closure compares, started by
/// [`field`](FirestorePipelineFilterBuilder::field) or
/// [`expr`](FirestorePipelineFilterBuilder::expr).
///
/// Each condition is the [`FirestoreExpr`] method of the same name, returned as `Some` so that
/// it joins the `Option` conditions of `for_all` and `for_any`.
#[derive(Debug)]
pub struct FirestorePipelineFilterFieldExpr(FirestoreExpr);

impl FirestorePipelineFilterFieldExpr {
    /// See [`FirestoreExpr::equal`].
    pub fn equal<V: Into<FirestoreExpr>>(self, other: V) -> Option<FirestoreBoolExpr> {
        Some(self.0.equal(other))
    }

    /// See [`FirestoreExpr::eq`].
    pub fn eq<V: Into<FirestoreExpr>>(self, other: V) -> Option<FirestoreBoolExpr> {
        Some(self.0.eq(other))
    }

    /// See [`FirestoreExpr::not_equal`].
    pub fn not_equal<V: Into<FirestoreExpr>>(self, other: V) -> Option<FirestoreBoolExpr> {
        Some(self.0.not_equal(other))
    }

    /// See [`FirestoreExpr::neq`].
    pub fn neq<V: Into<FirestoreExpr>>(self, other: V) -> Option<FirestoreBoolExpr> {
        Some(self.0.neq(other))
    }

    /// See [`FirestoreExpr::less_than`].
    pub fn less_than<V: Into<FirestoreExpr>>(self, other: V) -> Option<FirestoreBoolExpr> {
        Some(self.0.less_than(other))
    }

    /// See [`FirestoreExpr::less_than_or_equal`].
    pub fn less_than_or_equal<V: Into<FirestoreExpr>>(self, other: V) -> Option<FirestoreBoolExpr> {
        Some(self.0.less_than_or_equal(other))
    }

    /// See [`FirestoreExpr::greater_than`].
    pub fn greater_than<V: Into<FirestoreExpr>>(self, other: V) -> Option<FirestoreBoolExpr> {
        Some(self.0.greater_than(other))
    }

    /// See [`FirestoreExpr::greater_than_or_equal`].
    pub fn greater_than_or_equal<V: Into<FirestoreExpr>>(
        self,
        other: V,
    ) -> Option<FirestoreBoolExpr> {
        Some(self.0.greater_than_or_equal(other))
    }

    /// See [`FirestoreExpr::equal_any`].
    pub fn equal_any(self, search_space: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.equal_any(search_space))
    }

    /// See [`FirestoreExpr::is_in`].
    pub fn is_in(self, search_space: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_in(search_space))
    }

    /// See [`FirestoreExpr::not_equal_any`].
    pub fn not_equal_any(
        self,
        search_space: impl Into<FirestoreExpr>,
    ) -> Option<FirestoreBoolExpr> {
        Some(self.0.not_equal_any(search_space))
    }

    /// See [`FirestoreExpr::is_not_in`].
    pub fn is_not_in(self, search_space: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_not_in(search_space))
    }

    /// See [`FirestoreExpr::is_null`].
    pub fn is_null(self) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_null())
    }

    /// See [`FirestoreExpr::is_not_null`].
    pub fn is_not_null(self) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_not_null())
    }

    /// See [`FirestoreExpr::is_nan`].
    pub fn is_nan(self) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_nan())
    }

    /// See [`FirestoreExpr::is_not_nan`].
    pub fn is_not_nan(self) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_not_nan())
    }

    /// See [`FirestoreExpr::is_absent`].
    pub fn is_absent(self) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_absent())
    }

    /// See [`FirestoreExpr::is_error`].
    pub fn is_error(self) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_error())
    }

    /// See [`FirestoreExpr::exists`].
    pub fn exists(self) -> Option<FirestoreBoolExpr> {
        Some(self.0.exists())
    }

    /// See [`FirestoreExpr::is_type`].
    pub fn is_type(self, value_type: FirestoreValueType) -> Option<FirestoreBoolExpr> {
        Some(self.0.is_type(value_type))
    }

    /// See [`FirestoreExpr::starts_with`].
    pub fn starts_with(self, prefix: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.starts_with(prefix))
    }

    /// See [`FirestoreExpr::ends_with`].
    pub fn ends_with(self, postfix: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.ends_with(postfix))
    }

    /// See [`FirestoreExpr::string_contains`].
    pub fn string_contains(self, substring: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.string_contains(substring))
    }

    /// See [`FirestoreExpr::like`].
    pub fn like(self, pattern: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.like(pattern))
    }

    /// See [`FirestoreExpr::regex_contains`].
    pub fn regex_contains(self, regexp: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.regex_contains(regexp))
    }

    /// See [`FirestoreExpr::regex_match`].
    pub fn regex_match(self, regexp: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.regex_match(regexp))
    }

    /// See [`FirestoreExpr::array_contains`].
    pub fn array_contains(self, value: impl Into<FirestoreExpr>) -> Option<FirestoreBoolExpr> {
        Some(self.0.array_contains(value))
    }

    /// See [`FirestoreExpr::array_contains_all`].
    pub fn array_contains_all(
        self,
        search_values: impl Into<FirestoreExpr>,
    ) -> Option<FirestoreBoolExpr> {
        Some(self.0.array_contains_all(search_values))
    }

    /// See [`FirestoreExpr::array_contains_any`].
    pub fn array_contains_any(
        self,
        search_values: impl Into<FirestoreExpr>,
    ) -> Option<FirestoreBoolExpr> {
        Some(self.0.array_contains_any(search_values))
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline_builder::FirestorePipelineInitialBuilder;
    use crate::{FirestoreExpr, FirestoreLimitStage, FirestorePipeline, FirestorePipelineStage};
    use gcloud_sdk::google::firestore::v1::{pipeline::Stage, value::ValueType, Pipeline};

    fn builder() -> FirestorePipelineInitialBuilder<()> {
        FirestorePipelineInitialBuilder::new(())
    }

    fn where_stages(pipeline: FirestorePipeline) -> Vec<Stage> {
        Pipeline::try_from(pipeline)
            .unwrap()
            .stages
            .into_iter()
            .filter(|stage| stage.name == "where")
            .collect()
    }

    #[test]
    fn plain_and_optional_conditions_join_the_present_ones_into_one_and() {
        let genre = Some("Dystopian");
        let max_pages: Option<i64> = None;
        let min_rating = Some(4.0);

        let list = builder()
            .from("pipeline-books")
            .filter(|q| {
                q.for_all([
                    q.field("stock").greater_than(0),
                    genre.and_then(|genre| q.field("genre").eq(genre)),
                    max_pages.and_then(|pages| q.field("pages").less_than(pages)),
                    min_rating.and_then(|rating| q.field("rating").greater_than_or_equal(rating)),
                ])
            })
            .into_pipeline();
        let flat = builder()
            .from("pipeline-books")
            .filter(|q| {
                q.condition(q.values().and([
                    q.values().field("stock").greater_than(0),
                    q.values().field("genre").eq("Dystopian"),
                    q.values().field("rating").greater_than_or_equal(4.0),
                ]))
            })
            .into_pipeline();

        let expected = where_stages(flat);
        assert_eq!(where_stages(list), expected);
        let Some(ValueType::FunctionValue(and)) = &expected[0].args[0].value_type else {
            panic!("the condition must be a function: {expected:?}");
        };
        assert_eq!((and.name.as_str(), and.args.len()), ("and", 3));
    }

    #[test]
    fn expr_and_condition_lift_a_value_and_a_plain_condition() {
        let lifted = builder()
            .from("pipeline-books")
            .filter(|q| {
                q.for_all([
                    q.expr(FirestoreExpr::field("title").to_lower())
                        .starts_with("the"),
                    q.condition(q.values().not(q.values().field("stock").eq(0))),
                ])
            })
            .into_pipeline();
        let by_hand = builder()
            .from("pipeline-books")
            .filter(|q| {
                let e = q.values();
                q.condition(e.and([
                    FirestoreExpr::field("title").to_lower().starts_with("the"),
                    e.not(e.field("stock").eq(0)),
                ]))
            })
            .into_pipeline();

        assert_eq!(where_stages(lifted), where_stages(by_hand));
    }

    #[test]
    fn a_single_present_condition_stands_alone() {
        let joined = builder()
            .from("pipeline-books")
            .filter(|q| q.for_all([None, q.field("stock").greater_than(0), None]))
            .into_pipeline();
        let alone = builder()
            .from("pipeline-books")
            .filter(|q| q.field("stock").greater_than(0))
            .into_pipeline();

        assert_eq!(joined, alone);
    }

    #[test]
    fn absent_conditions_add_no_where_stage() {
        let genre: Option<&str> = None;
        let pipeline = builder()
            .from("pipeline-books")
            .filter(|q| q.for_any([genre.and_then(|genre| q.field("genre").eq(genre))]))
            .limit(1)
            .into_pipeline();

        assert_eq!(
            pipeline.stages,
            vec![FirestorePipelineStage::Limit(FirestoreLimitStage::new(1))]
        );
    }

    #[test]
    fn for_any_drops_the_absent_conditions() {
        let genre: Option<&str> = None;
        let any = builder()
            .from("pipeline-books")
            .filter(|q| {
                q.for_any([
                    q.field("stock").greater_than(0),
                    genre.and_then(|genre| q.field("genre").eq(genre)),
                    q.field("rating").greater_than_or_equal(4.0),
                ])
            })
            .into_pipeline();
        let or = builder()
            .from("pipeline-books")
            .filter(|q| {
                q.condition(q.values().or([
                    q.values().field("stock").greater_than(0),
                    q.values().field("rating").greater_than_or_equal(4.0),
                ]))
            })
            .into_pipeline();

        assert_eq!(where_stages(any), where_stages(or));
    }
}
