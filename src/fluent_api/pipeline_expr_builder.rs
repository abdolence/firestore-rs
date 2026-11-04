use crate::{FirestoreBoolExpr, FirestoreExpr, FirestoreValue};

/// The argument of every expression closure of the fluent pipeline API, such as
/// `.filter(|e| ..)` and `.select(|e| ..)`.
///
/// It starts expressions and combines conditions. An operation on one expression is a method on
/// that expression, such as `e.field(path!(Book::rating)).greater_than(4.0)`.
#[derive(Debug, Clone, Copy)]
pub struct FirestorePipelineExprBuilder;

impl FirestorePipelineExprBuilder {
    /// A field of the current document, by its path, such as `path!(Book::title)`.
    pub fn field<S: AsRef<str>>(&self, path: S) -> FirestoreExpr {
        FirestoreExpr::field(path.as_ref())
    }

    /// A constant value. A value passed to a comparison becomes a constant without this.
    pub fn constant<V: Into<FirestoreValue>>(&self, value: V) -> FirestoreExpr {
        FirestoreExpr::constant(value)
    }

    /// The variable `name`, defined by the `let` stage or bound by a function such as
    /// `array_filter`.
    pub fn variable<S: Into<String>>(&self, name: S) -> FirestoreExpr {
        FirestoreExpr::Variable(name.into())
    }

    /// A call of the function `name`, for a function that has no method of its own.
    pub fn raw<N, I>(&self, name: N, args: I) -> FirestoreExpr
    where
        N: Into<String>,
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw(name, args)
    }

    /// True when every condition is. A single condition is returned as it is, and no condition
    /// gives the constant `true`, the identity of `and`. For a `filter` built from conditions
    /// that may be absent, use [`for_all`](Self::for_all).
    pub fn and<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        self.connect("and", conditions)
    }

    /// True when any condition is. A single condition is returned as it is, and no condition
    /// gives the constant `false`, the identity of `or`. For a `filter` built from conditions
    /// that may be absent, use [`for_any`](Self::for_any).
    pub fn or<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        self.connect("or", conditions)
    }

    /// True when every condition present is, for a `filter` built from conditions that may be
    /// absent.
    ///
    /// The items are all conditions or all `Option`s of one, and a `None` drops out. A single
    /// condition is returned as it is, and none gives `None`, which a `filter` takes as no filter
    /// at all. For a condition used as a value, such as in `select`, `conditional` or `not`, use
    /// [`and`](Self::and).
    pub fn for_all<I>(&self, conditions: I) -> Option<FirestoreBoolExpr>
    where
        I: IntoIterator,
        I::Item: FirestorePipelineConditionExpr,
    {
        self.connect_present("and", conditions)
    }

    /// True when any condition present is, for a `filter` built from conditions that may be
    /// absent.
    ///
    /// The items are all conditions or all `Option`s of one, and a `None` drops out. A single
    /// condition is returned as it is, and none gives `None`, which a `filter` takes as no filter
    /// at all. For a condition used as a value, such as in `select`, `conditional` or `not`, use
    /// [`or`](Self::or).
    pub fn for_any<I>(&self, conditions: I) -> Option<FirestoreBoolExpr>
    where
        I: IntoIterator,
        I::Item: FirestorePipelineConditionExpr,
    {
        self.connect_present("or", conditions)
    }

    /// True when an odd number of the conditions are.
    pub fn xor<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        FirestoreBoolExpr(FirestoreExpr::raw("xor", conditions))
    }

    /// True when no condition is.
    pub fn nor<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        FirestoreBoolExpr(FirestoreExpr::raw("nor", conditions))
    }

    /// True when `condition` is false.
    pub fn not(&self, condition: FirestoreBoolExpr) -> FirestoreBoolExpr {
        FirestoreBoolExpr(FirestoreExpr::raw("not", [condition]))
    }

    /// `function` of `conditions`, where a single condition stands for itself and none stands for
    /// the identity of `function`, since Firestore's `and` and `or` take two or more arguments.
    fn connect<I>(&self, function: &str, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        match <[FirestoreBoolExpr; 1]>::try_from(conditions.into_iter().collect::<Vec<_>>()) {
            Ok([condition]) => condition,
            Err(conditions) if conditions.is_empty() => {
                FirestoreBoolExpr(FirestoreExpr::constant(function == "and"))
            }
            Err(conditions) => FirestoreBoolExpr(FirestoreExpr::raw(function, conditions)),
        }
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
        (!present.is_empty()).then(|| self.connect(function, present))
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

#[cfg(test)]
mod tests {
    use crate::pipeline_builder::FirestorePipelineInitialBuilder;
    use crate::{FirestoreLimitStage, FirestorePipeline, FirestorePipelineStage};
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
    fn optional_conditions_join_the_present_ones_into_one_connective() {
        let genre = Some("Dystopian");
        let max_pages: Option<i64> = None;
        let min_rating = Some(4.0);

        let chain = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.field("stock")
                    .greater_than(0)
                    .and_maybe(genre.map(|genre| e.field("genre").eq(genre)))
                    .and_maybe(max_pages.map(|pages| e.field("pages").less_than(pages)))
                    .and_maybe(
                        min_rating.map(|rating| e.field("rating").greater_than_or_equal(rating)),
                    )
            })
            .into_pipeline();
        let list = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.for_all([
                    Some(e.field("stock").greater_than(0)),
                    genre.map(|genre| e.field("genre").eq(genre)),
                    max_pages.map(|pages| e.field("pages").less_than(pages)),
                    min_rating.map(|rating| e.field("rating").greater_than_or_equal(rating)),
                ])
            })
            .into_pipeline();
        let flat = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.and([
                    e.field("stock").greater_than(0),
                    e.field("genre").eq("Dystopian"),
                    e.field("rating").greater_than_or_equal(4.0),
                ])
            })
            .into_pipeline();
        let either = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.field("stock")
                    .greater_than(0)
                    .or_maybe(max_pages.map(|pages| e.field("pages").less_than(pages)))
                    .or_maybe(genre.map(|genre| e.field("genre").eq(genre)))
            })
            .into_pipeline();
        let flat_either = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.or([
                    e.field("stock").greater_than(0),
                    e.field("genre").eq("Dystopian"),
                ])
            })
            .into_pipeline();

        let expected = where_stages(flat);
        assert_eq!(where_stages(chain), expected);
        assert_eq!(where_stages(list), expected);
        assert_eq!(where_stages(either), where_stages(flat_either));
        let Some(ValueType::FunctionValue(and)) = &expected[0].args[0].value_type else {
            panic!("the condition must be a function: {expected:?}");
        };
        assert_eq!((and.name.as_str(), and.args.len()), ("and", 3));
    }

    #[test]
    fn a_single_present_condition_stands_alone() {
        let joined = builder()
            .collection("pipeline-books")
            .filter(|e| e.for_all([None, Some(e.field("stock").greater_than(0)), None]))
            .into_pipeline();
        let alone = builder()
            .collection("pipeline-books")
            .filter(|e| e.field("stock").greater_than(0))
            .into_pipeline();

        assert_eq!(joined, alone);
    }

    #[test]
    fn absent_conditions_add_no_where_stage() {
        let genre: Option<&str> = None;
        let pipeline = builder()
            .collection("pipeline-books")
            .filter(|e| e.for_any([genre.map(|genre| e.field("genre").eq(genre))]))
            .limit(1)
            .into_pipeline();

        assert_eq!(
            pipeline.stages,
            vec![FirestorePipelineStage::Limit(FirestoreLimitStage::new(1))]
        );
    }

    #[test]
    fn for_all_of_plain_conditions_is_the_and_of_them() {
        let all = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.for_all([
                    e.field("stock").greater_than(0),
                    e.field("genre").eq("Dystopian"),
                ])
            })
            .into_pipeline();
        let and = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.and([
                    e.field("stock").greater_than(0),
                    e.field("genre").eq("Dystopian"),
                ])
            })
            .into_pipeline();

        assert_eq!(where_stages(all), where_stages(and));
    }

    #[test]
    fn for_any_of_optional_conditions_drops_the_absent_ones() {
        let genre: Option<&str> = None;
        let any = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.for_any([
                    Some(e.field("stock").greater_than(0)),
                    genre.map(|genre| e.field("genre").eq(genre)),
                    Some(e.field("rating").greater_than_or_equal(4.0)),
                ])
            })
            .into_pipeline();
        let or = builder()
            .collection("pipeline-books")
            .filter(|e| {
                e.or([
                    e.field("stock").greater_than(0),
                    e.field("rating").greater_than_or_equal(4.0),
                ])
            })
            .into_pipeline();

        assert_eq!(where_stages(any), where_stages(or));
    }
}
