use crate::pipeline_builder::{
    FirestorePipelineBuilder, FirestorePipelineInitialBuilder, FirestorePipelineStaged,
};
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreAggregate, FirestoreAggregateSelection, FirestoreAggregateStage,
    FirestoreDefineVariablesStage, FirestoreDistinctStage, FirestoreExpr,
    FirestoreFindNearestDistanceMeasure, FirestoreFindNearestStage, FirestorePipeline,
    FirestorePipelineSource, FirestorePipelineStage, FirestoreReplaceWithMode,
    FirestoreReplaceWithStage, FirestoreSampleSize, FirestoreSampleStage, FirestoreSelection,
    FirestoreSubcollectionSource, FirestoreUnionStage, FirestoreUnnestStage, FirestoreVector,
};

/// The context of a pipeline nested in another: a subquery or a `union` branch.
///
/// It has no database, so a nested pipeline cannot be run, and it offers `subcollection`, which
/// only a nested pipeline may start with.
#[derive(Clone, Debug)]
pub struct FirestorePipelineNestedContext;

impl FirestorePipelineInitialBuilder<FirestorePipelineNestedContext> {
    /// Reads the collection `collection_id` under the current document of the enclosing pipeline.
    ///
    /// It starts a subquery or a `union` branch. A top-level pipeline cannot start with it, and
    /// does not compile:
    ///
    /// ```compile_fail,E0599
    /// use firestore::FirestoreDb;
    ///
    /// fn top_level(db: &FirestoreDb) {
    ///     db.fluent().pipeline().select().subcollection("reviews");
    /// }
    /// ```
    ///
    /// Inside a subquery, it does:
    ///
    /// ```no_run
    /// use firestore::FirestoreDb;
    ///
    /// # async fn example(db: &FirestoreDb) -> firestore::FirestoreResult<()> {
    /// db.fluent()
    ///     .pipeline()
    ///     .select()
    ///     .from("restaurants")
    ///     .fields(|e| {
    ///         [e.array_subquery(|p| p.subcollection("reviews")).alias("reviews")]
    ///     })
    ///     .query()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subcollection<S: AsRef<str>>(
        self,
        collection_id: S,
    ) -> FirestorePipelineBuilder<FirestorePipelineNestedContext, FirestorePipelineStaged> {
        self.staged(FirestorePipelineSource::Subcollection(
            FirestoreSubcollectionSource::new(collection_id.as_ref().to_string()),
        ))
    }

    /// The nested pipeline that `build` returns.
    pub(crate) fn pipeline_of<FN, R>(build: FN) -> FirestorePipeline
    where
        FN: FnOnce(Self) -> R,
        R: Into<FirestorePipeline>,
    {
        build(Self::new(FirestorePipelineNestedContext)).into()
    }
}

impl<S> From<FirestorePipelineBuilder<FirestorePipelineNestedContext, S>> for FirestorePipeline
where
    S: Into<FirestorePipeline>,
{
    fn from(builder: FirestorePipelineBuilder<FirestorePipelineNestedContext, S>) -> Self {
        builder.into_pipeline()
    }
}

impl<C, S> FirestorePipelineBuilder<C, S>
where
    S: Into<FirestorePipeline>,
{
    /// Computes accumulators over all documents, such as `e.count_all().alias("total")`, and
    /// outputs one document that holds them.
    pub fn aggregate<FN, I>(
        self,
        accumulators: FN,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> I,
        I: IntoIterator,
        I::Item: Into<FirestoreAggregateSelection>,
    {
        self.aggregate_grouped(|_| Vec::<FirestoreSelection>::new(), accumulators)
    }

    /// Computes accumulators per group, and outputs one document per group that holds the
    /// group's values and its accumulators. A null value is a group of its own.
    pub fn aggregate_grouped<FG, IG, FA, IA>(
        self,
        groups: FG,
        accumulators: FA,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FG: FnOnce(FirestorePipelineExprBuilder) -> IG,
        IG: IntoIterator,
        IG::Item: Into<FirestoreSelection>,
        FA: FnOnce(FirestorePipelineExprBuilder) -> IA,
        IA: IntoIterator,
        IA::Item: Into<FirestoreAggregateSelection>,
    {
        self.stage(FirestorePipelineStage::Aggregate(
            FirestoreAggregateStage::new(build_all(accumulators)).with_groups(build_all(groups)),
        ))
    }

    /// Outputs one document per distinct combination of the selected values. A null value is a
    /// group of its own.
    pub fn distinct<FN, I>(self, groups: FN) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> I,
        I: IntoIterator,
        I::Item: Into<FirestoreSelection>,
    {
        self.stage(FirestorePipelineStage::Distinct(
            FirestoreDistinctStage::new(build_all(groups)),
        ))
    }

    /// Keeps the `neighbors_limit` documents whose vector field `field_name` is nearest to
    /// `vector`.
    ///
    /// The stage scans every document and sorts by distance: unlike a Core query, it does not use
    /// a vector index.
    pub fn find_nearest<F: AsRef<str>>(
        self,
        field_name: F,
        vector: FirestoreVector,
        measure: FirestoreFindNearestDistanceMeasure,
        neighbors_limit: u32,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.find_nearest_with_options(FirestoreFindNearestStage::new(
            field_name.as_ref().to_string(),
            vector,
            measure,
            neighbors_limit,
        ))
    }

    /// [`find_nearest`](Self::find_nearest) with the field to output the distance under.
    pub fn find_nearest_with_options(
        self,
        options: FirestoreFindNearestStage,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.stage(FirestorePipelineStage::FindNearest(options))
    }

    /// Keeps a random sample of the documents.
    pub fn sample(
        self,
        size: FirestoreSampleSize,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.stage(FirestorePipelineStage::Sample(FirestoreSampleStage::new(
            size,
        )))
    }

    /// Appends the documents of the pipeline that `other` builds, which starts from the same
    /// builder as a subquery does.
    pub fn union<FN, R>(self, other: FN) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineInitialBuilder<FirestorePipelineNestedContext>) -> R,
        R: Into<FirestorePipeline>,
    {
        self.stage(FirestorePipelineStage::Union(FirestoreUnionStage::new(
            FirestorePipelineInitialBuilder::pipeline_of(other),
        )))
    }

    /// Outputs one document per element of an array, with the element under the alias of the
    /// selection: `e.field("tags").alias("tag")`. An unaliased field is replaced by its own
    /// elements.
    pub fn unnest<FN, R>(
        self,
        selection: FN,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> R,
        R: Into<FirestoreSelection>,
    {
        self.stage(FirestorePipelineStage::Unnest(FirestoreUnnestStage::new(
            selection(FirestorePipelineExprBuilder).into(),
        )))
    }

    /// [`unnest`](Self::unnest), also outputting each element's position in the array under
    /// `index_field`.
    pub fn unnest_with_index_field<FN, R, F>(
        self,
        selection: FN,
        index_field: F,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> R,
        R: Into<FirestoreSelection>,
        F: AsRef<str>,
    {
        self.stage(FirestorePipelineStage::Unnest(
            FirestoreUnnestStage::new(selection(FirestorePipelineExprBuilder).into())
                .with_index_field(index_field.as_ref().to_string()),
        ))
    }

    /// Replaces each document with the map that `expr` evaluates to, in the way `mode` says.
    pub fn replace_with<FN>(
        self,
        expr: FN,
        mode: FirestoreReplaceWithMode,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> FirestoreExpr,
    {
        self.stage(FirestorePipelineStage::ReplaceWith(
            FirestoreReplaceWithStage::new(expr(FirestorePipelineExprBuilder), mode),
        ))
    }

    /// Defines variables for the stages that follow, which read them with
    /// [`variable`](FirestorePipelineExprBuilder::variable). The wire stage is `let`, which is a
    /// Rust keyword.
    #[doc(alias = "define")]
    #[doc(alias = "let")]
    pub fn define_variables<FN, I>(
        self,
        variables: FN,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> I,
        I: IntoIterator,
        I::Item: Into<FirestoreSelection>,
    {
        self.stage(FirestorePipelineStage::DefineVariables(
            FirestoreDefineVariablesStage::new(build_all(variables)),
        ))
    }
}

impl FirestorePipelineExprBuilder {
    /// The results of a pipeline run for each document, as an array: empty when it finds
    /// nothing. Each result is a map, or just the value when the pipeline outputs a single field.
    ///
    /// Variables defined with `define_variables` before this expression are visible inside it.
    /// Give the expression an alias to select it.
    pub fn array_subquery<FN, R>(&self, subquery: FN) -> FirestoreExpr
    where
        FN: FnOnce(FirestorePipelineInitialBuilder<FirestorePipelineNestedContext>) -> R,
        R: Into<FirestorePipeline>,
    {
        FirestoreExpr::ArraySubquery(FirestorePipelineInitialBuilder::pipeline_of(subquery))
    }

    /// The single result of a pipeline run for each document: null when it finds nothing, and
    /// the value of the field when it outputs a single one. A pipeline that finds several
    /// results fails the request as `INVALID_ARGUMENT`.
    ///
    /// Variables defined with `define_variables` before this expression are visible inside it.
    pub fn scalar_subquery<FN, R>(&self, subquery: FN) -> FirestoreExpr
    where
        FN: FnOnce(FirestorePipelineInitialBuilder<FirestorePipelineNestedContext>) -> R,
        R: Into<FirestorePipeline>,
    {
        FirestoreExpr::ScalarSubquery(FirestorePipelineInitialBuilder::pipeline_of(subquery))
    }

    /// Counts all documents of the group, whether or not they have any field.
    pub fn count_all(&self) -> FirestoreAggregate {
        FirestoreAggregate::raw("count", Vec::<FirestoreExpr>::new())
    }
}

/// The items that the closure `build` returns, converted to `T`.
pub(crate) fn build_all<FN, I, T>(build: FN) -> Vec<T>
where
    FN: FnOnce(FirestorePipelineExprBuilder) -> I,
    I: IntoIterator,
    I::Item: Into<T>,
{
    build(FirestorePipelineExprBuilder)
        .into_iter()
        .map(Into::into)
        .collect()
}
