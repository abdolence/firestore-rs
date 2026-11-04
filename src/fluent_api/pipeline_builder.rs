use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::pipeline_filter_builder::FirestorePipelineFilterBuilder;
use crate::pipeline_search_builder::FirestorePipelineSearchBuilder;
use crate::{
    FirestoreAddFieldsStage, FirestoreBoolExpr, FirestoreCollectionGroupSource,
    FirestoreCollectionSource, FirestoreDocument, FirestoreDocumentsSource,
    FirestoreExplainOptions, FirestoreIndexHint, FirestoreLimitStage, FirestoreLiteralsSource,
    FirestoreOffsetStage, FirestorePipeline, FirestorePipelineOrdering, FirestorePipelineParams,
    FirestorePipelineSource, FirestorePipelineStage, FirestorePipelineSupport, FirestoreRawStage,
    FirestoreRemoveFieldsStage, FirestoreRequestOptions, FirestoreRequestTag, FirestoreResult,
    FirestoreSearch, FirestoreSelectStage, FirestoreSelection, FirestoreSortStage,
    FirestoreWhereStage, FirestoreWithPipelineMetadata,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// The context of a top-level pipeline: the database it runs on and the options it runs with.
#[derive(Clone, Debug)]
pub struct FirestorePipelineDbContext<'a, D> {
    db: &'a D,
    explain_options: Option<FirestoreExplainOptions>,
    request_options: Option<FirestoreRequestOptions>,
}

impl<'a, D> FirestorePipelineDbContext<'a, D> {
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            explain_options: None,
            request_options: None,
        }
    }
}

/// Picks the source of a pipeline, its first stage.
///
/// Created by [`FirestoreExprBuilder::pipeline()`](crate::FirestoreExprBuilder::pipeline).
#[derive(Clone, Debug)]
pub struct FirestorePipelineInitialBuilder<C> {
    context: C,
}

/// Adds stages to a pipeline and, at the top level, runs it.
///
/// `C` is the context the pipeline is built in. `S` records what the pipeline has so far, so
/// that a method valid only right after a source, such as
/// [`parent`](FirestorePipelineBuilder::parent), is gone once a stage follows.
#[derive(Clone, Debug)]
pub struct FirestorePipelineBuilder<C, S> {
    context: C,
    state: S,
}

/// A pipeline of a single collection source, which can still take a parent and an index hint.
#[derive(Clone, Debug)]
pub struct FirestorePipelineCollectionSource(FirestoreCollectionSource);

/// A pipeline of a single collection-group source, which can still take an index hint.
#[derive(Clone, Debug)]
pub struct FirestorePipelineCollectionGroupSource(FirestoreCollectionGroupSource);

/// A pipeline that takes no more source options.
#[derive(Clone, Debug)]
pub struct FirestorePipelineStaged(FirestorePipeline);

impl From<FirestorePipelineCollectionSource> for FirestorePipeline {
    fn from(state: FirestorePipelineCollectionSource) -> Self {
        FirestorePipeline::new(FirestorePipelineSource::Collection(state.0))
    }
}

impl From<FirestorePipelineCollectionGroupSource> for FirestorePipeline {
    fn from(state: FirestorePipelineCollectionGroupSource) -> Self {
        FirestorePipeline::new(FirestorePipelineSource::CollectionGroup(state.0))
    }
}

impl From<FirestorePipelineStaged> for FirestorePipeline {
    fn from(state: FirestorePipelineStaged) -> Self {
        state.0
    }
}

impl<C> FirestorePipelineInitialBuilder<C> {
    pub(crate) fn new(context: C) -> Self {
        Self { context }
    }

    /// Reads the collection `collection_id`, a root collection unless
    /// [`parent`](FirestorePipelineBuilder::parent) follows.
    pub fn collection<S: AsRef<str>>(
        self,
        collection_id: S,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineCollectionSource> {
        self.source(FirestorePipelineCollectionSource(
            FirestoreCollectionSource::new(collection_id.as_ref().to_string()),
        ))
    }

    /// Reads every collection with the ID `collection_id`, anywhere in the database.
    pub fn collection_group<S: AsRef<str>>(
        self,
        collection_id: S,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineCollectionGroupSource> {
        self.source(FirestorePipelineCollectionGroupSource(
            FirestoreCollectionGroupSource::new(collection_id.as_ref().to_string()),
        ))
    }

    /// Reads every document of the database.
    pub fn database(self) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.staged(FirestorePipelineSource::Database)
    }

    /// Reads the documents at `paths`, relative to the database's documents root, such as
    /// `"books/b1"`. A path that names no document is left out.
    pub fn documents<I>(self, paths: I) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        self.staged(FirestorePipelineSource::Documents(
            FirestoreDocumentsSource::new(
                paths
                    .into_iter()
                    .map(|path| path.as_ref().to_string())
                    .collect(),
            ),
        ))
    }

    /// Starts from `rows` instead of stored documents. Every row must serialise to a map.
    pub fn literals<I>(self, rows: I) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        self.staged(FirestorePipelineSource::Literals(
            FirestoreLiteralsSource::new(rows.into_iter().map(Into::into).collect()),
        ))
    }

    fn source<S>(self, state: S) -> FirestorePipelineBuilder<C, S> {
        FirestorePipelineBuilder {
            context: self.context,
            state,
        }
    }

    pub(crate) fn staged(
        self,
        source: FirestorePipelineSource,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.source(FirestorePipelineStaged(FirestorePipeline::new(source)))
    }
}

impl<'a, D> FirestorePipelineInitialBuilder<FirestorePipelineDbContext<'a, D>> {
    /// Runs `pipeline`, built with the model types rather than this builder.
    pub fn from_pipeline(
        self,
        pipeline: FirestorePipeline,
    ) -> FirestorePipelineBuilder<FirestorePipelineDbContext<'a, D>, FirestorePipelineStaged> {
        self.source(FirestorePipelineStaged(pipeline))
    }
}

impl<C> FirestorePipelineBuilder<C, FirestorePipelineCollectionSource> {
    /// Reads the collection under the document `parent`, a full document path as
    /// [`FirestoreDb::parent_path`](crate::FirestoreDb::parent_path) builds it.
    pub fn parent<P: AsRef<str>>(self, parent: P) -> Self {
        Self {
            state: FirestorePipelineCollectionSource(
                self.state.0.with_parent(parent.as_ref().to_string()),
            ),
            ..self
        }
    }

    /// Reads through the index `hint` names.
    pub fn force_index(self, hint: FirestoreIndexHint) -> Self {
        Self {
            state: FirestorePipelineCollectionSource(self.state.0.with_force_index(hint)),
            ..self
        }
    }
}

impl<C> FirestorePipelineBuilder<C, FirestorePipelineCollectionGroupSource> {
    /// Reads through the index `hint` names.
    pub fn force_index(self, hint: FirestoreIndexHint) -> Self {
        Self {
            state: FirestorePipelineCollectionGroupSource(self.state.0.with_force_index(hint)),
            ..self
        }
    }
}

impl<C> FirestorePipelineBuilder<C, FirestorePipelineCollectionSource> {
    /// Searches the collection, which needs a search index that covers the query. The search
    /// must come straight after the source, so no option of the source follows it.
    ///
    /// ```no_run
    /// use firestore::*;
    ///
    /// # async fn example(db: &FirestoreDb) -> FirestoreResult<()> {
    /// let hits = db
    ///     .fluent()
    ///     .pipeline()
    ///     .collection("restaurants")
    ///     .search(|s| {
    ///         s.text(|t| t.for_all([t.term("waffles"), t.exclude(t.term("bacon"))]))
    ///             .sort_by_score()
    ///             .score_field("score")
    ///             .limit(20)
    ///     })
    ///     .query()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn search<FN, R>(self, search: FN) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineSearchBuilder) -> R,
        R: Into<FirestoreSearch>,
    {
        let search = search(FirestorePipelineSearchBuilder).into();
        Self {
            state: FirestorePipelineCollectionSource(self.state.0.with_search(search)),
            ..self
        }
        .staged()
    }
}

impl<C> FirestorePipelineBuilder<C, FirestorePipelineCollectionGroupSource> {
    /// Searches the collection group, which needs a search index scoped to the collection group.
    /// The search must come straight after the source, so no option of the source follows it.
    pub fn search<FN, R>(self, search: FN) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineSearchBuilder) -> R,
        R: Into<FirestoreSearch>,
    {
        let search = search(FirestorePipelineSearchBuilder).into();
        Self {
            state: FirestorePipelineCollectionGroupSource(self.state.0.with_search(search)),
            ..self
        }
        .staged()
    }
}

impl<C, S> FirestorePipelineBuilder<C, S>
where
    S: Into<FirestorePipeline>,
{
    /// Keeps the documents for which the condition is true. A closure that returns `None`, such
    /// as a [`for_all`](FirestorePipelineFilterBuilder::for_all) of no conditions, adds no stage.
    ///
    /// `q` is the Core-style filter builder: its conditions are `Option`s, so a list may mix plain
    /// and optional ones. Every other closure of a pipeline takes the value builder `e`, whose
    /// conditions are plain.
    pub fn filter<FN>(self, filter: FN) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineFilterBuilder) -> Option<FirestoreBoolExpr>,
    {
        match filter(FirestorePipelineFilterBuilder) {
            Some(condition) => self.stage(FirestorePipelineStage::Where(FirestoreWhereStage::new(
                condition,
            ))),
            None => self.staged(),
        }
    }

    /// Replaces each document with the selected fields. An item that is not a field needs an
    /// [`alias`](crate::FirestoreExpr::alias).
    pub fn select<FN, I>(self, fields: FN) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> I,
        I: IntoIterator,
        I::Item: Into<FirestoreSelection>,
    {
        let fields = fields(FirestorePipelineExprBuilder)
            .into_iter()
            .map(Into::into)
            .collect();
        self.stage(FirestorePipelineStage::Select(FirestoreSelectStage::new(
            fields,
        )))
    }

    /// Adds computed fields to each document, keeping the others.
    pub fn add_fields<FN, I>(
        self,
        fields: FN,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> I,
        I: IntoIterator,
        I::Item: Into<FirestoreSelection>,
    {
        let fields = fields(FirestorePipelineExprBuilder)
            .into_iter()
            .map(Into::into)
            .collect();
        self.stage(FirestorePipelineStage::AddFields(
            FirestoreAddFieldsStage::new(fields),
        ))
    }

    /// Removes the fields at `paths`, such as `paths!(Book::{tags, awards})`, from each document.
    pub fn remove_fields<I>(self, paths: I) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        self.stage(FirestorePipelineStage::RemoveFields(
            FirestoreRemoveFieldsStage::new(
                paths
                    .into_iter()
                    .map(|path| path.as_ref().to_string())
                    .collect(),
            ),
        ))
    }

    /// Orders the documents.
    pub fn sort<FN, I>(self, orderings: FN) -> FirestorePipelineBuilder<C, FirestorePipelineStaged>
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> I,
        I: IntoIterator,
        I::Item: Into<FirestorePipelineOrdering>,
    {
        let orderings = orderings(FirestorePipelineExprBuilder)
            .into_iter()
            .map(Into::into)
            .collect();
        self.stage(FirestorePipelineStage::Sort(FirestoreSortStage::new(
            orderings,
        )))
    }

    /// Keeps the first `limit` documents.
    pub fn limit(self, limit: u32) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.stage(FirestorePipelineStage::Limit(FirestoreLimitStage::new(
            limit,
        )))
    }

    /// Skips the first `offset` documents.
    pub fn offset(self, offset: u32) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.stage(FirestorePipelineStage::Offset(FirestoreOffsetStage::new(
            offset,
        )))
    }

    /// Adds a stage by its wire name, for a stage that has no method of its own.
    pub fn raw_stage(
        self,
        stage: FirestoreRawStage,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        self.stage(FirestorePipelineStage::Raw(stage))
    }

    /// The pipeline built so far, as the model type.
    pub fn into_pipeline(self) -> FirestorePipeline {
        self.state.into()
    }

    fn staged(self) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        FirestorePipelineBuilder {
            context: self.context,
            state: FirestorePipelineStaged(self.state.into()),
        }
    }

    pub(crate) fn stage(
        self,
        stage: FirestorePipelineStage,
    ) -> FirestorePipelineBuilder<C, FirestorePipelineStaged> {
        FirestorePipelineBuilder {
            context: self.context,
            state: FirestorePipelineStaged(self.state.into().with_stage(stage)),
        }
    }
}

impl<'a, D, S> FirestorePipelineBuilder<FirestorePipelineDbContext<'a, D>, S>
where
    D: FirestorePipelineSupport,
    S: Into<FirestorePipeline>,
{
    /// Runs the pipeline with Query Explain for its plan only: no document is read or returned,
    /// and the plan arrives in the metadata of the last item of
    /// [`stream_query_with_metadata`](Self::stream_query_with_metadata).
    pub fn explain(self) -> Self {
        self.explain_with_options(FirestoreExplainOptions::new())
    }

    /// Runs the pipeline with Query Explain. With `analyze` set, the documents are returned too,
    /// and the explain output adds the execution profile.
    pub fn explain_with_options(self, options: FirestoreExplainOptions) -> Self {
        Self {
            context: FirestorePipelineDbContext {
                explain_options: Some(options),
                ..self.context
            },
            ..self
        }
    }

    /// Attaches request options to this pipeline.
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            context: FirestorePipelineDbContext {
                request_options: Some(options),
                ..self.context
            },
            ..self
        }
    }

    /// Attaches request tags to this pipeline. They override any session wide default
    /// configured with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    pub fn request_tags<I>(self, tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(tags))
    }

    /// Deserialises the results into `T`. A result of `select` has no document name, so an
    /// `_firestore_id` field of `T` must be optional.
    pub fn obj<T>(self) -> FirestorePipelineObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        let db = self.context.db;
        FirestorePipelineObjBuilder {
            db,
            params: self.into(),
            _pd: PhantomData,
        }
    }

    /// Runs the pipeline and returns every result.
    pub async fn query(self) -> FirestoreResult<Vec<FirestoreDocument>> {
        let db = self.context.db;
        db.query_pipeline_doc(self.into()).await
    }

    /// Runs the pipeline and streams the results as they arrive, logging and leaving out any
    /// error.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, FirestoreDocument>> {
        let db = self.context.db;
        db.stream_pipeline_doc(self.into()).await
    }

    /// Runs the pipeline and streams the results as they arrive, with an error as an `Err`
    /// item.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        let db = self.context.db;
        db.stream_pipeline_doc_with_errors(self.into()).await
    }

    /// Runs the pipeline and streams each result with the metadata of its response, with an
    /// error as an `Err` item.
    pub async fn stream_query_with_metadata<'b>(
        self,
    ) -> FirestoreResult<
        BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<FirestoreDocument>>>,
    > {
        let db = self.context.db;
        db.stream_pipeline_doc_with_metadata(self.into()).await
    }
}

impl<'a, D, S> From<FirestorePipelineBuilder<FirestorePipelineDbContext<'a, D>, S>>
    for FirestorePipelineParams
where
    S: Into<FirestorePipeline>,
{
    fn from(builder: FirestorePipelineBuilder<FirestorePipelineDbContext<'a, D>, S>) -> Self {
        FirestorePipelineParams::new(builder.state.into())
            .opt_explain_options(builder.context.explain_options)
            .opt_request_options(builder.context.request_options)
    }
}

/// Runs a pipeline and deserialises its results into `T`.
#[derive(Clone, Debug)]
pub struct FirestorePipelineObjBuilder<'a, D, T> {
    db: &'a D,
    params: FirestorePipelineParams,
    _pd: PhantomData<T>,
}

impl<'a, D, T> FirestorePipelineObjBuilder<'a, D, T>
where
    D: FirestorePipelineSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    /// Attaches request options to this pipeline.
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            params: self.params.with_request_options(options),
            ..self
        }
    }

    /// Attaches request tags to this pipeline. They override any session wide default
    /// configured with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    pub fn request_tags<I>(self, tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(tags))
    }

    /// Runs the pipeline and deserialises every result.
    pub async fn query(self) -> FirestoreResult<Vec<T>> {
        self.db.query_pipeline_obj(self.params).await
    }

    /// Runs the pipeline and streams the deserialised results as they arrive, logging and
    /// leaving out any error.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, T>>
    where
        T: 'b,
    {
        self.db.stream_pipeline_obj(self.params).await
    }

    /// Runs the pipeline and streams the deserialised results as they arrive, with an error as
    /// an `Err` item.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        self.db.stream_pipeline_obj_with_errors(self.params).await
    }

    /// Runs the pipeline and streams each deserialised result with the metadata of its
    /// response, with an error as an `Err` item.
    pub async fn stream_query_with_metadata<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<T>>>>
    where
        T: 'b,
    {
        self.db.stream_pipeline_obj_with_metadata(self.params).await
    }
}
