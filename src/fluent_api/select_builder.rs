//! Builder for constructing Firestore select (query) operations.
//!
//! This module provides a fluent API for building complex queries to retrieve documents
//! from Firestore. It supports filtering, ordering, limiting, pagination (cursors),
//! projections, and fetching documents by ID. It also serves as a base for
//! aggregation queries and real-time listeners.

use crate::errors::FirestoreError;
use crate::select_aggregation_builder::FirestoreAggregationBuilder;
use crate::select_filter_builder::FirestoreQueryFilterBuilder;
use crate::{
    FirestoreAggregatedQueryParams, FirestoreAggregatedQuerySupport, FirestoreAggregation,
    FirestoreCollectionDocuments, FirestoreExplainOptions, FirestoreFindNearestDistanceMeasure,
    FirestoreFindNearestOptions, FirestoreGetByIdSupport, FirestoreListenSupport,
    FirestoreListener, FirestoreListenerParams, FirestoreListenerTarget,
    FirestoreListenerTargetParams, FirestorePartition, FirestorePartitionQueryParams,
    FirestoreQueryCollection, FirestoreQueryCursor, FirestoreQueryFilter, FirestoreQueryOrder,
    FirestoreQueryParams, FirestoreQuerySupport, FirestoreResult, FirestoreResumeStateStorage,
    FirestoreTargetType, FirestoreVector, FirestoreWithMetadata,
};
use futures::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::Document;
use serde::Deserialize;
use std::collections::HashMap;
use std::marker::PhantomData;

/// The initial builder for a Firestore select/query operation.
///
/// Created by calling [`FirestoreExprBuilder::select()`](crate::FirestoreExprBuilder::select).
/// This builder allows specifying initial query parameters like projections (fields to return)
/// before defining the target collection or document IDs.
#[derive(Clone, Debug)]
pub struct FirestoreSelectInitialBuilder<'a, D>
where
    D: FirestoreQuerySupport
        + FirestoreGetByIdSupport
        + FirestoreListenSupport
        + FirestoreAggregatedQuerySupport
        + Clone
        + 'static,
{
    db: &'a D,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreSelectInitialBuilder<'a, D>
where
    D: FirestoreQuerySupport
        + FirestoreGetByIdSupport
        + FirestoreListenSupport
        + FirestoreAggregatedQuerySupport
        + Clone
        + Send
        + Sync
        + 'static,
{
    /// Creates a new `FirestoreSelectInitialBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            return_only_fields: None,
        }
    }

    /// Specifies which fields of the documents should be returned by the query (projection).
    ///
    /// If not set, all fields are returned.
    ///
    /// # Arguments
    /// * `return_only_fields`: An iterator of field paths to return.
    ///
    /// # Returns
    /// The builder instance with the projection mask set.
    #[inline]
    pub fn fields<I>(self, return_only_fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            return_only_fields: Some(
                return_only_fields
                    .into_iter()
                    .map(|field| field.as_ref().to_string())
                    .collect(),
            ),
            ..self
        }
    }

    /// Specifies the collection or collection group to query documents from.
    ///
    /// # Arguments
    /// * `collection`: The ID of the collection or a [`FirestoreQueryCollection`] enum
    ///   specifying a single collection or a collection group.
    ///
    /// # Returns
    /// A [`FirestoreSelectDocBuilder`] to further configure the query (filters, ordering, etc.).
    #[inline]
    pub fn from<C>(self, collection: C) -> FirestoreSelectDocBuilder<'a, D>
    where
        C: Into<FirestoreQueryCollection>,
    {
        let params: FirestoreQueryParams = FirestoreQueryParams::new(collection.into())
            .opt_return_only_fields(self.return_only_fields);
        FirestoreSelectDocBuilder::new(self.db, params)
    }

    /// Specifies that documents should be fetched by their IDs from a specific collection.
    ///
    /// # Arguments
    /// * `collection`: The ID of the collection where the documents reside.
    ///
    /// # Returns
    /// A [`FirestoreSelectByIdBuilder`] to specify the document IDs and other options.
    #[inline]
    pub fn by_id_in(self, collection: &str) -> FirestoreSelectByIdBuilder<'a, D> {
        FirestoreSelectByIdBuilder::new(self.db, collection.to_string(), self.return_only_fields)
    }
}

/// A builder for configuring and executing a Firestore query on a collection.
///
/// This builder allows setting filters, ordering, limits, cursors, and other
/// query parameters.
#[derive(Clone, Debug)]
pub struct FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport
        + FirestoreListenSupport
        + FirestoreAggregatedQuerySupport
        + Clone
        + Send
        + Sync,
{
    db: &'a D,
    params: FirestoreQueryParams,
}

impl<'a, D> FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport
        + FirestoreListenSupport
        + FirestoreAggregatedQuerySupport
        + Clone
        + Send
        + Sync
        + 'static,
{
    /// Creates a new `FirestoreSelectDocBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreQueryParams) -> Self {
        Self { db, params }
    }

    /// Specifies the parent document path for querying a sub-collection.
    ///
    /// # Arguments
    /// * `parent`: The full path to the parent document.
    ///
    /// # Returns
    /// The builder instance with the parent path set.
    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            params: self.params.with_parent(parent.as_ref().to_string()),
            ..self
        }
    }

    /// Sets the maximum number of documents to return.
    ///
    /// # Arguments
    /// * `value`: The limit.
    ///
    /// # Returns
    /// The builder instance with the limit set.
    #[inline]
    pub fn limit(self, value: u32) -> Self {
        Self {
            params: self.params.with_limit(value),
            ..self
        }
    }

    /// Sets the number of documents to skip before returning results.
    ///
    /// # Arguments
    /// * `value`: The offset.
    ///
    /// # Returns
    /// The builder instance with the offset set.
    #[inline]
    pub fn offset(self, value: u32) -> Self {
        Self {
            params: self.params.with_offset(value),
            ..self
        }
    }

    /// Specifies the order in which to sort the query results.
    ///
    /// Can be called multiple times to order by multiple fields.
    ///
    /// # Arguments
    /// * `fields`: An iterator of [`FirestoreQueryOrder`] specifying fields and directions.
    ///
    /// # Returns
    /// The builder instance with the ordering set.
    #[inline]
    pub fn order_by<I>(self, fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreQueryOrder>,
    {
        Self {
            params: self
                .params
                .with_order_by(fields.into_iter().map(|field| field.into()).collect()),
            ..self
        }
    }

    /// Sets the starting point for the query results using a cursor.
    ///
    /// # Arguments
    /// * `cursor`: A [`FirestoreQueryCursor`] defining the start point.
    ///
    /// # Returns
    /// The builder instance with the start cursor set.
    #[inline]
    pub fn start_at(self, cursor: FirestoreQueryCursor) -> Self {
        Self {
            params: self.params.with_start_at(cursor),
            ..self
        }
    }

    /// Sets the ending point for the query results using a cursor.
    ///
    /// # Arguments
    /// * `cursor`: A [`FirestoreQueryCursor`] defining the end point.
    ///
    /// # Returns
    /// The builder instance with the end cursor set.
    #[inline]
    pub fn end_at(self, cursor: FirestoreQueryCursor) -> Self {
        Self {
            params: self.params.with_end_at(cursor),
            ..self
        }
    }

    /// Configures the query to search all collections with the specified ID
    /// under the parent path (for collection group queries).
    ///
    /// # Returns
    /// The builder instance configured for a collection group query.
    #[inline]
    pub fn all_descendants(self) -> Self {
        Self {
            params: self.params.with_all_descendants(true),
            ..self
        }
    }

    /// Applies a filter to the query.
    ///
    /// The `filter` argument is a closure that receives a [`FirestoreQueryFilterBuilder`]
    /// and should return an `Option<FirestoreQueryFilter>`.
    ///
    /// # Arguments
    /// * `filter`: A closure to build the query filter.
    ///
    /// # Returns
    /// The builder instance with the filter applied.
    #[inline]
    pub fn filter<FN>(self, filter: FN) -> Self
    where
        FN: Fn(FirestoreQueryFilterBuilder) -> Option<FirestoreQueryFilter>,
    {
        let filter_builder = FirestoreQueryFilterBuilder::new();

        Self {
            params: self.params.opt_filter(filter(filter_builder)),
            ..self
        }
    }

    /// Requests an explanation of the query execution plan from Firestore.
    ///
    /// The explanation metrics will be available in the metadata of the query response.
    ///
    /// # Returns
    /// The builder instance with explain options enabled.
    #[inline]
    pub fn explain(self) -> FirestoreSelectDocBuilder<'a, D> {
        Self {
            params: self
                .params
                .with_explain_options(FirestoreExplainOptions::new()), // Default explain options
            ..self
        }
    }

    /// Configures a vector similarity search (find nearest neighbors).
    ///
    /// # Arguments
    /// * `field_name`: The path to the vector field.
    /// * `vector`: The query vector.
    /// * `measure`: The distance measure to use.
    /// * `neighbors_limit`: The maximum number of neighbors to return.
    ///
    /// # Returns
    /// The builder instance configured for a vector search.
    #[inline]
    pub fn find_nearest<F>(
        self,
        field_name: F,
        vector: FirestoreVector,
        measure: FirestoreFindNearestDistanceMeasure,
        neighbors_limit: u32,
    ) -> FirestoreSelectDocBuilder<'a, D>
    where
        F: AsRef<str>,
    {
        self.find_nearest_with_options(FirestoreFindNearestOptions::new(
            field_name.as_ref().to_string(),
            vector,
            measure,
            neighbors_limit,
        ))
    }

    /// Configures a vector similarity search with detailed options.
    ///
    /// # Arguments
    /// * `options`: [`FirestoreFindNearestOptions`] specifying the vector search parameters.
    ///
    /// # Returns
    /// The builder instance configured for a vector search.
    #[inline]
    pub fn find_nearest_with_options(
        self,
        options: FirestoreFindNearestOptions,
    ) -> FirestoreSelectDocBuilder<'a, D> {
        Self {
            params: self.params.with_find_nearest(options),
            ..self
        }
    }

    /// Requests an explanation of the query execution plan with specific options.
    ///
    /// # Arguments
    /// * `options`: [`FirestoreExplainOptions`] specifying the analysis options.
    ///
    /// # Returns
    /// The builder instance with the specified explain options.
    #[inline]
    pub fn explain_with_options(
        self,
        options: FirestoreExplainOptions,
    ) -> FirestoreSelectDocBuilder<'a, D> {
        Self {
            params: self.params.with_explain_options(options),
            ..self
        }
    }

    /// Specifies that the query results should be deserialized into a specific Rust type `T`.
    ///
    /// # Type Parameters
    /// * `T`: The type to deserialize documents into. Must implement `serde::Deserialize`.
    ///
    /// # Returns
    /// A [`FirestoreSelectObjBuilder`] for executing the query and streaming deserialized objects.
    #[inline]
    pub fn obj<T>(self) -> FirestoreSelectObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreSelectObjBuilder::new(self.db, self.params)
    }

    /// Configures the query as a partitioned query.
    ///
    /// Partitioned queries are used to divide a large dataset into smaller chunks
    /// that can be processed in parallel.
    ///
    /// # Returns
    /// A [`FirestorePartitionQueryDocBuilder`] to configure and stream partitions.
    #[inline]
    pub fn partition_query(self) -> FirestorePartitionQueryDocBuilder<'a, D> {
        FirestorePartitionQueryDocBuilder::new(self.db, self.params.with_all_descendants(true))
    }

    /// Sets up a real-time listener for changes to the documents matching this query.
    ///
    /// # Returns
    /// A [`FirestoreDocChangesListenerInitBuilder`] to configure and start the listener.
    #[inline]
    pub fn listen(self) -> FirestoreDocChangesListenerInitBuilder<'a, D> {
        FirestoreDocChangesListenerInitBuilder::new(
            self.db,
            FirestoreTargetType::Query(self.params),
        )
    }

    /// Specifies aggregations to be performed over the documents matching this query.
    ///
    /// The `aggregation` argument is a closure that receives a [`FirestoreAggregationBuilder`]
    /// and should return a `Vec<FirestoreAggregation>`.
    ///
    /// # Arguments
    /// * `aggregation`: A closure to build the list of aggregations.
    ///
    /// # Returns
    /// A [`FirestoreAggregatedQueryDocBuilder`] to execute the aggregation query.
    #[inline]
    pub fn aggregate<FN>(self, aggregation: FN) -> FirestoreAggregatedQueryDocBuilder<'a, D>
    where
        FN: Fn(FirestoreAggregationBuilder) -> Vec<FirestoreAggregation>,
    {
        FirestoreAggregatedQueryDocBuilder::new(
            self.db,
            FirestoreAggregatedQueryParams::new(
                self.params,
                aggregation(FirestoreAggregationBuilder::new()),
            ),
        )
    }

    /// Executes the configured query and retrieves all matching documents.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `Vec` of [`Document`]s.
    pub async fn query(self) -> FirestoreResult<Vec<Document>> {
        self.db.query_doc(self.params).await
    }

    /// Executes the configured query and returns a stream of matching documents.
    ///
    /// Errors encountered during streaming will terminate the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of [`Document`]s.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, Document>> {
        self.db.stream_query_doc(self.params).await
    }

    /// Executes the configured query and returns a stream of `FirestoreResult<Document>`.
    ///
    /// Errors encountered during streaming are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<Document>`.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        self.db.stream_query_doc_with_errors(self.params).await
    }

    /// Executes the query and returns a stream of documents along with their metadata.
    ///
    /// Errors are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<FirestoreWithMetadata<Document>>`.
    pub async fn stream_query_with_metadata<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithMetadata<Document>>>> {
        self.db.stream_query_doc_with_metadata(self.params).await
    }
}

/// A builder for executing a query and deserializing results into a Rust type `T`.
#[derive(Clone, Debug)]
pub struct FirestoreSelectObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    params: FirestoreQueryParams,
    _pd: PhantomData<T>,
}

impl<'a, D, T> FirestoreSelectObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    /// Creates a new `FirestoreSelectObjBuilder`.
    pub(crate) fn new(
        db: &'a D,
        params: FirestoreQueryParams,
    ) -> FirestoreSelectObjBuilder<'a, D, T> {
        Self {
            db,
            params,
            _pd: PhantomData,
        }
    }

    /// Executes the query and deserializes all matching documents into a `Vec<T>`.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `Vec<T>`.
    pub async fn query(self) -> FirestoreResult<Vec<T>> {
        self.db.query_obj(self.params).await
    }

    /// Executes the query and returns a stream of deserialized objects `T`.
    ///
    /// Errors during streaming or deserialization will terminate the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `T`.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, T>>
    where
        T: 'b,
    {
        self.db.stream_query_obj(self.params).await
    }

    /// Executes the query and returns a stream of `FirestoreResult<T>`.
    ///
    /// Errors during streaming or deserialization are yielded as `Err` items.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<T>`.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        self.db.stream_query_obj_with_errors(self.params).await
    }

    /// Executes the query and returns a stream of deserialized objects `T` along with their metadata.
    ///
    /// Errors are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<FirestoreWithMetadata<T>>`.
    pub async fn stream_query_with_metadata<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithMetadata<T>>>>
    where
        T: 'b,
    {
        self.db.stream_query_obj_with_metadata(self.params).await
    }

    /// Configures the query as a partitioned query for deserialized objects.
    ///
    /// # Returns
    /// A [`FirestorePartitionQueryObjBuilder`] to configure and stream partitions of `T`.
    pub fn partition_query(self) -> FirestorePartitionQueryObjBuilder<'a, D, T>
    where
        T: 'a, // Ensure T lives as long as the builder
    {
        FirestorePartitionQueryObjBuilder::new(self.db, self.params.with_all_descendants(true))
    }
}

/// A builder for selecting documents by their IDs from a collection.
#[derive(Clone, Debug)]
pub struct FirestoreSelectByIdBuilder<'a, D>
where
    D: FirestoreGetByIdSupport,
{
    db: &'a D,
    collection: String,
    parent: Option<String>,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreSelectByIdBuilder<'a, D>
where
    D: FirestoreGetByIdSupport + FirestoreListenSupport + Send + Sync + Clone + 'static,
{
    /// Creates a new `FirestoreSelectByIdBuilder`.
    pub(crate) fn new(
        db: &'a D,
        collection: String,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreSelectByIdBuilder<'a, D> {
        Self {
            db,
            collection,
            parent: None,
            return_only_fields,
        }
    }

    /// Specifies the parent document path for selecting documents from a sub-collection.
    ///
    /// # Arguments
    /// * `parent`: The full path to the parent document.
    ///
    /// # Returns
    /// The builder instance with the parent path set.
    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }

    /// Specifies that the fetched documents should be deserialized into a specific Rust type `T`.
    ///
    /// # Type Parameters
    /// * `T`: The type to deserialize documents into. Must implement `serde::Deserialize`.
    ///
    /// # Returns
    /// A [`FirestoreSelectObjByIdBuilder`] for fetching and deserializing documents by ID.
    #[inline]
    pub fn obj<T>(self) -> FirestoreSelectObjByIdBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreSelectObjByIdBuilder::new(
            self.db,
            self.collection,
            self.parent,
            self.return_only_fields,
        )
    }

    /// Fetches a single document by its ID.
    ///
    /// # Arguments
    /// * `document_id`: The ID of the document to fetch.
    ///
    /// # Returns
    /// A `FirestoreResult` containing an `Option<Document>`. `None` if the document doesn't exist.
    pub async fn one<S>(self, document_id: S) -> FirestoreResult<Option<Document>>
    where
        S: AsRef<str> + Send,
    {
        if let Some(parent) = self.parent {
            match self
                .db
                .get_doc_at::<S>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        } else {
            match self
                .db
                .get_doc::<S>(
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        }
    }

    /// Fetches multiple documents by their IDs in a batch.
    ///
    /// Returns a stream of `(String, Option<Document>)` tuples, where the string is the document ID.
    /// Errors during fetching will terminate the stream.
    ///
    /// # Arguments
    /// * `document_ids`: An iterator of document IDs to fetch.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `(String, Option<Document>)`.
    pub async fn batch<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_docs_at::<S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_docs::<S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }

    /// Fetches multiple documents by their IDs in a batch, yielding `FirestoreResult` for each.
    ///
    /// Errors during fetching for individual documents are yielded as `Err` items in the stream.
    ///
    /// # Arguments
    /// * `document_ids`: An iterator of document IDs to fetch.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<(String, Option<Document>)>`.
    pub async fn batch_with_errors<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_docs_at_with_errors::<S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_docs_with_errors::<S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }

    /// Sets up a real-time listener for changes to a specific set of documents by their IDs.
    ///
    /// # Arguments
    /// * `document_ids`: An iterator of document IDs to listen to.
    ///
    /// # Returns
    /// A [`FirestoreDocChangesListenerInitBuilder`] to configure and start the listener.
    pub fn batch_listen<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreDocChangesListenerInitBuilder<'a, D>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        FirestoreDocChangesListenerInitBuilder::new(
            self.db,
            FirestoreTargetType::Documents(
                FirestoreCollectionDocuments::new(
                    self.collection,
                    document_ids
                        .into_iter()
                        .map(|s| s.as_ref().to_string())
                        .collect(),
                )
                .opt_parent(self.parent),
            ),
        )
    }
}

/// A builder for fetching documents by ID and deserializing them into a Rust type `T`.
#[derive(Clone, Debug)]
pub struct FirestoreSelectObjByIdBuilder<'a, D, T>
where
    D: FirestoreGetByIdSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    collection: String,
    parent: Option<String>,
    return_only_fields: Option<Vec<String>>,
    _pd: PhantomData<T>,
}

impl<'a, D, T> FirestoreSelectObjByIdBuilder<'a, D, T>
where
    D: FirestoreGetByIdSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    /// Creates a new `FirestoreSelectObjByIdBuilder`.
    pub(crate) fn new(
        db: &'a D,
        collection: String,
        parent: Option<String>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreSelectObjByIdBuilder<'a, D, T> {
        Self {
            db,
            collection,
            parent,
            return_only_fields,
            _pd: PhantomData,
        }
    }

    /// Fetches a single document by its ID and deserializes it into type `T`.
    ///
    /// # Arguments
    /// * `document_id`: The ID of the document to fetch.
    ///
    /// # Returns
    /// A `FirestoreResult` containing an `Option<T>`. `None` if the document doesn't exist.
    pub async fn one<S>(self, document_id: S) -> FirestoreResult<Option<T>>
    where
        S: AsRef<str> + Send,
    {
        if let Some(parent) = self.parent {
            match self
                .db
                .get_obj_at_return_fields::<T, S>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        } else {
            match self
                .db
                .get_obj_return_fields::<T, S>(
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        }
    }

    /// Fetches multiple documents by their IDs in a batch and deserializes them into type `T`.
    ///
    /// Returns a stream of `(String, Option<T>)` tuples.
    /// Errors during fetching or deserialization will terminate the stream.
    ///
    /// # Arguments
    /// * `document_ids`: An iterator of document IDs to fetch.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `(String, Option<T>)`.
    pub async fn batch<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
        T: Send + 'a,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_objects_at::<T, S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_objects::<T, S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }

    /// Fetches multiple documents by IDs in a batch, deserializing them and yielding `FirestoreResult`.
    ///
    /// Errors during fetching or deserialization for individual documents are yielded as `Err` items.
    ///
    /// # Arguments
    /// * `document_ids`: An iterator of document IDs to fetch.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<(String, Option<T>)>`.
    pub async fn batch_with_errors<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
        T: Send + 'a,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_objects_at_with_errors::<T, S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_objects_with_errors::<T, S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }
}

/// A builder for configuring and executing a partitioned query for documents.
#[derive(Clone, Debug)]
pub struct FirestorePartitionQueryDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    db: &'a D,
    params: FirestoreQueryParams,
    parallelism: usize,
    partition_count: u32,
    page_size: u32,
}

impl<'a, D> FirestorePartitionQueryDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    /// Creates a new `FirestorePartitionQueryDocBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreQueryParams) -> Self {
        Self {
            db,
            params,
            parallelism: 2,      // Default parallelism
            partition_count: 10, // Default number of partitions to request
            page_size: 1000,     // Default page size for fetching partitions
        }
    }

    /// Sets the desired parallelism for processing partitions.
    ///
    /// This hints at how many partitions might be processed concurrently by the caller.
    ///
    /// # Arguments
    /// * `max_threads`: The desired level of parallelism.
    ///
    /// # Returns
    /// The builder instance with the parallelism level set.
    #[inline]
    pub fn parallelism(self, max_threads: usize) -> Self {
        Self {
            parallelism: max_threads,
            ..self
        }
    }

    /// Sets the desired number of partitions to divide the query into.
    ///
    /// # Arguments
    /// * `count`: The number of partitions.
    ///
    /// # Returns
    /// The builder instance with the partition count set.
    #[inline]
    pub fn partition_count(self, count: u32) -> Self {
        Self {
            partition_count: count,
            ..self
        }
    }

    /// Sets the page size for retrieving partition cursors.
    ///
    /// This controls how many partition definitions are fetched in each request to the server.
    ///
    /// # Arguments
    /// * `len`: The page size for partition cursors.
    ///
    /// # Returns
    /// The builder instance with the partition page size set.
    #[inline]
    pub fn page_size(self, len: u32) -> Self {
        Self {
            page_size: len,
            ..self
        }
    }

    /// Streams query partitions along with the documents within each partition.
    ///
    /// Errors are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<(FirestorePartition, Document)>`.
    pub async fn stream_partitions_with_errors(
        self,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(FirestorePartition, Document)>>> {
        self.db
            .stream_partition_query_doc_with_errors(
                self.parallelism,
                FirestorePartitionQueryParams::new(
                    self.params,
                    self.partition_count,
                    self.page_size,
                ),
            )
            .await
    }
}

/// A builder for partitioned queries that deserialize results into a Rust type `T`.
#[derive(Clone, Debug)]
pub struct FirestorePartitionQueryObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    params: FirestoreQueryParams,
    parallelism: usize,
    partition_count: u32,
    page_size: u32,
    _ph: PhantomData<T>,
}

impl<'a, D, T> FirestorePartitionQueryObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send + 'a,
    for<'de> T: Deserialize<'de>,
{
    /// Creates a new `FirestorePartitionQueryObjBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreQueryParams) -> Self {
        Self {
            db,
            params,
            parallelism: 2,
            partition_count: 10,
            page_size: 1000,
            _ph: PhantomData,
        }
    }

    /// Sets the desired parallelism for processing partitions.
    #[inline]
    pub fn parallelism(self, max_threads: usize) -> Self {
        Self {
            parallelism: max_threads,
            ..self
        }
    }

    /// Sets the desired number of partitions.
    #[inline]
    pub fn partition_count(self, count: u32) -> Self {
        Self {
            partition_count: count,
            ..self
        }
    }

    /// Sets the page size for retrieving partition cursors.
    #[inline]
    pub fn page_size(self, len: u32) -> Self {
        Self {
            page_size: len,
            ..self
        }
    }

    /// Streams query partitions along with the deserialized objects `T` within each partition.
    ///
    /// Errors are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<(FirestorePartition, T)>`.
    pub async fn stream_partitions_with_errors(
        self,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(FirestorePartition, T)>>> {
        self.db
            .stream_partition_query_obj_with_errors(
                self.parallelism,
                FirestorePartitionQueryParams::new(
                    self.params,
                    self.partition_count,
                    self.page_size,
                ),
            )
            .await
    }
}

/// Builder for initializing a Firestore document changes listener.
#[derive(Clone, Debug)]
pub struct FirestoreDocChangesListenerInitBuilder<'a, D>
where
    D: FirestoreListenSupport + Clone,
{
    _db: &'a D, // Keep a reference to the DB for lifetime, though not directly used in all methods
    listener_params: FirestoreListenerParams,
    target_type: FirestoreTargetType,
    labels: HashMap<String, String>,
}

impl<'a, D> FirestoreDocChangesListenerInitBuilder<'a, D>
where
    D: FirestoreListenSupport + Clone + Send + Sync + 'static,
{
    /// Creates a new `FirestoreDocChangesListenerInitBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, target_type: FirestoreTargetType) -> Self {
        Self {
            _db: db,
            listener_params: FirestoreListenerParams::new(),
            target_type,
            labels: HashMap::new(),
        }
    }

    /// Sets labels for the listener.
    ///
    /// Labels are key-value pairs that can be used to identify or categorize listeners.
    ///
    /// # Arguments
    /// * `labels`: A map of labels.
    ///
    /// # Returns
    /// The builder instance with labels set.
    #[inline]
    pub fn labels(self, labels: HashMap<String, String>) -> Self {
        Self { labels, ..self }
    }

    /// Sets the initial delay for retrying the listener connection on failure.
    ///
    /// # Arguments
    /// * `delay`: The retry delay duration.
    ///
    /// # Returns
    /// The builder instance with the retry delay set.
    #[inline]
    pub fn retry_delay(self, delay: std::time::Duration) -> Self {
        Self {
            listener_params: self.listener_params.with_retry_delay(delay),
            ..self
        }
    }

    /// Adds the configured target to an existing [`FirestoreListener`].
    ///
    /// This method finalizes the listener target configuration and registers it
    /// with the provided listener instance.
    ///
    /// # Arguments
    /// * `target`: The specific target (e.g., document path, query) for the listener.
    /// * `listener`: A mutable reference to the `FirestoreListener` to add this target to.
    ///
    /// # Type Parameters
    /// * `S`: The type of storage used for persisting resume states for the listener.
    ///
    /// # Returns
    /// A `FirestoreResult` indicating success or failure of adding the target.
    #[inline]
    pub fn add_target<S>(
        self,
        target: FirestoreListenerTarget,
        listener: &mut FirestoreListener<D, S>,
    ) -> FirestoreResult<()>
    where
        S: FirestoreResumeStateStorage + Send + Sync + Clone + 'static,
    {
        listener.add_target(FirestoreListenerTargetParams::new(
            target,
            self.target_type,
            self.labels,
        ))?;

        Ok(())
    }
}

/// A builder for configuring and executing an aggregated query, returning raw documents.
#[derive(Clone, Debug)]
pub struct FirestoreAggregatedQueryDocBuilder<'a, D>
where
    D: FirestoreAggregatedQuerySupport,
{
    db: &'a D,
    params: FirestoreAggregatedQueryParams,
}

impl<'a, D> FirestoreAggregatedQueryDocBuilder<'a, D>
where
    D: FirestoreAggregatedQuerySupport,
{
    /// Creates a new `FirestoreAggregatedQueryDocBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreAggregatedQueryParams) -> Self {
        Self { db, params }
    }

    /// Specifies that the aggregation results should be deserialized into a specific Rust type `T`.
    ///
    /// The structure of `T` should match the aliases defined in the aggregation.
    ///
    /// # Type Parameters
    /// * `T`: The type to deserialize results into. Must implement `serde::Deserialize`.
    ///
    /// # Returns
    /// A [`FirestoreAggregatedQueryObjBuilder`] for executing and deserializing results.
    #[inline]
    pub fn obj<T>(self) -> FirestoreAggregatedQueryObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreAggregatedQueryObjBuilder::new(self.db, self.params)
    }

    /// Executes the aggregation query and returns the results as raw documents.
    ///
    /// Each "document" in the result typically contains fields corresponding to the
    /// aliases of the aggregations.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `Vec` of [`Document`]s representing aggregation results.
    pub async fn query(self) -> FirestoreResult<Vec<Document>> {
        self.db.aggregated_query_doc(self.params).await
    }

    /// Executes the aggregation query and returns a stream of result documents.
    ///
    /// Errors terminate the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of [`Document`]s.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, Document>> {
        self.db.stream_aggregated_query_doc(self.params).await
    }

    /// Executes the aggregation query and returns a stream of `FirestoreResult<Document>`.
    ///
    /// Errors are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<Document>`.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        self.db
            .stream_aggregated_query_doc_with_errors(self.params)
            .await
    }
}

/// A builder for executing an aggregated query and deserializing results into type `T`.
#[derive(Clone, Debug)]
pub struct FirestoreAggregatedQueryObjBuilder<'a, D, T>
where
    D: FirestoreAggregatedQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    params: FirestoreAggregatedQueryParams,
    _ph: PhantomData<T>,
}

impl<'a, D, T> FirestoreAggregatedQueryObjBuilder<'a, D, T>
where
    D: FirestoreAggregatedQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    /// Creates a new `FirestoreAggregatedQueryObjBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreAggregatedQueryParams) -> Self {
        Self {
            db,
            params,
            _ph: PhantomData,
        }
    }

    /// Executes the aggregation query and deserializes all results into a `Vec<T>`.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `Vec<T>`.
    pub async fn query(self) -> FirestoreResult<Vec<T>> {
        self.db.aggregated_query_obj(self.params).await
    }

    /// Executes the aggregation query and returns a stream of deserialized objects `T`.
    ///
    /// Errors terminate the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `T`.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, T>> {
        self.db.stream_aggregated_query_obj(self.params).await
    }

    /// Executes the aggregation query and returns a stream of `FirestoreResult<T>`.
    ///
    /// Errors are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<T>`.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        self.db
            .stream_aggregated_query_obj_with_errors(self.params)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::fluent_api::tests::*;
    use crate::fluent_api::FirestoreExprBuilder;
    use crate::{path, paths, FirestoreQueryCollection};

    #[test]
    fn select_query_builder_test_fields() {
        let select_only_fields = FirestoreExprBuilder::new(&mockdb::MockDatabase {})
            .select()
            .fields(paths!(TestStructure::{some_id, one_more_string, some_num}))
            .return_only_fields;

        assert_eq!(
            select_only_fields,
            Some(vec![
                path!(TestStructure::some_id),
                path!(TestStructure::one_more_string),
                path!(TestStructure::some_num),
            ])
        )
    }

    #[test]
    fn select_query_builder_from_collection() {
        let select_only_fields = FirestoreExprBuilder::new(&mockdb::MockDatabase {})
            .select()
            .from("test");

        assert_eq!(
            select_only_fields.params.collection_id,
            FirestoreQueryCollection::Single("test".to_string())
        )
    }
}
