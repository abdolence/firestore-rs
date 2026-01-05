//! Builder for constructing Firestore list operations.
//!
//! This module provides a fluent API for listing documents within a collection
//! or listing collection IDs under a parent document (or the database root).
//! It supports pagination, ordering (for document listing), and projections.

use crate::{
    FirestoreListCollectionIdsParams, FirestoreListCollectionIdsResult, FirestoreListDocParams,
    FirestoreListDocResult, FirestoreListingSupport, FirestoreQueryOrder, FirestoreResult,
};
use futures::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::Document;
use serde::Deserialize;
use std::marker::PhantomData;

/// The initial builder for a Firestore list operation.
///
/// Created by calling [`FirestoreExprBuilder::list()`](crate::FirestoreExprBuilder::list).
/// From here, you can choose to list documents from a collection or list collection IDs.
#[derive(Clone, Debug)]
pub struct FirestoreListingInitialBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    db: &'a D,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreListingInitialBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    /// Creates a new `FirestoreListingInitialBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            return_only_fields: None,
        }
    }

    /// Specifies which fields of the documents should be returned when listing documents.
    ///
    /// This is a projection. If not set, all fields are returned.
    /// This option is only applicable when listing documents, not collection IDs.
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

    /// Specifies that documents should be listed from the given collection.
    ///
    /// # Arguments
    /// * `collection`: The ID of the collection to list documents from.
    ///
    /// # Returns
    /// A [`FirestoreListingDocBuilder`] to further configure and execute the document listing.
    #[inline]
    pub fn from(self, collection: &str) -> FirestoreListingDocBuilder<'a, D> {
        let params: FirestoreListDocParams = FirestoreListDocParams::new(collection.to_string())
            .opt_return_only_fields(self.return_only_fields);
        FirestoreListingDocBuilder::new(self.db, params)
    }

    /// Specifies that collection IDs should be listed.
    ///
    /// # Returns
    /// A [`FirestoreListCollectionIdsBuilder`] to configure and execute the collection ID listing.
    #[inline]
    pub fn collections(self) -> FirestoreListCollectionIdsBuilder<'a, D> {
        FirestoreListCollectionIdsBuilder::new(self.db)
    }
}

/// A builder for configuring and executing a document listing operation.
#[derive(Clone, Debug)]
pub struct FirestoreListingDocBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    db: &'a D,
    params: FirestoreListDocParams,
}

impl<'a, D> FirestoreListingDocBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    /// Creates a new `FirestoreListingDocBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreListDocParams) -> Self {
        Self { db, params }
    }

    /// Specifies that the listed documents should be deserialized into a specific Rust type `T`.
    ///
    /// # Type Parameters
    /// * `T`: The type to deserialize documents into. Must implement `serde::Deserialize`.
    ///
    /// # Returns
    /// A [`FirestoreListingObjBuilder`] for streaming deserialized objects.
    #[inline]
    pub fn obj<T>(self) -> FirestoreListingObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreListingObjBuilder::new(self.db, self.params)
    }

    /// Specifies the parent document path for listing documents in a sub-collection.
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

    /// Sets the maximum number of documents to return in a single page.
    ///
    /// # Arguments
    /// * `value`: The page size.
    ///
    /// # Returns
    /// The builder instance with the page size set.
    #[inline]
    pub fn page_size(self, value: usize) -> Self {
        Self {
            params: self.params.with_page_size(value),
            ..self
        }
    }

    /// Sets the page token for pagination.
    ///
    /// # Arguments
    /// * `value`: The page token from a previous listing operation.
    ///
    /// # Returns
    /// The builder instance with the page token set.
    #[inline]
    fn page_token(self, value: String) -> Self {
        Self {
            params: self.params.with_page_token(value),
            ..self
        }
    }

    /// Specifies the order in which to sort the documents.
    ///
    /// # Arguments
    /// * `fields`: An iterator of [`FirestoreQueryOrder`] specifying the fields and directions to sort by.
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

    /// Retrieves a single page of documents.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a [`FirestoreListDocResult`], which includes the documents
    /// for the current page and a potential next page token.
    pub async fn get_page(self) -> FirestoreResult<FirestoreListDocResult> {
        self.db.list_doc(self.params).await
    }

    /// Streams all documents matching the configuration, handling pagination automatically.
    ///
    /// Errors encountered during streaming will terminate the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of [`Document`]s.
    pub async fn stream_all<'b>(self) -> FirestoreResult<BoxStream<'b, Document>> {
        self.db.stream_list_doc(self.params).await
    }

    /// Streams all documents matching the configuration, handling pagination automatically.
    ///
    /// Errors encountered during streaming are yielded as `Err` items in the stream,
    /// allowing the caller to handle them without terminating the entire stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<Document>`.
    pub async fn stream_all_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        self.db.stream_list_doc_with_errors(self.params).await
    }
}

/// A builder for streaming listed documents deserialized into a Rust type `T`.
#[derive(Clone, Debug)]
pub struct FirestoreListingObjBuilder<'a, D, T>
where
    D: FirestoreListingSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    params: FirestoreListDocParams,
    _pd: PhantomData<T>,
}

impl<'a, D, T> FirestoreListingObjBuilder<'a, D, T>
where
    D: FirestoreListingSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    /// Creates a new `FirestoreListingObjBuilder`.
    pub(crate) fn new(
        db: &'a D,
        params: FirestoreListDocParams,
    ) -> FirestoreListingObjBuilder<'a, D, T> {
        Self {
            db,
            params,
            _pd: PhantomData,
        }
    }

    /// Streams all documents matching the configuration, deserializing them into type `T`
    /// and handling pagination automatically.
    ///
    /// Errors encountered during streaming or deserialization will terminate the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of deserialized objects `T`.
    pub async fn stream_all<'b>(self) -> FirestoreResult<BoxStream<'b, T>>
    where
        T: 'b,
    {
        self.db.stream_list_obj(self.params).await
    }

    /// Streams all documents matching the configuration, deserializing them into type `T`
    /// and handling pagination automatically.
    ///
    /// Errors encountered during streaming or deserialization are yielded as `Err` items
    /// in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<T>`.
    pub async fn stream_all_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        self.db.stream_list_obj_with_errors(self.params).await
    }
}

/// A builder for configuring and executing a collection ID listing operation.
#[derive(Clone, Debug)]
pub struct FirestoreListCollectionIdsBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    db: &'a D,
    params: FirestoreListCollectionIdsParams,
}

impl<'a, D> FirestoreListCollectionIdsBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    /// Creates a new `FirestoreListCollectionIdsBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            params: FirestoreListCollectionIdsParams::new(),
        }
    }

    /// Specifies the parent document path under which to list collection IDs.
    ///
    /// If not specified, collection IDs directly under the database root are listed.
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

    /// Sets the maximum number of collection IDs to return in a single page.
    ///
    /// # Arguments
    /// * `value`: The page size.
    ///
    /// # Returns
    /// The builder instance with the page size set.
    #[inline]
    pub fn page_size(self, value: usize) -> Self {
        Self {
            params: self.params.with_page_size(value),
            ..self
        }
    }

    /// Retrieves a single page of collection IDs.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a [`FirestoreListCollectionIdsResult`], which includes
    /// the collection IDs for the current page and a potential next page token.
    pub async fn get_page(self) -> FirestoreResult<FirestoreListCollectionIdsResult> {
        self.db.list_collection_ids(self.params).await
    }

    /// Streams all collection IDs matching the configuration, handling pagination automatically.
    ///
    /// Errors encountered during streaming will terminate the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `String` (collection IDs).
    pub async fn stream_all(self) -> FirestoreResult<BoxStream<'a, String>> {
        self.db.stream_list_collection_ids(self.params).await
    }

    /// Streams all collection IDs matching the configuration, handling pagination automatically.
    ///
    /// Errors encountered during streaming are yielded as `Err` items in the stream.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a `BoxStream` of `FirestoreResult<String>`.
    pub async fn stream_all_with_errors(
        self,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<String>>> {
        self.db
            .stream_list_collection_ids_with_errors(self.params)
            .await
    }
}
