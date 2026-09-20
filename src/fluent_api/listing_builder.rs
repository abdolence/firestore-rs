//! Builder for constructing Firestore list operations.
//!
//! This module provides a fluent API for listing documents within a collection
//! or listing collection IDs under a parent document (or the database root).
//! It supports pagination, ordering (for document listing), and projections.

use crate::{
    FirestoreListCollectionIdsParams, FirestoreListCollectionIdsResult, FirestoreListDocParams,
    FirestoreListDocResult, FirestoreListingSupport, FirestoreQueryOrder, FirestoreRequestOptions,
    FirestoreRequestTag, FirestoreResult,
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
    #[inline]
    pub fn from<S: AsRef<str>>(self, collection: S) -> FirestoreListingDocBuilder<'a, D> {
        let params: FirestoreListDocParams =
            FirestoreListDocParams::new(collection.as_ref().to_string())
                .opt_return_only_fields(self.return_only_fields);
        FirestoreListingDocBuilder::new(self.db, params)
    }

    /// Specifies that collection IDs should be listed.
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
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreListDocParams) -> Self {
        Self { db, params }
    }

    /// Specifies that the listed documents should be deserialized into a specific Rust type `T`.
    #[inline]
    pub fn obj<T>(self) -> FirestoreListingObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreListingObjBuilder::new(self.db, self.params)
    }

    /// Specifies the parent document path for listing documents in a sub-collection.
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
    #[inline]
    pub fn page_size(self, value: usize) -> Self {
        Self {
            params: self.params.with_page_size(value),
            ..self
        }
    }

    /// Sets the page token for pagination.
    #[inline]
    pub fn page_token(self, value: String) -> Self {
        Self {
            params: self.params.with_page_token(value),
            ..self
        }
    }

    /// Specifies the order in which to sort the documents.
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

    /// Attaches request tags to this listing operation.
    ///
    /// They override any session wide default configured with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    #[inline]
    pub fn request_tags<I>(self, request_tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(request_tags))
    }

    /// Attaches request options to this listing operation.
    #[inline]
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            params: self.params.with_request_options(options),
            ..self
        }
    }

    /// Fetches one page of documents, with a token for the next page when more remain.
    ///
    /// Returns an error if the request to Firestore fails.
    pub async fn get_page(self) -> FirestoreResult<FirestoreListDocResult> {
        self.db.list_doc(self.params).await
    }

    /// Streams every document, sending further requests to Firestore as needed to page through
    /// the full result.
    ///
    /// An error while streaming terminates the stream early.
    ///
    /// ```rust,no_run
    /// use firestore::FirestoreDb;
    /// use futures::stream::BoxStream;
    /// use futures::StreamExt;
    ///
    /// # async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut documents: BoxStream<_> = db.fluent().list().from("users").stream_all().await?;
    /// while let Some(document) = documents.next().await {
    ///     let _ = document;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream_all<'b>(self) -> FirestoreResult<BoxStream<'b, Document>> {
        self.db.stream_list_doc(self.params).await
    }

    /// Streams a `FirestoreResult<Document>` per document, paging through the full result, so
    /// one failed request does not end the stream.
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

    /// Attaches request tags to this listing operation.
    ///
    /// They override any session wide default configured with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    #[inline]
    pub fn request_tags<I>(self, request_tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(request_tags))
    }

    /// Attaches request options to this listing operation.
    #[inline]
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            params: self.params.with_request_options(options),
            ..self
        }
    }

    /// Streams every document deserialized into `T`, paging through the full result.
    ///
    /// An error while streaming or deserializing terminates the stream early.
    pub async fn stream_all<'b>(self) -> FirestoreResult<BoxStream<'b, T>>
    where
        T: 'b,
    {
        self.db.stream_list_obj(self.params).await
    }

    /// Streams a `FirestoreResult<T>` per document, paging through the full result, so one
    /// failed fetch or deserialization does not end the stream.
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
    #[inline]
    pub fn page_size(self, value: usize) -> Self {
        Self {
            params: self.params.with_page_size(value),
            ..self
        }
    }

    /// Attaches request tags to this listing operation.
    ///
    /// They override any session wide default configured with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    #[inline]
    pub fn request_tags<I>(self, request_tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(request_tags))
    }

    /// Attaches request options to this listing operation.
    #[inline]
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            params: self.params.with_request_options(options),
            ..self
        }
    }

    /// Fetches one page of collection IDs, with a token for the next page when more remain.
    ///
    /// Returns an error if the request to Firestore fails.
    pub async fn get_page(self) -> FirestoreResult<FirestoreListCollectionIdsResult> {
        self.db.list_collection_ids(self.params).await
    }

    /// Streams every collection ID, paging through the full result.
    ///
    /// An error while streaming terminates the stream early.
    pub async fn stream_all(self) -> FirestoreResult<BoxStream<'a, String>> {
        self.db.stream_list_collection_ids(self.params).await
    }

    /// Streams a `FirestoreResult<String>` per collection ID, paging through the full result, so
    /// one failed request does not end the stream.
    pub async fn stream_all_with_errors(
        self,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<String>>> {
        self.db
            .stream_list_collection_ids_with_errors(self.params)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::fluent_api::tests::*;
    use crate::fluent_api::FirestoreExprBuilder;
    use crate::FirestoreRequestOptions;

    #[test]
    fn list_doc_builder_request_tags() {
        let builder = FirestoreExprBuilder::new(&mockdb::MockDatabase {})
            .list()
            .from("test")
            .request_tags(["tag-1", "tag-2"]);

        assert_eq!(
            builder.params.request_options,
            Some(FirestoreRequestOptions::from_tags(["tag-1", "tag-2"]))
        )
    }

    #[test]
    fn list_collection_ids_builder_request_tags() {
        let builder = FirestoreExprBuilder::new(&mockdb::MockDatabase {})
            .list()
            .collections()
            .request_tags(["tag-1"]);

        assert_eq!(
            builder.params.request_options,
            Some(FirestoreRequestOptions::from_tags(["tag-1"]))
        )
    }
}
