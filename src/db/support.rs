//! Crate-private low-level "support" traits.
//!
//! These are the mechanical, gRPC-shaped operations that the fluent API is built on.
//! They are **not** public API: [`FirestoreDb::fluent()`](crate::FirestoreDb::fluent) is the only
//! supported entry point of this crate.
//!
//! They are deliberately declared as `pub trait` inside this *private* module. Writing
//! `pub(crate) trait` instead would trip the `private_bounds` lint wherever the fluent builders
//! use them as generic bounds, and CI runs `cargo clippy -- -Dwarnings`. A `pub trait` in a
//! private module has exactly the same reachability - it cannot be named, implemented or called
//! from outside this crate - while keeping the lint quiet.
//!
//! Please do not "fix" this to `pub(crate) trait`: it will not compile.

use crate::*;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};

/// A peekable boxed stream, used by the query support trait.
pub type PeekableBoxStream<'a, T> = futures::stream::Peekable<BoxStream<'a, T>>;

#[async_trait]
pub trait FirestoreCreateSupport {
    async fn create_doc<S>(
        &self,
        collection_id: &str,
        document_id: Option<S>,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;

    async fn create_doc_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: Option<S>,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;

    async fn create_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: Option<S>,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn create_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: Option<S>,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;
}

#[async_trait]
pub trait FirestoreUpdateSupport {
    async fn update_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn update_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn update_doc(
        &self,
        collection_id: &str,
        firestore_doc: Document,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<Document>;
}

#[async_trait]
pub trait FirestoreDeleteSupport {
    async fn delete_by_id<S>(
        &self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send;

    async fn delete_by_id_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send;
}

#[async_trait]
pub trait FirestoreGetByIdSupport {
    async fn get_doc<S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;

    async fn get_doc_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;

    async fn get_obj<T, S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_return_fields<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_at<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_at_return_fields<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_if_exists<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_at_if_exists<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn batch_stream_get_docs<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_docs_with_errors<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_docs_at<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_docs_at_with_errors<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects_with_errors<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects_at<'a, T, S, I>(
        &'a self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects_at_with_errors<'a, T, S, I>(
        &'a self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;
}

#[async_trait]
pub trait FirestoreQuerySupport {
    async fn query_doc(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<Document>>;

    async fn stream_query_doc<'b>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>>;

    async fn stream_query_doc_with_errors<'b>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>>;

    async fn stream_query_doc_with_metadata<'b>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithMetadata<FirestoreDocument>>>>;

    async fn query_obj<T>(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>;
    async fn stream_query_obj<'b, T>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;

    async fn stream_query_obj_with_errors<'b, T>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;

    async fn stream_query_obj_with_metadata<'b, T>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithMetadata<T>>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;

    fn stream_partition_cursors_with_errors(
        &self,
        params: FirestorePartitionQueryParams,
    ) -> BoxFuture<'_, FirestoreResult<PeekableBoxStream<'_, FirestoreResult<FirestoreQueryCursor>>>>;

    async fn stream_partition_query_doc_with_errors(
        &self,
        parallelism: usize,
        partition_params: FirestorePartitionQueryParams,
    ) -> FirestoreResult<BoxStream<'_, FirestoreResult<(FirestorePartition, Document)>>>;

    async fn stream_partition_query_obj_with_errors<'a, T>(
        &'a self,
        parallelism: usize,
        partition_params: FirestorePartitionQueryParams,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(FirestorePartition, T)>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'a;
}

#[async_trait]
pub trait FirestoreAggregatedQuerySupport {
    async fn aggregated_query_doc(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<Document>>;

    async fn stream_aggregated_query_doc<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>>;

    async fn stream_aggregated_query_doc_with_errors<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>>;

    async fn aggregated_query_obj<T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>;

    async fn stream_aggregated_query_obj<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>;

    async fn stream_aggregated_query_obj_with_errors<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;
}

#[async_trait]
pub trait FirestoreListingSupport {
    async fn list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<FirestoreListDocResult>;

    async fn stream_list_doc<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, Document>>;

    async fn stream_list_doc_with_errors<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>>;

    async fn stream_list_obj<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de> + 'b;

    async fn stream_list_obj_with_errors<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de> + 'b;

    async fn list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<FirestoreListCollectionIdsResult>;

    async fn stream_list_collection_ids_with_errors(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<String>>>;

    async fn stream_list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<String>>;
}

#[async_trait]
pub trait FirestoreListenSupport {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        targets: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>>;
}
