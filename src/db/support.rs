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
    /// Opens a listen stream that sends each of `requests` as it arrives.
    ///
    /// The request side stays open until the response ends or is dropped, even once `requests`
    /// is closed.
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        requests: tokio::sync::mpsc::UnboundedReceiver<ListenRequest>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>>;

    /// The request that adds a target to a listen stream.
    fn add_target_request(
        &self,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<ListenRequest>;

    /// The request that removes a target from a listen stream, with the request options the
    /// target was added with.
    fn remove_target_request(
        &self,
        target: &FirestoreListenerTarget,
        request_options: Option<&FirestoreRequestOptions>,
    ) -> FirestoreResult<ListenRequest>;
}

/// Runs pipelines on an Enterprise edition database.
#[async_trait]
pub trait FirestorePipelineSupport {
    async fn query_pipeline_doc(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<Vec<FirestoreDocument>>;

    async fn stream_pipeline_doc<'b>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreDocument>>;

    async fn stream_pipeline_doc_with_errors<'b>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>>;

    async fn stream_pipeline_doc_with_metadata<'b>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<
        BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<FirestoreDocument>>>,
    >;

    async fn query_pipeline_obj<T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>;

    async fn stream_pipeline_obj<'b, T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;

    async fn stream_pipeline_obj_with_errors<'b, T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;

    async fn stream_pipeline_obj_with_metadata<'b, T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<T>>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;

    /// Runs a pipeline that ends in a write. It is sent once and never retried: without a
    /// transaction, a failed run may have written part of its documents.
    async fn execute_pipeline_write(
        &self,
        params: FirestorePipelineWriteParams,
    ) -> FirestoreResult<FirestorePipelineWriteResult>;

    /// Runs a pipeline that ends in a write in `transaction`, which holds it until commit.
    /// `params.transaction` is ignored.
    async fn execute_pipeline_write_in_transaction(
        &self,
        params: FirestorePipelineWriteParams,
        transaction: &mut FirestoreTransaction<'_>,
    ) -> FirestoreResult<FirestorePipelineWriteResult>;
}

/// Plans or applies one collection group's declared indexes, field overrides and TTL policy.
///
/// Mirrors [`FirestoreQuerySupport::query_doc`]. Today the fluent `.plan()` and `.sync()`
/// terminals on [`FirestoreIndexesBuilder`](crate::index_builder::FirestoreIndexesBuilder) are
/// the only callers, and they validate `params` before calling in - but an implementation must
/// not rely on that: the planning step behind `plan_indexes`/`sync_indexes` validates `params`
/// again on its own, so a declaration is checked regardless of caller.
#[cfg(feature = "admin")]
#[async_trait]
pub trait FirestoreIndexSupport {
    /// Reports what [`sync_indexes`](Self::sync_indexes) would change with the same `options`,
    /// without writing anything. Only `options.prune` affects the plan; `options.wait` is ignored.
    async fn plan_indexes(
        &self,
        params: FirestoreIndexParams,
        options: FirestoreIndexSyncOptions,
    ) -> FirestoreResult<FirestoreIndexPlan>;

    /// Reconciles Firestore with `params`, according to `options`.
    async fn sync_indexes(
        &self,
        params: FirestoreIndexParams,
        options: FirestoreIndexSyncOptions,
    ) -> FirestoreResult<FirestoreIndexSyncReport>;

    /// Lists the composite indexes and field resources of `collection_group`, or of every group
    /// but the ones Firestore reserves, such as `__default__`, when it is `None`.
    async fn list_indexes(
        &self,
        collection_group: Option<FirestoreCollectionId>,
    ) -> FirestoreResult<FirestoreIndexListing>;

    /// Starts building `definition`'s index and returns it as Firestore lists it once the build
    /// has started, or, with `wait`, once the build has finished. An index that already exists
    /// fails with [`DataConflictError`](crate::errors::FirestoreError::DataConflictError), unless
    /// `if_not_exists` is set.
    ///
    /// With `if_not_exists`, the one index of the group that `definition` matches is the outcome,
    /// and an `ALREADY_EXISTS` for an index the listing does not show stays an error. Such an
    /// index is returned as listed, or, with `wait`, once it has left `CREATING`, polled under the
    /// same deadline as a build this call started. With `wait`, an index that ends in
    /// `NEEDS_REPAIR` fails the call, as a failed build of its own does. Any other error stays an
    /// error.
    async fn create_index(
        &self,
        definition: FirestoreIndexDefinition,
        wait: Option<FirestoreOperationWaitOptions>,
        if_not_exists: bool,
    ) -> FirestoreResult<FirestoreListedCompositeIndex>;

    /// Deletes the one index `target` names and returns it as it was listed just before.
    /// Deletes nothing and fails with
    /// [`DataNotFoundError`](crate::errors::FirestoreError::DataNotFoundError) when no index
    /// matches or the index belongs to another group than the target names, and with
    /// [`DataConflictError`](crate::errors::FirestoreError::DataConflictError) when a definition
    /// matches more than one.
    ///
    /// With `if_exists`, an ID nothing has and a definition nothing matches delete nothing and
    /// return `None`; an index in another group than the target names, several matches and every
    /// other error stay errors. Without it the result is always `Some`.
    async fn delete_index(
        &self,
        target: FirestoreIndexDeleteTarget,
        if_exists: bool,
    ) -> FirestoreResult<Option<FirestoreListedCompositeIndex>>;

    /// Sends `update` as one `UpdateField` write and returns the field as Firestore reads it right
    /// after the write, or, with `wait`, once the write has finished. Writes to one field are not
    /// sequenced: Firestore accepts a second while the first is in progress.
    async fn update_field(
        &self,
        update: FirestoreFieldUpdate,
        wait: Option<FirestoreOperationWaitOptions>,
    ) -> FirestoreResult<FirestoreListedField>;
}

/// Deletes every document in one or more collection groups, at any depth, across the whole
/// database, via Firestore's `BulkDeleteDocuments` admin RPC.
///
/// The fluent `.execute()` terminal on
/// [`FirestoreBulkDeleteBuilder`](crate::delete_builder::FirestoreBulkDeleteBuilder) is today's
/// only caller, and it validates `params` before calling in - but an implementation must not rely
/// on that: `bulk_delete_documents` validates `params` again on its own.
#[cfg(feature = "admin")]
#[async_trait]
pub trait FirestoreBulkDeleteSupport {
    /// Starts the bulk delete, and - when [`FirestoreBulkDeleteParams::wait`] is set - waits for
    /// it to reach a terminal state before returning.
    async fn bulk_delete_documents(
        &self,
        params: FirestoreBulkDeleteParams,
    ) -> FirestoreResult<FirestoreBulkDeleteResult>;
}
