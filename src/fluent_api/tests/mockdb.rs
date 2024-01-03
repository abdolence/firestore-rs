use crate::*;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::{Document, ListenResponse};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct MockDatabase;

#[allow(unused)]
#[async_trait]
impl FirestoreQuerySupport for MockDatabase {
    async fn query_doc(&self, _params: FirestoreQueryParams) -> FirestoreResult<Vec<Document>> {
        unreachable!()
    }

    async fn stream_query_doc<'b>(
        &self,
        _params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>> {
        unreachable!()
    }

    async fn stream_query_doc_with_errors<'b>(
        &self,
        _params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        unreachable!()
    }

    async fn query_obj<T>(&self, _params: FirestoreQueryParams) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        unreachable!()
    }

    async fn stream_query_obj<'b, T>(
        &self,
        _params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>,
        T: 'b,
    {
        unreachable!()
    }

    async fn stream_query_obj_with_errors<'b, T>(
        &self,
        _params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b,
    {
        unreachable!()
    }

    fn stream_partition_cursors_with_errors(
        &self,
        params: FirestorePartitionQueryParams,
    ) -> BoxFuture<FirestoreResult<PeekableBoxStream<FirestoreResult<FirestoreQueryCursor>>>> {
        unreachable!()
    }

    async fn stream_partition_query_doc_with_errors(
        &self,
        parallelism: usize,
        partition_params: FirestorePartitionQueryParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(FirestorePartition, Document)>>> {
        unreachable!()
    }

    async fn stream_partition_query_obj_with_errors<'a, T>(
        &'a self,
        parallelism: usize,
        partition_params: FirestorePartitionQueryParams,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(FirestorePartition, T)>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'a,
    {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreCreateSupport for MockDatabase {
    async fn create_doc<S>(
        &self,
        collection_id: &str,
        document_id: Option<S>,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn create_doc_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: Option<S>,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

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
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

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
        S: AsRef<str> + Send,
    {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreUpdateSupport for MockDatabase {
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
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

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
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn update_doc(
        &self,
        collection_id: &str,
        firestore_doc: Document,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<Document> {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreDeleteSupport for MockDatabase {
    async fn delete_by_id<S>(
        &self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn delete_by_id_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreListingSupport for MockDatabase {
    async fn list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<FirestoreListDocResult> {
        unreachable!()
    }

    async fn stream_list_doc<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, Document>> {
        unreachable!()
    }

    async fn stream_list_doc_with_errors<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        unreachable!()
    }

    async fn stream_list_obj<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de> + 'b,
    {
        unreachable!()
    }

    async fn stream_list_obj_with_errors<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de> + 'b,
    {
        unreachable!()
    }

    async fn list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<FirestoreListCollectionIdsResult> {
        unreachable!()
    }

    async fn stream_list_collection_ids_with_errors(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<String>>> {
        unreachable!()
    }

    async fn stream_list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<String>> {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreGetByIdSupport for MockDatabase {
    async fn get_doc<S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn get_doc_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn get_obj<T, S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn get_obj_at<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn get_obj_at_return_fields<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn get_obj_if_exists<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn get_obj_at_if_exists<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn batch_stream_get_docs_at<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

    async fn batch_stream_get_objects<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de> + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

    async fn batch_stream_get_docs_at_with_errors<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

    async fn batch_stream_get_objects_with_errors<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

    async fn batch_stream_get_docs<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

    async fn batch_stream_get_docs_with_errors<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

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
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

    async fn batch_stream_get_objects_at_with_errors<'a, T, S, I>(
        &'a self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        unreachable!()
    }

    async fn get_obj_return_fields<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreListenSupport for MockDatabase {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        targets: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreAggregatedQuerySupport for MockDatabase {
    async fn aggregated_query_doc(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<Document>> {
        unreachable!()
    }

    async fn stream_aggregated_query_doc<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>> {
        unreachable!()
    }

    async fn stream_aggregated_query_doc_with_errors<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        unreachable!()
    }

    async fn aggregated_query_obj<T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        unreachable!()
    }

    async fn stream_aggregated_query_obj<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        unreachable!()
    }

    async fn stream_aggregated_query_obj_with_errors<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b,
    {
        unreachable!()
    }
}
