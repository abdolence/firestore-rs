use crate::{
    FirestoreCreateSupport, FirestoreDeleteSupport, FirestoreListDocParams, FirestoreListDocResult,
    FirestoreListingSupport, FirestoreQueryParams, FirestoreQuerySupport, FirestoreResult,
    FirestoreUpdateSupport,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::Document;
use serde::{Deserialize, Serialize};

pub struct MockDatabase;

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
}

#[allow(unused)]
#[async_trait]
impl FirestoreCreateSupport for MockDatabase {
    async fn create_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn create_obj_return_fields<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
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
        document_id: S,
        obj: &I,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn create_obj_at_return_fields<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
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

    async fn create_doc_root<S>(
        &self,
        collection_id: &str,
        document_id: S,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn create_doc<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
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
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn update_obj_return_fields<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
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
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn update_obj_at_return_fields<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
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
    ) -> FirestoreResult<Document> {
        unreachable!()
    }
}

#[allow(unused)]
#[async_trait]
impl FirestoreDeleteSupport for MockDatabase {
    async fn delete_by_id<S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<()>
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

    async fn stream_list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<Document>> {
        unreachable!()
    }

    async fn stream_list_obj<T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        unreachable!()
    }
}
