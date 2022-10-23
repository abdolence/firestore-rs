use crate::{FirestoreCreateSupport, FirestoreQueryParams, FirestoreQuerySupport, FirestoreResult};
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

impl FirestoreCreateSupport for MockDatabase {
    async fn create_obj<T, S>(
        &self,
        _collection_id: &str,
        _document_id: S,
        _obj: &T,
    ) -> FirestoreResult<T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn create_obj_at<T, S>(
        &self,
        _parent: &str,
        _collection_id: &str,
        _document_id: S,
        _obj: &T,
    ) -> FirestoreResult<T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        unreachable!()
    }

    async fn create_doc<S>(
        &self,
        _parent: &str,
        _collection_id: &str,
        _document_id: S,
        _input_doc: Document,
        _return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        unreachable!()
    }
}
