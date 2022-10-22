pub mod query_builder;
use crate::fluent_api::query_builder::FirestoreSelectInitialBuilder;
use crate::{FirestoreDb, FirestoreQuerySupport};

#[derive(Clone, Debug)]
pub struct FirestoreExprBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    db: &'a D,
}

impl<'a, D> FirestoreExprBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    pub fn select(self) -> FirestoreSelectInitialBuilder<'a, D> {
        FirestoreSelectInitialBuilder::new(self)
    }
}

impl FirestoreDb {
    pub fn fluent(&self) -> FirestoreExprBuilder<FirestoreDb> {
        FirestoreExprBuilder::new(self)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{FirestoreQueryParams, FirestoreQuerySupport, FirestoreResult};
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use gcloud_sdk::google::firestore::v1::Document;
    use serde::Deserialize;

    pub struct TestStructure {
        pub some_id: String,
        pub one_more_string: String,
        pub some_num: u64,
    }

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
}
