pub mod create_builder;
pub mod delete_builder;
pub mod filter_builder;
pub mod query_builder;

use crate::create_builder::FirestoreInsertInitialBuilder;
use crate::delete_builder::FirestoreDeleteInitialBuilder;
use crate::fluent_api::query_builder::FirestoreSelectInitialBuilder;
use crate::{FirestoreCreateSupport, FirestoreDb, FirestoreDeleteSupport, FirestoreQuerySupport};

#[derive(Clone, Debug)]
pub struct FirestoreExprBuilder<'a, D>
where
    D: FirestoreQuerySupport + FirestoreCreateSupport,
{
    db: &'a D,
}

impl<'a, D> FirestoreExprBuilder<'a, D>
where
    D: FirestoreQuerySupport + FirestoreCreateSupport + FirestoreDeleteSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    #[inline]
    pub fn select(self) -> FirestoreSelectInitialBuilder<'a, D> {
        FirestoreSelectInitialBuilder::new(self.db)
    }

    #[inline]
    pub fn insert(self) -> FirestoreInsertInitialBuilder<'a, D> {
        FirestoreInsertInitialBuilder::new(self.db)
    }

    #[inline]
    pub fn delete(self) -> FirestoreDeleteInitialBuilder<'a, D> {
        FirestoreDeleteInitialBuilder::new(self.db)
    }
}

impl FirestoreDb {
    #[inline]
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

    mod mockdb;

    pub struct TestStructure {
        pub some_id: String,
        pub one_more_string: String,
        pub some_num: u64,
    }
}
