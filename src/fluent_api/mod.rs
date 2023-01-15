#![allow(clippy::too_many_arguments)]

pub mod delete_builder;
pub mod document_transform_builder;
pub mod insert_builder;
pub mod listing_builder;
pub mod select_aggregation_builder;
pub mod select_builder;
pub mod select_filter_builder;
pub mod update_builder;

use crate::delete_builder::FirestoreDeleteInitialBuilder;
use crate::fluent_api::select_builder::FirestoreSelectInitialBuilder;
use crate::insert_builder::FirestoreInsertInitialBuilder;
use crate::listing_builder::FirestoreListingInitialBuilder;
use crate::update_builder::FirestoreUpdateInitialBuilder;
use crate::{
    FirestoreAggregatedQuerySupport, FirestoreCreateSupport, FirestoreDb, FirestoreDeleteSupport,
    FirestoreGetByIdSupport, FirestoreListenSupport, FirestoreListingSupport,
    FirestoreQuerySupport, FirestoreUpdateSupport,
};

#[derive(Clone, Debug)]
pub struct FirestoreExprBuilder<'a, D> {
    db: &'a D,
}

impl<'a, D> FirestoreExprBuilder<'a, D>
where
    D: FirestoreQuerySupport
        + FirestoreCreateSupport
        + FirestoreDeleteSupport
        + FirestoreUpdateSupport
        + FirestoreListingSupport
        + FirestoreGetByIdSupport
        + FirestoreListenSupport
        + FirestoreAggregatedQuerySupport
        + Clone
        + Send
        + Sync
        + 'static,
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
    pub fn update(self) -> FirestoreUpdateInitialBuilder<'a, D> {
        FirestoreUpdateInitialBuilder::new(self.db)
    }

    #[inline]
    pub fn delete(self) -> FirestoreDeleteInitialBuilder<'a, D> {
        FirestoreDeleteInitialBuilder::new(self.db)
    }

    #[inline]
    pub fn list(self) -> FirestoreListingInitialBuilder<'a, D> {
        FirestoreListingInitialBuilder::new(self.db)
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
    pub mod mockdb;

    pub struct TestStructure {
        pub some_id: String,
        pub one_more_string: String,
        pub some_num: u64,
    }
}
