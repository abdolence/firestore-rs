#![allow(clippy::too_many_arguments)]

pub mod delete_builder;
pub mod document_transform_builder;
pub mod insert_builder;
pub mod listing_builder;
pub mod query_filter_builder;
pub mod select_builder;
pub mod update_builder;

use crate::delete_builder::FirestoreDeleteInitialBuilder;
use crate::fluent_api::select_builder::FirestoreSelectInitialBuilder;
use crate::insert_builder::FirestoreInsertInitialBuilder;
use crate::listing_builder::FirestoreListingInitialBuilder;
use crate::update_builder::FirestoreUpdateInitialBuilder;
use crate::{
    FirestoreCreateSupport, FirestoreDb, FirestoreDeleteSupport, FirestoreGetByIdSupport,
    FirestoreListenSupport, FirestoreListingSupport, FirestoreQuerySupport, FirestoreUpdateSupport,
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
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// Select one or more documents from the database
    ///
    /// Single documents can be selected by thier IDs, multiple documents can be selected with a query filter.
    #[inline]
    pub fn select(self) -> FirestoreSelectInitialBuilder<'a, D> {
        FirestoreSelectInitialBuilder::new(self.db)
    }

    /// Insert a new document into the database
    #[inline]
    pub fn insert(self) -> FirestoreInsertInitialBuilder<'a, D> {
        FirestoreInsertInitialBuilder::new(self.db)
    }

    /// Update a document in the database
    ///
    /// Documents can be implicitly created using update when using your own document IDs. Useful to use in batches and transactions.
    #[inline]
    pub fn update(self) -> FirestoreUpdateInitialBuilder<'a, D> {
        FirestoreUpdateInitialBuilder::new(self.db)
    }

    /// Delete a document in the database
    #[inline]
    pub fn delete(self) -> FirestoreDeleteInitialBuilder<'a, D> {
        FirestoreDeleteInitialBuilder::new(self.db)
    }

    /// List documents in a collection (or subcollection)
    ///
    /// This is a convenience method to list all documents in a collection. For more complex queries, use the select method.
    #[inline]
    pub fn list(self) -> FirestoreListingInitialBuilder<'a, D> {
        FirestoreListingInitialBuilder::new(self.db)
    }
}

impl FirestoreDb {
    /// Fluent API
    ///
    /// The Fluent API simplifies development and the developer experience. The library provides a more high level API starting with v0.12.x. This is the recommended API for all applications to use.
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
