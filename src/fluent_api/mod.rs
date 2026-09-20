//! A fluent, chainable API for building and executing Firestore operations.
//!
//! Start from [`FirestoreDb::fluent()`](crate::FirestoreDb::fluent), then chain `select`,
//! `insert`, `update`, `delete` or `list` to pick the operation, followed by builder methods to
//! configure it and a terminal method to send it to Firestore.

#![allow(clippy::too_many_arguments)]

/// Deletes a document, or queues the delete on a batch or transaction.
///
/// Reach for this from [`FirestoreExprBuilder::delete()`](FirestoreExprBuilder::delete).
pub mod delete_builder;

/// Builds server-side field transformations (increment, array append/remove, server timestamp)
/// for an update.
///
/// Reach for this via `.transforms()` on an update builder, not directly.
pub mod document_transform_builder;

/// Inserts a document from a raw `Document` or a serializable Rust object.
///
/// Reach for this from [`FirestoreExprBuilder::insert()`](FirestoreExprBuilder::insert).
pub mod insert_builder;

/// Lists the documents in a collection, or the collection IDs under a path, with pagination.
///
/// Reach for this from [`FirestoreExprBuilder::list()`](FirestoreExprBuilder::list).
pub mod listing_builder;

/// Builds `COUNT`, `SUM` and `AVG` aggregations for a query.
///
/// Reach for this via `.aggregate()` on a select builder, not directly.
pub mod select_aggregation_builder;

/// Runs queries and document/collection-group reads, including filtering, ordering, cursors,
/// vector search, partitioned queries and listeners.
///
/// Reach for this from [`FirestoreExprBuilder::select()`](FirestoreExprBuilder::select).
pub mod select_builder;

/// Builds comparison, unary and composite (AND/OR) filters for a query.
///
/// Reach for this via `.filter()` on a select builder, not directly.
pub mod select_filter_builder;

/// Builds the per-field sort direction for a query or listing's result ordering.
///
/// Reach for this via `.order()` on a select or listing builder, not directly.
pub mod select_order_builder;

/// Updates a document from a raw `Document`, a serializable Rust object, or transformations
/// only, and queues updates on a batch or transaction.
///
/// Reach for this from [`FirestoreExprBuilder::update()`](FirestoreExprBuilder::update).
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

/// The entry point for building fluent Firestore expressions.
///
/// Obtained from [`FirestoreDb::fluent()`](crate::FirestoreDb::fluent).
///
/// The type parameter `D` is an internal implementation detail; in practice it is always
/// [`FirestoreDb`](crate::FirestoreDb).
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

    /// Starts a query or a fetch by document ID. Continue with `.fields()`, then `.from()` for a
    /// collection or `.by_id_in()` for known IDs.
    #[inline]
    pub fn select(self) -> FirestoreSelectInitialBuilder<'a, D> {
        FirestoreSelectInitialBuilder::new(self.db)
    }

    /// Starts inserting a document. Continue with `.into()` to name the target collection.
    #[inline]
    pub fn insert(self) -> FirestoreInsertInitialBuilder<'a, D> {
        FirestoreInsertInitialBuilder::new(self.db)
    }

    /// Starts updating a document. Continue with `.fields()` to restrict which fields are
    /// touched, then `.in_col()` to name the target collection.
    #[inline]
    pub fn update(self) -> FirestoreUpdateInitialBuilder<'a, D> {
        FirestoreUpdateInitialBuilder::new(self.db)
    }

    /// Starts deleting a document. Continue with `.from()` to name the source collection.
    #[inline]
    pub fn delete(self) -> FirestoreDeleteInitialBuilder<'a, D> {
        FirestoreDeleteInitialBuilder::new(self.db)
    }

    /// Starts listing documents in a collection or collection IDs under a path. Continue with
    /// `.from()` for documents or `.collections()` for collection IDs.
    #[inline]
    pub fn list(self) -> FirestoreListingInitialBuilder<'a, D> {
        FirestoreListingInitialBuilder::new(self.db)
    }
}

impl FirestoreDb {
    /// Starts a fluent Firestore operation. Continue with `select`, `insert`, `update`, `delete`
    /// or `list`.
    #[inline]
    pub fn fluent(&self) -> FirestoreExprBuilder<'_, FirestoreDb> {
        FirestoreExprBuilder::new(self)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    pub mod mockdb;

    // Test structure used in fluent API examples and tests.
    pub struct TestStructure {
        pub some_id: String,
        pub one_more_string: String,
        pub some_num: u64,
    }
}
