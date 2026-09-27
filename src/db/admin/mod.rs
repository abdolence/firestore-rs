//! Declarative index, single-field override and TTL management, and bulk delete of collection
//! groups, behind the `admin` feature.
//!
//! Nothing in this module runs unless a fluent `db.fluent().indexes()...` or
//! `db.fluent().delete().bulk()...` chain is called explicitly; enabling the `admin` feature alone
//! changes no behavior at [`FirestoreDb`](crate::FirestoreDb) construction.

mod index_models;
pub use index_models::*;

mod index_diff;

mod indexes;

mod coordination;

mod bulk_delete_models;
pub use bulk_delete_models::*;

mod bulk_delete;

mod operation_wait;
