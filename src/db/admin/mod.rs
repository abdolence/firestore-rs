//! Declarative index, single-field override and TTL management, behind the `admin` feature.
//!
//! Nothing in this module runs unless a fluent `db.fluent().indexes()...` chain is called
//! explicitly; enabling the `admin` feature alone changes no behavior at
//! [`FirestoreDb`](crate::FirestoreDb) construction.

mod index_models;
pub use index_models::*;

mod index_diff;

mod indexes;
