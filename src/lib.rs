//! # Firestore for Rust
//!
//! A client for Google Firestore built on the official gRPC API, with a fluent, strongly typed
//! query builder and its own Serde serializer for Firestore's protobuf values.
//!
//! Full documentation, with a chapter per topic: <https://firestore-rust.abdolence.dev>
//!
//! ## Example
//!
//! ```rust,no_run
//! use firestore::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct MyTestStructure {
//!     some_id: FirestoreDocumentId,
//!     some_string: String,
//!     some_num: u64,
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let db = FirestoreDb::new("my-project-id").await?;
//!
//! const TEST_COLLECTION_NAME: FirestoreCollectionId =
//!     FirestoreCollectionId::from_static("test");
//!
//! let my_struct = MyTestStructure {
//!     some_id: FirestoreDocumentId::from_static("test-1"),
//!     some_string: "Test".to_string(),
//!     some_num: 42,
//! };
//!
//! // Create a document from a Rust structure
//! let created: MyTestStructure = db.fluent()
//!     .insert()
//!     .into(TEST_COLLECTION_NAME)
//!     .document_id(&my_struct.some_id)
//!     .object(&my_struct)
//!     .execute()
//!     .await?;
//!
//! // Query, selecting fields by compile-time checked paths
//! let as_vec: Vec<MyTestStructure> = db.fluent()
//!     .select()
//!     .fields(paths!(MyTestStructure::{some_id, some_num, some_string}))
//!     .from(TEST_COLLECTION_NAME)
//!     .filter(|q| q.for_all([
//!         q.field(path!(MyTestStructure::some_num)).is_not_null(),
//!         q.field(path!(MyTestStructure::some_string)).eq("Test"),
//!     ]))
//!     .order(|o| o.fields([o.field(path!(MyTestStructure::some_num)).desc()]))
//!     .obj()
//!     .query()
//!     .await?;
//!
//! db.fluent()
//!     .delete()
//!     .from(TEST_COLLECTION_NAME)
//!     .document_id(&my_struct.some_id)
//!     .execute()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Where to look
//!
//! | For | Start at |
//! |---|---|
//! | Creating a client | [`FirestoreDb::new`], [`FirestoreDb::with_options`] |
//! | Every read and write | [`FirestoreDb::fluent`] |
//! | Document and collection IDs | [`FirestoreDocumentId`], [`FirestoreCollectionId`], [`ParentPathBuilder`] |
//! | Field paths in queries and updates | [`path!`], [`paths!`] |
//! | Transactions | [`FirestoreDb::run_transaction`], [`FirestoreTransaction`] |
//! | Batch writes | [`FirestoreDb::create_simple_batch_writer`], [`FirestoreDb::create_streaming_batch_writer`] |
//! | Realtime changes | [`FirestoreDb::create_listener`], [`FirestoreListener`] |
//! | Timestamps | [`FirestoreTimestamp`], [`FirestoreInstant`], [`serialize_as_timestamp`] |
//! | Schemaless documents | [`FirestoreDb::serialize_map_to_doc`], [`FirestoreValue`] |
//! | Errors | [`errors`] |
//!
#![cfg_attr(
    feature = "caching",
    doc = "Caching is enabled in this build: see [`FirestoreCache`]."
)]
//!
//! ## Cargo features
//!
//! | Feature | Effect |
//! |---|---|
//! | `tls-roots` (default) | TLS trust anchors from the platform's native root store |
//! | `tls-webpki-roots` | TLS trust anchors bundled from the `webpki-roots` crate instead |
//! | `caching-memory` | In-memory collection and document cache, kept current by a listener |
//! | `caching-persistent` | The same cache backed by an on-disk database |
//! | `admin` | Declarative index management: composite indexes, vector indexes, single-field overrides and TTL policy |
//!
//! Runnable examples for every topic:
//! <https://github.com/abdolence/firestore-rs/tree/master/examples>

#![allow(clippy::new_without_default)]
#![allow(clippy::needless_lifetimes)]
#![forbid(unsafe_code)]

/// Defines the error types used throughout the `firestore-rs` crate.
///
/// This module contains the primary [`FirestoreError`] enum
/// and various specific error structs that provide detailed information about
/// issues encountered during Firestore operations.
pub mod errors;

mod firestore_value;

pub use firestore_value::*;

mod db;

pub use db::*;

mod firestore_serde;

pub use firestore_serde::*;

mod struct_path_macro;

#[allow(unused_imports)]
pub use struct_path_macro::*;

/// Re-export of the [`macro@async_trait`] macro.
///
/// It is needed to implement the public async traits of this crate, such as
/// [`FirestoreResumeStateStorage`] and the cache backend traits, without having to depend on
/// `async-trait` directly.
pub use async_trait::async_trait;

/// Re-export of [`rvstruct::ValueStruct`].
///
/// This trait provides the `.value()` accessor on the newtypes of this crate, such as
/// [`FirestoreListenerTarget`] and [`FirestoreListenerToken`], whose inner fields are private.
/// Implementations of [`FirestoreResumeStateStorage`] need it to read those values.
pub use rvstruct::ValueStruct;

/// Re-export of the [`jiff`] crate, so that the date/time API used by this
/// library is available without depending on `jiff` explicitly.
pub use jiff;

/// An exact instant in time, used by this library for Firestore timestamps.
///
/// This is an alias for [`jiff::Timestamp`]. Prefer this alias over naming
/// `jiff::Timestamp` directly, so that your code does not need an explicit
/// `jiff` dependency and stays insulated from changes of the underlying
/// implementation.
///
/// Use it for the fields of your structures. To have a field serialized as a
/// Firestore timestamp, either annotate it with one of the
/// [`serialize_as_timestamp`] attributes, or wrap it in
/// [`FirestoreTimestamp`], which does the same without an attribute.
///
/// # Examples
///
/// ```rust
/// use firestore::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Deserialize, Serialize)]
/// struct MyTestStructure {
///     #[serde(with = "firestore::serialize_as_timestamp")]
///     created_at: FirestoreInstant,
///
///     #[serde(default)]
///     #[serde(with = "firestore::serialize_as_optional_timestamp")]
///     updated_at: Option<FirestoreInstant>,
/// }
/// ```
pub type FirestoreInstant = jiff::Timestamp;

/// The duration type used by this library.
///
/// This is an alias for [`jiff::SignedDuration`], used for values such as
/// [`FirestoreTransactionOptions::max_elapsed_time`] and the query execution
/// statistics.
pub type FirestoreDuration = jiff::SignedDuration;

/// Provides utility functions for working with Firestore timestamps.
///
/// This module includes helpers for converting between [`FirestoreInstant`]
/// and Google's `Timestamp` protobuf type, often used with `#[serde(with)]`
/// attributes for automatic conversion.
pub mod timestamp_utils;

use crate::errors::FirestoreError;

/// A type alias for `std::result::Result<T, FirestoreError>`.
///
/// This is the standard result type used throughout the `firestore-rs` crate
/// for operations that can fail, encapsulating either a successful value `T`
/// or a [`FirestoreError`].
pub type FirestoreResult<T> = std::result::Result<T, FirestoreError>;

/// A type alias for the raw Firestore document representation.
///
/// This refers to `gcloud_sdk::google::firestore::v1::Document`, which is the
/// underlying gRPC/protobuf structure for a Firestore document.
pub type FirestoreDocument = gcloud_sdk::google::firestore::v1::Document;

/// The kind of change a listener reports for one of its targets.
///
/// This refers to `gcloud_sdk::google::firestore::v1::target_change::TargetChangeType`, and is
/// re-exported so that a
/// [`FirestoreListenEvent::TargetChange`](crate::FirestoreListenEvent::TargetChange) can be
/// matched without depending on `gcloud-sdk` directly:
///
/// ```rust,no_run
/// # use firestore::*;
/// # fn example(event: FirestoreListenEvent) {
/// if let FirestoreListenEvent::TargetChange(target_change) = event {
///     match FirestoreListenerTargetChangeType::try_from(target_change.target_change_type) {
///         // The target now reflects a consistent snapshot.
///         Ok(FirestoreListenerTargetChangeType::Current) => {}
///         // Firestore dropped the target; `target_change.cause` says why.
///         Ok(FirestoreListenerTargetChangeType::Remove) => {}
///         _ => {}
///     }
/// }
/// # }
/// ```
pub type FirestoreListenerTargetChangeType =
    gcloud_sdk::google::firestore::v1::target_change::TargetChangeType;

mod firestore_meta;

pub use firestore_meta::*;

mod firestore_document_functions;

pub use firestore_document_functions::*;

mod fluent_api;

pub use fluent_api::*;

/// The crate backing [`path!`] and [`paths!`].
///
/// Re-exported because those macros expand to paths into it, so a caller cannot use them
/// without it being in scope.
pub extern crate struct_path;

#[cfg(feature = "caching")]
mod cache;

#[cfg(feature = "caching")]
pub use cache::*;

#[cfg(doctest)]
mod book;
