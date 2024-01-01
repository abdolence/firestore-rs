//! # Firestore for Rust
//!
//! Library provides a simple API for Google Firestore:
//! - Create or update documents using Rust structures and Serde;
//! - Support for querying / streaming / listing / listening changes / aggregated queries of documents from Firestore;
//! - Fluent high-level and strongly typed API;
//! - Full async based on Tokio runtime;
//! - Macro that helps you use JSON paths as references to your structure fields;
//! - Implements own Serde serializer to Firestore gRPC values;
//! - Supports for Firestore timestamp with `#[serde(with)]`;
//! - Transactions support;
//! - Streaming batch writes with automatic throttling to avoid time limits from Firestore;
//! - Aggregated Queries;
//! - Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
//!   that automatically detects GKE environment or application default accounts for local development;
//!
//! ## Example using the Fluent API:
//!
//! ```rust,no_run
//!
//!use firestore::*;
//!use serde::{Deserialize, Serialize};
//!use futures::stream::BoxStream;
//!use futures::StreamExt;
//!
//!pub fn config_env_var(name: &str) -> Result<String, String> {
//!    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
//!}
//!
//!// Example structure to play with
//!#[derive(Debug, Clone, Deserialize, Serialize)]
//!struct MyTestStructure {
//!    some_id: String,
//!    some_string: String,
//!    one_more_string: String,
//!    some_num: u64,
//!}
//!
//!#[tokio::main]
//!async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {//!
//!   // Create an instance
//!   let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;
//!
//!   const TEST_COLLECTION_NAME: &'static str = "test";
//!
//!   let my_struct = MyTestStructure {
//!        some_id: "test-1".to_string(),
//!        some_string: "Test".to_string(),
//!        one_more_string: "Test2".to_string(),
//!        some_num: 42,
//!   };
//!
//!   // Create documents
//!   let object_returned: MyTestStructure = db.fluent()
//!       .insert()
//!       .into(TEST_COLLECTION_NAME)
//!       .document_id(&my_struct.some_id)
//!       .object(&my_struct)
//!       .execute()
//!       .await?;
//!
//!   // Update documents
//!   let object_updated: MyTestStructure = db.fluent()
//!       .update()
//!       .fields(paths!(MyTestStructure::{some_num, one_more_string})) // Update only specified fields
//!       .in_col(TEST_COLLECTION_NAME)
//!       .document_id(&my_struct.some_id)
//!       .object(&MyTestStructure {
//!           some_num: my_struct.some_num + 1,
//!          one_more_string: "updated-value".to_string(),
//!           ..my_struct.clone()
//!       })
//!       .execute()
//!      .await?;
//!  
//!   // Get a document as an object by id
//!   let find_it_again: Option<MyTestStructure> = db.fluent()
//!         .select()
//!         .by_id_in(TEST_COLLECTION_NAME)
//!         .obj()
//!         .one(&my_struct.some_id)
//!         .await?;
//!
//!   // Query and read stream of objects
//!   let object_stream: BoxStream<MyTestStructure> = db.fluent()
//!     .select()
//!     .fields(paths!(MyTestStructure::{some_id, some_num, some_string, one_more_string})) // Optionally select the fields needed
//!     .from(TEST_COLLECTION_NAME)
//!     .filter(|q| { // Fluent filter API example
//!         q.for_all([
//!             q.field(path!(MyTestStructure::some_num)).is_not_null(),
//!             q.field(path!(MyTestStructure::some_string)).eq("Test"),
//!             // Sometimes you have optional filters
//!             Some("Test2")
//!                 .and_then(|value| q.field(path!(MyTestStructure::one_more_string)).eq(value)),
//!         ])
//!     })
//!     .order_by([(
//!         path!(MyTestStructure::some_num),
//!         FirestoreQueryDirection::Descending,
//!     )])
//!     .obj() // Reading documents as structures using Serde gRPC deserializer
//!     .stream_query()
//!     .await?;
//!
//!     let as_vec: Vec<MyTestStructure> = object_stream.collect().await;
//!     println!("{:?}", as_vec);
//!
//!     // Delete documents
//!     db.fluent()
//!         .delete()
//!         .from(TEST_COLLECTION_NAME)
//!         .document_id(&my_struct.some_id)
//!         .execute()
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! All examples and more docs available at: [github](https://github.com/abdolence/firestore-rs/tree/master/examples)
//!

#![allow(clippy::new_without_default)]
#![forbid(unsafe_code)]

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

pub mod timestamp_utils;

use crate::errors::FirestoreError;

pub type FirestoreResult<T> = std::result::Result<T, FirestoreError>;

pub type FirestoreDocument = gcloud_sdk::google::firestore::v1::Document;

mod firestore_document_functions;
pub use firestore_document_functions::*;

mod fluent_api;
pub use fluent_api::*;

pub extern crate struct_path;

#[cfg(feature = "caching")]
mod cache;

#[cfg(feature = "caching")]
pub use cache::*;
