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
//! - Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
//!   that automatically detects GKE environment or application default accounts for local development;
//!
//! ## Example:
//!
//! ```rust,no_run
//!use firestore::*;
//!use serde::{Deserialize, Serialize};
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
//!async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!    // Create an instance
//!    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;
//!
//!    const TEST_COLLECTION_NAME: &'static str = "test";
//!
//!    let my_struct = MyTestStructure {
//!        some_id: "test-1".to_string(),
//!        some_string: "Test".to_string(),
//!        one_more_string: "Test2".to_string(),
//!        some_num: 42,
//!    };
//!
//!    // Remove if it already exist
//!    db.delete_by_id(TEST_COLLECTION_NAME, &my_struct.some_id)
//!        .await?;
//!
//!    // Let's insert some data
//!    db.create_obj(TEST_COLLECTION_NAME, &my_struct.some_id, &my_struct)
//!        .await?;
//!
//!    // Update some field in it
//!    let updated_obj = db
//!        .update_obj(
//!            TEST_COLLECTION_NAME,
//!            &my_struct.some_id,
//!            &MyTestStructure {
//!                some_num: my_struct.some_num + 1,
//!                some_string: "updated-value".to_string(),
//!                ..my_struct.clone()
//!            },
//!            Some(paths!(MyTestStructure::{
//!                some_num,
//!                some_string
//!            })),
//!        )
//!        .await?;
//!
//!    println!("Updated object: {:?}", updated_obj);
//!
//!    // Get object by id
//!    let find_it_again: MyTestStructure =
//!        db.get_obj(TEST_COLLECTION_NAME, &my_struct.some_id).await?;
//!
//!    println!("Should be the same: {:?}", find_it_again);
//!
//!    // Query our data
//!    let objects: Vec<MyTestStructure> = db
//!        .query_obj(
//!            FirestoreQueryParams::new(TEST_COLLECTION_NAME.into()).with_filter(
//!                FirestoreQueryFilter::Compare(Some(FirestoreQueryFilterCompare::Equal(
//!                    path!(MyTestStructure::some_num),
//!                    find_it_again.some_num.into(),
//!                ))),
//!            ),
//!        )
//!        .await?;
//!
//!    println!("Now in the list: {:?}", objects);
//!
//!    Ok(())
//!}
//! ```
//!
//! ## Fluent API
//! To simplify development and developer experience the library provides the Fluent API:
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
//!    // Create an instance
//!    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;
//!
//!    const TEST_COLLECTION_NAME: &'static str = "test";
//!
//!    // Query as a stream our data
//!    let object_stream: BoxStream<MyTestStructure> = db.fluent()
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
use crate::errors::FirestoreError;
pub use struct_path_macro::*;

pub mod timestamp_utils;

pub type FirestoreResult<T> = std::result::Result<T, FirestoreError>;

mod fluent_api;
pub use fluent_api::*;

pub extern crate struct_path;
