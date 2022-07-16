//! # Firestore for Rust
//!
//! Library provides a simple API for Google Firestore:
//! - Create or update documents using Rust structures and Serde;
//! - Support for querying / streaming / listening documents from Firestore;
//! - Full async based on Tokio runtime;
//! - Macro that helps you use JSON paths as references to your structure fields;
//! - Caching Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
//!   that automatically detects tokens or GKE environment;
//!
//! Examples available at: https://github.com/abdolence/firestore-rs/tree/master/src/examples
//!

#![allow(clippy::new_without_default)]

pub mod errors;
mod query;

pub use query::*;
mod db;
pub use db::*;

mod serde;
mod struct_path_macro;
pub use struct_path_macro::*;
