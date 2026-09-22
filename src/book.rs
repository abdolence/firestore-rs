//! Compiles the code in the book chapters as doctests, so an example cannot
//! go stale without a test failing. Only reachable through `cfg(doctest)`,
//! which rustdoc sets while collecting doctests and never while building the
//! crate, so this module adds nothing to normal builds.

#[doc = include_str!("../docs/src/intro.md")]
mod intro {}

#[doc = include_str!("../docs/src/getting-started.md")]
mod getting_started {}

#[doc = include_str!("../docs/src/client.md")]
mod client {}

#[doc = include_str!("../docs/src/fluent-api.md")]
mod fluent_api {}

#[doc = include_str!("../docs/src/querying.md")]
mod querying {}

#[doc = include_str!("../docs/src/get-and-batch-get.md")]
mod get_and_batch_get {}

#[doc = include_str!("../docs/src/timestamps.md")]
mod timestamps {}

#[doc = include_str!("../docs/src/nested-collections.md")]
mod nested_collections {}

#[doc = include_str!("../docs/src/transactions.md")]
mod transactions {}

#[doc = include_str!("../docs/src/batch-writes.md")]
mod batch_writes {}

#[doc = include_str!("../docs/src/document-metadata.md")]
mod document_metadata {}

#[doc = include_str!("../docs/src/dynamic-documents.md")]
mod dynamic_documents {}

#[doc = include_str!("../docs/src/document-transformations.md")]
mod document_transformations {}

#[doc = include_str!("../docs/src/listening-changes.md")]
mod listening_changes {}

#[doc = include_str!("../docs/src/null-serialization.md")]
mod null_serialization {}

#[doc = include_str!("../docs/src/aggregations.md")]
mod aggregations {}

#[doc = include_str!("../docs/src/preconditions.md")]
mod preconditions {}

#[doc = include_str!("../docs/src/explain-query.md")]
mod explain_query {}

#[doc = include_str!("../docs/src/request-tags.md")]
mod request_tags {}

#[doc = include_str!("../docs/src/document-collection-ids.md")]
mod document_collection_ids {}

#[doc = include_str!("../docs/src/caching.md")]
mod caching {}

#[cfg(feature = "caching")]
#[doc = include_str!("../docs/src/caching/usage.md")]
mod usage {}

#[cfg(feature = "caching")]
#[doc = include_str!("../docs/src/caching/named-documents.md")]
mod named_documents {}

#[cfg(feature = "caching")]
#[doc = include_str!("../docs/src/caching/dynamic-collections.md")]
mod dynamic_collections {}

#[cfg(feature = "caching")]
#[doc = include_str!("../docs/src/caching/modes.md")]
mod modes {}

#[cfg(feature = "caching")]
#[doc = include_str!("../docs/src/caching/load-modes.md")]
mod load_modes {}

#[cfg(feature = "caching")]
#[doc = include_str!("../docs/src/caching/updates.md")]
mod updates {}

#[doc = include_str!("../docs/src/auth.md")]
mod auth {}

#[doc = include_str!("../docs/src/docker.md")]
mod docker {}

#[doc = include_str!("../docs/src/emulator.md")]
mod emulator {}

#[doc = include_str!("../docs/src/testing.md")]
mod testing {}

/// The README's quick start, compiled so it cannot drift from the API it demonstrates.
#[doc = include_str!("../README.md")]
mod readme {}
