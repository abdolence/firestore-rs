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

#[doc = include_str!("../docs/src/editions.md")]
mod editions {}

#[doc = include_str!("../docs/src/editions/semantics.md")]
mod editions_semantics {}

#[doc = include_str!("../docs/src/editions/query-plans.md")]
mod editions_query_plans {}

#[doc = include_str!("../docs/src/editions/choosing-an-api.md")]
mod editions_choosing_an_api {}

#[doc = include_str!("../docs/src/vector-search.md")]
mod vector_search {}

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

#[doc = include_str!("../docs/src/basic-writes.md")]
mod basic_writes {}

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

#[doc = include_str!("../docs/src/pipelines.md")]
mod pipelines {}

#[doc = include_str!("../docs/src/pipeline-aggregations-and-joins.md")]
mod pipeline_aggregations_and_joins {}

#[doc = include_str!("../docs/src/pipeline-search.md")]
mod pipeline_search {}

#[doc = include_str!("../docs/src/pipeline-writes.md")]
mod pipeline_writes {}

#[cfg(feature = "sql")]
#[doc = include_str!("../docs/src/sql.md")]
mod sql {}

#[cfg(feature = "sql")]
#[doc = include_str!("../docs/src/sql/reads.md")]
mod sql_reads {}

#[cfg(feature = "sql")]
#[doc = include_str!("../docs/src/sql/writes.md")]
mod sql_writes {}

#[cfg(all(feature = "sql", feature = "admin"))]
#[doc = include_str!("../docs/src/sql/indexes.md")]
mod sql_indexes {}

#[cfg(feature = "codegen")]
#[doc = include_str!("../docs/src/sql/codegen.md")]
mod sql_codegen {}

#[doc = include_str!("../docs/src/request-tags.md")]
mod request_tags {}

#[doc = include_str!("../docs/src/request-timeouts.md")]
mod request_timeouts {}

#[doc = include_str!("../docs/src/document-collection-ids.md")]
mod document_collection_ids {}

#[cfg(feature = "json-conversion")]
#[doc = include_str!("../docs/src/json.md")]
mod json {}

#[cfg(feature = "caching")]
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

#[cfg(feature = "admin")]
#[doc = include_str!("../docs/src/index-management.md")]
mod index_management {}

#[cfg(feature = "admin")]
#[doc = include_str!("../docs/src/index-management/composite.md")]
mod index_management_composite {}

#[cfg(feature = "admin")]
#[doc = include_str!("../docs/src/index-management/search.md")]
mod index_management_search {}

#[cfg(feature = "admin")]
#[doc = include_str!("../docs/src/index-management/ttl-and-enterprise.md")]
mod index_management_ttl_and_enterprise {}

#[cfg(feature = "admin")]
#[doc = include_str!("../docs/src/bulk-delete.md")]
mod bulk_delete {}

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
