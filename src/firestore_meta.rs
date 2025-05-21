use crate::errors::FirestoreError;
use crate::timestamp_utils::{from_duration, from_timestamp};
use crate::FirestoreTransactionId;
use chrono::{DateTime, Duration, Utc};
use gcloud_sdk::google::firestore::v1::{Document, ExplainMetrics, RunQueryResponse};
use gcloud_sdk::prost_types::value::Kind;
use rsb_derive::Builder;
use std::collections::BTreeMap;

/// A container that pairs a document (or other data `T`) with its associated Firestore metadata.
///
/// This is often used in query responses where each document is accompanied by
/// metadata like its read time, transaction ID (if applicable), and potentially
/// query explanation metrics.
///
/// # Type Parameters
/// * `T`: The type of the document or data being wrapped. Typically `gcloud_sdk::google::firestore::v1::Document`
///   or a user-defined struct after deserialization.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreWithMetadata<T> {
    /// The document or data itself. This is an `Option` because some Firestore
    /// operations (like a query that finds no results but still has metadata)
    /// might not return a document.
    pub document: Option<T>,
    /// The metadata associated with the document or operation.
    pub metadata: FirestoreDocumentMetadata,
}

/// Metadata associated with a Firestore document or a query operation.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreDocumentMetadata {
    /// The ID of the transaction that was started as part of this request.
    /// Present if the request initiated a transaction.
    pub transaction_id: Option<FirestoreTransactionId>,
    /// The time at which the document was read. This may be monotically increasing.
    pub read_time: Option<DateTime<Utc>>,
    /// The number of results that were skipped before returning the first result.
    /// This is relevant for paginated queries or queries with an offset.
    pub skipped_results: usize,
    /// Query execution explanation metrics, if requested and available.
    pub explain_metrics: Option<FirestoreExplainMetrics>,
}

/// Detailed metrics about query execution, if requested via [`FirestoreExplainOptions`](crate::FirestoreExplainOptions).
///
/// This includes a summary of the query plan and statistics about the execution.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreExplainMetrics {
    /// A summary of the query plan.
    pub plan_summary: Option<FirestorePlanSummary>,
    /// Statistics about the query execution.
    pub execution_stats: Option<FirestoreExecutionStats>,
}

/// A dynamically-typed structure used to represent complex, nested data
/// often found in Firestore's explain metrics or other metadata.
///
/// It holds fields as a `BTreeMap` of string keys to `gcloud_sdk::prost_types::Value`,
/// which is a generic protobuf value type capable of holding various data types.
#[derive(PartialEq, Clone, Builder)]
pub struct FirestoreDynamicStruct {
    /// A map of field names to their `prost_types::Value` representations.
    pub fields: BTreeMap<String, gcloud_sdk::prost_types::Value>,
}

impl std::fmt::Debug for FirestoreDynamicStruct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn pretty_print(v: &gcloud_sdk::prost_types::Value) -> String {
            match v.kind.as_ref() {
                Some(Kind::NullValue(_)) => "null".to_string(),
                Some(Kind::BoolValue(v)) => v.to_string(),
                Some(Kind::NumberValue(v)) => v.to_string(),
                Some(Kind::StringValue(v)) => format!("'{}'", v),
                Some(Kind::StructValue(v)) => v
                    .fields
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, pretty_print(v)))
                    .collect::<Vec<String>>()
                    .join(", "),
                Some(Kind::ListValue(v)) => v
                    .values
                    .iter()
                    .map(pretty_print)
                    .collect::<Vec<String>>()
                    .join(", "),
                None => "".to_string(),
            }
        }
        let pretty_print_fields = self
            .fields
            .iter()
            .map(|(k, v)| format!("{}: {}", k, pretty_print(v)))
            .collect::<Vec<String>>()
            .join(", ");
        f.debug_struct("FirestoreDynamicStruct")
            .field("fields", &pretty_print_fields)
            .finish()
    }
}

/// A summary of the query plan used by Firestore to execute a query.
///
/// This is part of the [`FirestoreExplainMetrics`] and provides insights into
/// how Firestore satisfied the query, particularly which indexes were utilized.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestorePlanSummary {
    /// A list of indexes used to execute the query. Each index is represented
    /// as a [`FirestoreDynamicStruct`] containing details about the index.
    pub indexes_used: Vec<FirestoreDynamicStruct>,
}

/// Statistics related to the execution of a Firestore query.
///
/// This is part of the [`FirestoreExplainMetrics`] and provides performance-related
/// information such as the number of results, execution time, and read operations.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreExecutionStats {
    /// The number of results returned by the query.
    pub results_returned: usize,
    /// The duration it took to execute the query on the server.
    pub execution_duration: Option<Duration>,
    /// The number of read operations performed by the query.
    pub read_operations: usize,
    /// Additional debugging statistics, represented as a [`FirestoreDynamicStruct`].
    /// The content of this can vary and is intended for debugging purposes.
    pub debug_stats: Option<FirestoreDynamicStruct>,
}

impl TryFrom<RunQueryResponse> for FirestoreWithMetadata<Document> {
    type Error = FirestoreError;

    fn try_from(value: RunQueryResponse) -> Result<Self, Self::Error> {
        Ok(FirestoreWithMetadata {
            document: value.document,
            metadata: FirestoreDocumentMetadata {
                transaction_id: if !value.transaction.is_empty() {
                    Some(value.transaction)
                } else {
                    None
                },
                read_time: value.read_time.map(from_timestamp).transpose()?,
                skipped_results: value.skipped_results as usize,
                explain_metrics: value.explain_metrics.map(|v| v.try_into()).transpose()?,
            },
        })
    }
}

impl TryFrom<ExplainMetrics> for FirestoreExplainMetrics {
    type Error = FirestoreError;

    fn try_from(value: ExplainMetrics) -> Result<Self, Self::Error> {
        Ok(FirestoreExplainMetrics {
            plan_summary: value.plan_summary.map(|v| v.try_into()).transpose()?,
            execution_stats: value.execution_stats.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl TryFrom<gcloud_sdk::google::firestore::v1::PlanSummary> for FirestorePlanSummary {
    type Error = FirestoreError;

    fn try_from(
        value: gcloud_sdk::google::firestore::v1::PlanSummary,
    ) -> Result<Self, Self::Error> {
        Ok(FirestorePlanSummary {
            indexes_used: value
                .indexes_used
                .into_iter()
                .map(|v| FirestoreDynamicStruct::new(v.fields))
                .collect(),
        })
    }
}

impl TryFrom<gcloud_sdk::google::firestore::v1::ExecutionStats> for FirestoreExecutionStats {
    type Error = FirestoreError;

    fn try_from(
        value: gcloud_sdk::google::firestore::v1::ExecutionStats,
    ) -> Result<Self, Self::Error> {
        Ok(FirestoreExecutionStats {
            results_returned: value.results_returned as usize,
            execution_duration: value.execution_duration.map(from_duration),
            read_operations: value.read_operations as usize,
            debug_stats: value
                .debug_stats
                .map(|v| FirestoreDynamicStruct::new(v.fields)),
        })
    }
}
