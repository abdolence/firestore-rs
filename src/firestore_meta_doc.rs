use crate::errors::FirestoreError;
use crate::timestamp_utils::{from_duration, from_timestamp};
use crate::FirestoreTransactionId;
use chrono::{DateTime, Duration, Utc};
use gcloud_sdk::google::firestore::v1::{Document, ExplainMetrics, RunQueryResponse};
use rsb_derive::Builder;
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreWithMetadata<T> {
    pub document: Option<T>,
    pub metadata: FirestoreDocumentMetadata,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreDocumentMetadata {
    pub transaction_id: Option<FirestoreTransactionId>,
    pub read_time: Option<DateTime<Utc>>,
    pub skipped_results: usize,
    pub explain_metrics: Option<FirestoreExplainMetrics>,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreExplainMetrics {
    pub plan_summary: Option<FirestorePlanSummary>,
    pub execution_stats: Option<FirestoreExecutionStats>,
}

pub type FirestoreDynamicStruct = BTreeMap<String, gcloud_sdk::prost_types::Value>;

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestorePlanSummary {
    pub indexes_used: Vec<FirestoreDynamicStruct>,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreExecutionStats {
    pub results_returned: usize,
    pub execution_duration: Option<Duration>,
    pub read_operations: usize,
    pub debug_stats: Option<FirestoreDynamicStruct>,
}

impl TryFrom<RunQueryResponse> for FirestoreWithMetadata<Document> {
    type Error = FirestoreError;

    fn try_from(value: RunQueryResponse) -> Result<Self, Self::Error> {
        Ok(FirestoreWithMetadata {
            document: value.document,
            metadata: FirestoreDocumentMetadata {
                transaction_id: Some(value.transaction),
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
            indexes_used: value.indexes_used.into_iter().map(|v| v.fields).collect(),
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
            debug_stats: value.debug_stats.map(|v| v.fields),
        })
    }
}
