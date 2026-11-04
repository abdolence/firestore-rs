use crate::errors::FirestoreSerializationError;
use crate::timestamp_utils::from_timestamp;
use crate::{FirestoreDocument, FirestoreError, FirestoreInstant, FirestoreResult};
use gcloud_sdk::google::firestore::v1::{ExecutePipelineResponse, ExplainStats};
use rsb_derive::Builder;

/// A pipeline result with the metadata of the response it arrived in.
#[derive(Debug, Clone, PartialEq)]
pub struct FirestoreWithPipelineMetadata<T> {
    /// `None` for the item that ends a response with no results, or one with explain stats.
    pub document: Option<T>,
    pub metadata: FirestorePipelineMetadata,
}

/// The metadata of one pipeline response.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestorePipelineMetadata {
    /// The time at which the results were valid, as `read_time` is for a Core query.
    pub execution_time: Option<FirestoreInstant>,
    /// Present only on the item without a document that ends a pipeline run with explain
    /// options.
    pub explain_stats: Option<FirestorePipelineExplainStats>,
}

/// The Query Explain output of a pipeline.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestorePipelineExplainStats {
    /// The plan, and with `analyze` the execution profile, as Firestore formats it. A plan whose
    /// leaf node is `TableScan` reads every document from primary storage. The format is
    /// Firestore's and is not parsed here, since it can change at any time.
    pub text: String,
}

impl FirestoreWithPipelineMetadata<FirestoreDocument> {
    /// One item per document of `response`, each with the response's execution time, then one
    /// item without a document when the response carries explain stats or no documents at all.
    pub(crate) fn from_response(response: ExecutePipelineResponse) -> FirestoreResult<Vec<Self>> {
        let execution_time = response.execution_time.map(from_timestamp).transpose()?;
        let ends_with_metadata = response.explain_stats.is_some() || response.results.is_empty();
        let mut items: Vec<Self> = response
            .results
            .into_iter()
            .map(|document| FirestoreWithPipelineMetadata {
                document: Some(document),
                metadata: FirestorePipelineMetadata::new().opt_execution_time(execution_time),
            })
            .collect();
        if ends_with_metadata {
            items.push(FirestoreWithPipelineMetadata {
                document: None,
                metadata: FirestorePipelineMetadata {
                    execution_time,
                    explain_stats: response.explain_stats.map(TryInto::try_into).transpose()?,
                },
            });
        }
        Ok(items)
    }
}

impl TryFrom<ExplainStats> for FirestorePipelineExplainStats {
    type Error = FirestoreError;

    fn try_from(stats: ExplainStats) -> Result<Self, Self::Error> {
        let data = stats.data.ok_or_else(|| {
            FirestoreError::DeserializeError(FirestoreSerializationError::from_message(
                "explain stats without data",
            ))
        })?;
        data.to_msg::<String>()
            .map(|text| FirestorePipelineExplainStats { text })
            .map_err(|err| {
                FirestoreError::DeserializeError(FirestoreSerializationError::from_message(
                    format!("explain stats of type {}: {err}", data.type_url),
                ))
            })
    }
}
