use super::expr::wire_value;
use crate::{FirestoreExplainOptions, FirestorePipeline, FirestoreRequestOptions};
use gcloud_sdk::google::firestore::v1::{value::ValueType, MapValue, Value};
use rsb_derive::Builder;
use std::collections::HashMap;

/// A pipeline and the options it runs with.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestorePipelineParams {
    pub pipeline: FirestorePipeline,
    /// Runs the pipeline with Query Explain: `analyze` returns the documents and the execution
    /// profile, otherwise only the plan is returned and no document is read.
    pub explain_options: Option<FirestoreExplainOptions>,
    /// Overrides any session wide default configured with
    /// [`FirestoreDb::clone_with_request_options`](crate::FirestoreDb::clone_with_request_options).
    pub request_options: Option<FirestoreRequestOptions>,
}

/// The pipeline option `explain_options`, in the `text` output format that
/// [`FirestorePipelineExplainStats`](crate::FirestorePipelineExplainStats) holds.
impl From<&FirestoreExplainOptions> for Value {
    fn from(options: &FirestoreExplainOptions) -> Self {
        let mode = if options.analyze.unwrap_or(false) {
            "analyze"
        } else {
            "explain"
        };
        wire_value(ValueType::MapValue(MapValue {
            fields: HashMap::from([
                (
                    "mode".to_string(),
                    wire_value(ValueType::StringValue(mode.to_string())),
                ),
                (
                    "output_format".to_string(),
                    wire_value(ValueType::StringValue("text".to_string())),
                ),
            ]),
        }))
    }
}
