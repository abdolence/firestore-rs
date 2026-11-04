use super::expr::{selection_map, wire_value};
use super::model::stage;
use crate::errors::FirestoreSerializationError;
use crate::{
    FirestoreError, FirestoreExpr, FirestorePipeline, FirestoreRequestOptions, FirestoreSelection,
    FirestoreTransactionId,
};
use gcloud_sdk::google::firestore::v1::{
    pipeline::Stage, value::ValueType, ExecutePipelineResponse, Value,
};
use rsb_derive::Builder;
use std::collections::HashMap;

/// The write a pipeline ends in.
///
/// Firestore accepts a write only as the last stage, so it is not a
/// [`FirestorePipelineStage`](crate::FirestorePipelineStage), and no pipeline can carry one in
/// the middle or in a subquery.
#[derive(Debug, Clone, PartialEq)]
pub enum FirestorePipelineWrite {
    /// Sets fields on each document. A document that does not exist is not written: outside a
    /// transaction it counts 0, and in an atomic write it fails the write as `NOT_FOUND`.
    Update(FirestoreUpdateWrite),
    /// Deletes each document.
    Delete,
    /// Creates each document, and fails as `ALREADY_EXISTS` when one exists. Firestore runs an
    /// insert from any source but `literals` only atomically or in a transaction.
    Insert(FirestoreInsertWrite),
    /// Creates or replaces each document, replacing a stored document rather than merging into
    /// it. Firestore runs an upsert only atomically or in a transaction.
    Upsert(FirestoreUpsertWrite),
}

/// The fields an update sets on each document.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreUpdateWrite {
    /// Each item needs an alias unless it is a field.
    #[default = "Vec::new()"]
    pub transforms: Vec<FirestoreSelection>,
}

/// Where an insert writes its documents.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreInsertWrite {
    /// `None` writes each document to its own `__name__`, which a literal row must hold as a full
    /// document path.
    pub target: Option<FirestoreWriteCollection>,
}

/// The fields an upsert sets on each document, and where it writes them.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreUpsertWrite {
    /// Each item needs an alias unless it is a field.
    #[default = "Vec::new()"]
    pub transforms: Vec<FirestoreSelection>,
    /// `None` writes each document to its own `__name__`, which a literal row must hold as a full
    /// document path.
    pub target: Option<FirestoreWriteCollection>,
}

/// The collection an insert or upsert writes its documents to.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreWriteCollection {
    /// Relative to the database's documents root, such as `"archive"` or
    /// `"restaurants/r1/reviews"`.
    pub collection_path: String,
    /// The ID of each document, evaluated on it. `None` keeps the ID of each document's
    /// `__name__`, and lets Firestore assign one to a row that has none.
    pub document_id: Option<FirestoreExpr>,
}

/// A pipeline, the write it ends in, and the options it runs with.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestorePipelineWriteParams {
    pub pipeline: FirestorePipeline,
    pub write: FirestorePipelineWrite,
    /// `None` writes each document on its own, so a failure can leave part of the documents
    /// written.
    pub transaction: Option<FirestorePipelineWriteTransaction>,
    /// Overrides any session wide default configured with
    /// [`FirestoreDb::clone_with_request_options`](crate::FirestoreDb::clone_with_request_options).
    pub request_options: Option<FirestoreRequestOptions>,
}

/// The transaction a pipeline write runs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FirestorePipelineWriteTransaction {
    /// A new read-write transaction that commits as soon as the write is done, so that the write
    /// applies in full or not at all.
    Atomic,
    /// An open transaction, which holds the write until it commits and drops it on rollback.
    Existing(FirestoreTransactionId),
}

/// What a pipeline write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Builder)]
pub struct FirestorePipelineWriteResult {
    pub documents_modified: u64,
}

impl From<FirestoreUpdateWrite> for FirestorePipelineWrite {
    fn from(update: FirestoreUpdateWrite) -> Self {
        Self::Update(update)
    }
}

impl From<FirestoreInsertWrite> for FirestorePipelineWrite {
    fn from(insert: FirestoreInsertWrite) -> Self {
        Self::Insert(insert)
    }
}

impl From<FirestoreUpsertWrite> for FirestorePipelineWrite {
    fn from(upsert: FirestoreUpsertWrite) -> Self {
        Self::Upsert(upsert)
    }
}

impl TryFrom<FirestorePipelineWrite> for Stage {
    type Error = FirestoreError;

    fn try_from(write: FirestorePipelineWrite) -> Result<Self, Self::Error> {
        Ok(match write {
            FirestorePipelineWrite::Update(update) => {
                stage("update", vec![selection_map("update", update.transforms)?])
            }
            FirestorePipelineWrite::Delete => stage("delete", Vec::new()),
            FirestorePipelineWrite::Insert(insert) => Stage {
                options: insert
                    .target
                    .map(HashMap::try_from)
                    .transpose()?
                    .unwrap_or_default(),
                ..stage("insert", Vec::new())
            },
            FirestorePipelineWrite::Upsert(upsert) => Stage {
                options: upsert
                    .target
                    .map(HashMap::try_from)
                    .transpose()?
                    .unwrap_or_default(),
                ..stage("upsert", vec![selection_map("upsert", upsert.transforms)?])
            },
        })
    }
}

/// The `collection` and `document_id` options of an insert or upsert stage.
impl TryFrom<FirestoreWriteCollection> for HashMap<String, Value> {
    type Error = FirestoreError;

    fn try_from(target: FirestoreWriteCollection) -> Result<Self, Self::Error> {
        let mut options = HashMap::from([(
            "collection".to_string(),
            wire_value(ValueType::ReferenceValue(format!(
                "/{}",
                target.collection_path
            ))),
        )]);
        if let Some(document_id) = target.document_id {
            options.insert("document_id".to_string(), document_id.try_into()?);
        }
        Ok(options)
    }
}

/// The result of a write run, from the responses Firestore streamed for it: a single document
/// holding the count.
impl TryFrom<Vec<ExecutePipelineResponse>> for FirestorePipelineWriteResult {
    type Error = FirestoreError;

    fn try_from(responses: Vec<ExecutePipelineResponse>) -> Result<Self, Self::Error> {
        let results: Vec<_> = responses
            .into_iter()
            .flat_map(|response| response.results)
            .collect();
        let count = match results.as_slice() {
            [result] => result.fields.get("documents_modified"),
            _ => None,
        };
        let documents_modified = match count.and_then(|count| count.value_type.as_ref()) {
            Some(ValueType::IntegerValue(count)) => u64::try_from(*count).ok(),
            _ => None,
        };
        documents_modified.map(Self::new).ok_or_else(|| {
            FirestoreError::DeserializeError(FirestoreSerializationError::from_message(format!(
                "a pipeline write returned {results:?} instead of a documents_modified count"
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pipeline::fixtures::{fixture_case, pipeline, responses};

    #[test]
    fn writes_convert_to_the_captured_last_stage() {
        let dml = FirestoreWriteCollection::new("pipeline-dml".to_string());
        let captured = [
            ("dml-delete", "all", FirestorePipelineWrite::Delete),
            (
                "dml-update",
                "transformed_fields",
                FirestoreUpdateWrite::new()
                    .with_transforms(vec![
                        FirestoreExpr::constant("updated").alias("status"),
                        FirestoreExpr::field("n").add(10).alias("n"),
                    ])
                    .into(),
            ),
            (
                "dml-update",
                "in_place_after_add_fields",
                FirestoreUpdateWrite::new().into(),
            ),
            ("dml-insert", "by_name", FirestoreInsertWrite::new().into()),
            (
                "dml-insert",
                "collection_option",
                FirestoreInsertWrite::new().with_target(dml.clone()).into(),
            ),
            (
                "dml-insert",
                "collection_and_document_id_options",
                FirestoreInsertWrite::new()
                    .with_target(dml.with_document_id(FirestoreExpr::field("key")))
                    .into(),
            ),
            (
                "dml-upsert",
                "atomic",
                FirestoreUpsertWrite::new()
                    .with_transforms(vec![FirestoreExpr::constant("upsert").alias("source")])
                    .into(),
            ),
        ];

        for (fixture, case, write) in captured {
            let expected = pipeline(&fixture_case(fixture, Some(case)))
                .stages
                .pop()
                .unwrap();
            assert_eq!(
                Stage::try_from(write).unwrap(),
                expected,
                "{fixture}#{case}"
            );
        }
    }

    #[test]
    fn a_write_result_is_the_count_of_its_single_result() {
        let deleted = responses(&fixture_case("dml-delete", Some("all")));
        let read = responses(&fixture_case("stage-select", None));

        assert_eq!(
            FirestorePipelineWriteResult::try_from(deleted).unwrap(),
            FirestorePipelineWriteResult::new(9)
        );
        assert!(matches!(
            FirestorePipelineWriteResult::try_from(read),
            Err(FirestoreError::DeserializeError(_))
        ));
    }
}
