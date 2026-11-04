use super::expr::{selection_map, wire_value};
use crate::db::validate_path_segment;
use crate::{
    FirestoreAggregateSelection, FirestoreBoolExpr, FirestoreError, FirestoreExpr,
    FirestoreFindNearestDistanceMeasure, FirestorePipelineOrdering, FirestoreReference,
    FirestoreResult, FirestoreSearch, FirestoreSelection, FirestoreValue, FirestoreVector,
};
use gcloud_sdk::google::firestore::v1::{pipeline::Stage, value::ValueType, Pipeline, Value};
use rsb_derive::Builder;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// A pipeline: a source, then stages that each work on the documents of the one before.
///
/// The fluent API builds one from
/// [`FirestoreExprBuilder::pipeline()`](crate::FirestoreExprBuilder::pipeline), and runs one
/// built by hand through `db.fluent().pipeline().select().from_pipeline(pipeline)`.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestorePipeline {
    pub source: FirestorePipelineSource,
    #[default = "Vec::new()"]
    pub stages: Vec<FirestorePipelineStage>,
}

impl FirestorePipeline {
    /// Returns the pipeline with `stage` appended.
    pub fn with_stage(mut self, stage: FirestorePipelineStage) -> Self {
        self.stages.push(stage);
        self
    }
}

/// The first stage of a pipeline, which reads the documents the other stages work on.
#[derive(Debug, Clone, PartialEq)]
pub enum FirestorePipelineSource {
    Collection(FirestoreCollectionSource),
    CollectionGroup(FirestoreCollectionGroupSource),
    /// Every document of the database.
    Database,
    Documents(FirestoreDocumentsSource),
    Literals(FirestoreLiteralsSource),
    /// A collection under each document of the enclosing pipeline. Preview, and valid only at
    /// the start of a subquery: a top-level pipeline with this source fails as
    /// `INVALID_ARGUMENT`.
    Subcollection(FirestoreSubcollectionSource),
}

/// The documents of one collection.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreCollectionSource {
    pub collection_id: String,
    /// The full path of the document the collection is nested under, as
    /// [`FirestoreDb::parent_path`](crate::FirestoreDb::parent_path) builds it; `None` for a
    /// root collection. The pipeline reads the collection in the database it runs on, whichever
    /// project and database the path names.
    pub parent: Option<String>,
    pub force_index: Option<FirestoreIndexHint>,
    /// Searches the collection. Firestore accepts a search only as the stage right after the
    /// source, so the source carries it.
    pub search: Option<FirestoreSearch>,
}

/// The documents of every collection with this ID, anywhere in the database.
///
/// Unlike a Core collection-group query, it cannot be limited to the descendants of one
/// document: Firestore ignores the stage's ancestor argument.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreCollectionGroupSource {
    pub collection_id: String,
    pub force_index: Option<FirestoreIndexHint>,
    /// Searches the collection group, which needs a search index scoped to the collection
    /// group. Firestore accepts a search only as the stage right after the source, so the source
    /// carries it.
    pub search: Option<FirestoreSearch>,
}

/// Documents by path. A path that names no document is left out, not an error.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreDocumentsSource {
    /// Paths relative to the database's documents root, such as `"books/b1"`.
    pub paths: Vec<String>,
}

/// Rows given in the request, rather than read from the database.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreLiteralsSource {
    /// Each row must be a map. Converting the pipeline fails with
    /// [`InvalidParametersError`](crate::errors::FirestoreError::InvalidParametersError) for
    /// any other value, which includes a row whose serialisation failed.
    pub rows: Vec<FirestoreValue>,
}

/// The collection `collection_id` under the current document of the enclosing pipeline, for a
/// subquery. Preview.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSubcollectionSource {
    pub collection_id: String,
}

/// The index a collection or collection-group source must read through.
///
/// An index Firestore does not have fails the request as `INVALID_ARGUMENT`. An index that
/// cannot serve the pipeline's filter is still used, by reading all of it.
#[derive(Debug, Clone, PartialEq)]
pub enum FirestoreIndexHint {
    /// Reads every document from primary storage instead of an index.
    TableScan,
    Index(FirestoreIndexId),
}

/// An index ID as Firestore assigns it, such as `CICAgJim14AK`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FirestoreIndexId(String);

impl FirestoreIndexId {
    /// Fails with
    /// [`InvalidParametersError`](crate::errors::FirestoreError::InvalidParametersError) for an
    /// empty ID.
    pub fn new<S: Into<String>>(id: S) -> FirestoreResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(FirestoreError::invalid_parameters(
                "index_id",
                "an index ID must not be empty",
            ));
        }
        Ok(Self(id))
    }
}

impl FromStr for FirestoreIndexId {
    type Err = FirestoreError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

impl Display for FirestoreIndexId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A stage after the source.
#[derive(Debug, Clone, PartialEq)]
pub enum FirestorePipelineStage {
    Where(FirestoreWhereStage),
    Fields(FirestoreFieldsStage),
    AddFields(FirestoreAddFieldsStage),
    RemoveFields(FirestoreRemoveFieldsStage),
    Sort(FirestoreSortStage),
    Limit(FirestoreLimitStage),
    Offset(FirestoreOffsetStage),
    Raw(FirestoreRawStage),
    Aggregate(FirestoreAggregateStage),
    Distinct(FirestoreDistinctStage),
    FindNearest(FirestoreFindNearestStage),
    Sample(FirestoreSampleStage),
    Union(FirestoreUnionStage),
    Unnest(FirestoreUnnestStage),
    ReplaceWith(FirestoreReplaceWithStage),
    /// The wire stage `let`.
    DefineVariables(FirestoreDefineVariablesStage),
}

/// Keeps the documents for which `condition` is true.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreWhereStage {
    pub condition: FirestoreBoolExpr,
}

/// Replaces each document with the chosen fields. Its results carry no document name unless
/// `__name__` is chosen. The wire stage is `select`.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreFieldsStage {
    pub fields: Vec<FirestoreSelection>,
}

/// Adds computed fields to each document, keeping the others.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreAddFieldsStage {
    pub fields: Vec<FirestoreSelection>,
}

/// Removes fields, by path, from each document.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreRemoveFieldsStage {
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSortStage {
    pub orderings: Vec<FirestorePipelineOrdering>,
}

#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreLimitStage {
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreOffsetStage {
    pub offset: u32,
}

/// Any stage by its wire name, for a stage that has no type of its own.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreRawStage {
    pub name: String,
    #[default = "Vec::new()"]
    pub args: Vec<FirestoreExpr>,
    #[default = "BTreeMap::new()"]
    pub options: BTreeMap<String, FirestoreExpr>,
}

/// Replaces the documents with one document per group, holding the accumulators computed over
/// each group, or over all documents when `groups` is empty.
///
/// A variable defined before this stage is not visible after it.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreAggregateStage {
    pub accumulators: Vec<FirestoreAggregateSelection>,
    #[default = "Vec::new()"]
    pub groups: Vec<FirestoreSelection>,
}

/// Replaces the documents with one document per distinct combination of the selected values.
/// A null value is a group of its own.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreDistinctStage {
    pub groups: Vec<FirestoreSelection>,
}

/// Keeps the `neighbors_limit` documents whose vector field is nearest to `query_vector`.
///
/// This is the stage and the options type at once. It has the fields of Core's
/// [`FirestoreFindNearestOptions`](crate::FirestoreFindNearestOptions) except
/// `distance_threshold`, which the stage rejects. It scans every document and sorts by
/// distance: the pipeline stage does not use a vector index.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreFindNearestStage {
    /// The path of the vector field.
    pub field_name: String,
    pub query_vector: FirestoreVector,
    pub distance_measure: FirestoreFindNearestDistanceMeasure,
    pub neighbors_limit: u32,
    /// The field to output each document's distance under.
    pub distance_result_field: Option<String>,
}

/// Keeps a random sample of the documents.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSampleStage {
    pub size: FirestoreSampleSize,
}

/// How many documents a `sample` stage keeps.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FirestoreSampleSize {
    /// This many documents.
    Documents(u32),
    /// This fraction of the documents, from 0.0 to 1.0. Firestore calls the mode `percent`, but
    /// it takes a fraction.
    Fraction(f64),
}

/// Appends the documents of another pipeline.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreUnionStage {
    pub other: FirestorePipeline,
}

/// Replaces each document by one document per element of an array.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreUnnestStage {
    /// The array expression, and the field name each element is output under. Without an alias
    /// the expression must be a field, and its elements replace it.
    pub selection: FirestoreSelection,
    /// The field to output each element's position in the array under.
    pub index_field: Option<String>,
}

/// Replaces each document with the value of an expression, which must be a map.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreReplaceWithStage {
    pub expr: FirestoreExpr,
    pub mode: FirestoreReplaceWithMode,
}

/// What a `replace_with` stage does with the fields the map and the document have in common
/// and those only one has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirestoreReplaceWithMode {
    /// The map becomes the document.
    FullReplace,
    /// The map's fields are added to the document, and replace its fields of the same name.
    MergeOverwriteExisting,
    /// The map's fields are added to the document, and its fields of the same name stay.
    MergeKeepExisting,
}

/// Defines variables for the stages after it, read with
/// [`variable`](crate::pipeline_expr_builder::FirestorePipelineExprBuilder::variable). Preview.
/// The wire stage is `let`, which is a Rust keyword.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreDefineVariablesStage {
    pub variables: Vec<FirestoreSelection>,
}

impl TryFrom<FirestorePipeline> for Pipeline {
    type Error = FirestoreError;

    fn try_from(pipeline: FirestorePipeline) -> Result<Self, Self::Error> {
        let mut stages = Vec::<Stage>::try_from(pipeline.source)?;
        for stage in pipeline.stages {
            stages.push(stage.try_into()?);
        }
        Ok(Pipeline { stages })
    }
}

/// The source stage, followed by its `search` stage when it has one.
impl TryFrom<FirestorePipelineSource> for Vec<Stage> {
    type Error = FirestoreError;

    fn try_from(source: FirestorePipelineSource) -> Result<Self, Self::Error> {
        let (source, search) = match source {
            FirestorePipelineSource::Collection(mut source) => {
                let search = source.search.take();
                (Stage::try_from(source)?, search)
            }
            FirestorePipelineSource::CollectionGroup(mut source) => {
                let search = source.search.take();
                (source.into(), search)
            }
            FirestorePipelineSource::Database => (stage("database", Vec::new()), None),
            FirestorePipelineSource::Documents(source) => (source.into(), None),
            FirestorePipelineSource::Literals(source) => (source.try_into()?, None),
            FirestorePipelineSource::Subcollection(source) => (source.into(), None),
        };
        let mut stages = vec![source];
        stages.extend(search.map(Stage::try_from).transpose()?);
        Ok(stages)
    }
}

impl TryFrom<FirestoreCollectionSource> for Stage {
    type Error = FirestoreError;

    fn try_from(source: FirestoreCollectionSource) -> Result<Self, Self::Error> {
        // The collection ID is joined into a path, where a slash would name another collection.
        validate_path_segment(&source.collection_id, "collection_id")?;
        let path = match source.parent {
            Some(parent) => {
                let parent = FirestoreReference(parent);
                let parent_path = parent.parse().map_err(|_| {
                    FirestoreError::invalid_parameters(
                        "parent",
                        format!("{} is not a full document path", parent.as_str()),
                    )
                })?;
                format!("/{}/{}", parent_path.path(), source.collection_id)
            }
            None => format!("/{}", source.collection_id),
        };
        Ok(collection_stage(
            "collection",
            vec![wire_value(ValueType::ReferenceValue(path))],
            source.force_index,
        ))
    }
}

impl From<FirestoreCollectionGroupSource> for Stage {
    fn from(source: FirestoreCollectionGroupSource) -> Self {
        collection_stage(
            "collection_group",
            vec![
                // Firestore ignores the ancestor, so it is always the database root.
                wire_value(ValueType::ReferenceValue(String::new())),
                wire_value(ValueType::StringValue(source.collection_id)),
            ],
            source.force_index,
        )
    }
}

impl From<FirestoreDocumentsSource> for Stage {
    fn from(source: FirestoreDocumentsSource) -> Self {
        stage(
            "documents",
            source
                .paths
                .into_iter()
                .map(|path| wire_value(ValueType::ReferenceValue(format!("/{path}"))))
                .collect(),
        )
    }
}

impl TryFrom<FirestoreLiteralsSource> for Stage {
    type Error = FirestoreError;

    fn try_from(source: FirestoreLiteralsSource) -> Result<Self, Self::Error> {
        let rows = source
            .rows
            .into_iter()
            .enumerate()
            .map(|(index, row)| match row.value.value_type {
                Some(ValueType::MapValue(_)) => Ok(row.value),
                _ => Err(FirestoreError::invalid_parameters(
                    "literals",
                    format!("row {index} is not a map"),
                )),
            })
            .collect::<FirestoreResult<_>>()?;
        Ok(stage("literals", rows))
    }
}

impl From<FirestoreIndexHint> for Value {
    fn from(hint: FirestoreIndexHint) -> Self {
        wire_value(ValueType::StringValue(match hint {
            FirestoreIndexHint::TableScan => "primary".to_string(),
            FirestoreIndexHint::Index(id) => id.0,
        }))
    }
}

impl TryFrom<FirestorePipelineStage> for Stage {
    type Error = FirestoreError;

    fn try_from(stage: FirestorePipelineStage) -> Result<Self, Self::Error> {
        match stage {
            FirestorePipelineStage::Where(stage) => stage.try_into(),
            FirestorePipelineStage::Fields(stage) => stage.try_into(),
            FirestorePipelineStage::AddFields(stage) => stage.try_into(),
            FirestorePipelineStage::RemoveFields(stage) => Ok(stage.into()),
            FirestorePipelineStage::Sort(stage) => stage.try_into(),
            FirestorePipelineStage::Limit(stage) => Ok(stage.into()),
            FirestorePipelineStage::Offset(stage) => Ok(stage.into()),
            FirestorePipelineStage::Raw(stage) => stage.try_into(),
            FirestorePipelineStage::Aggregate(stage) => stage.try_into(),
            FirestorePipelineStage::Distinct(stage) => stage.try_into(),
            FirestorePipelineStage::FindNearest(stage) => stage.try_into(),
            FirestorePipelineStage::Sample(stage) => Ok(stage.into()),
            FirestorePipelineStage::Union(stage) => stage.try_into(),
            FirestorePipelineStage::Unnest(stage) => stage.try_into(),
            FirestorePipelineStage::ReplaceWith(stage) => stage.try_into(),
            FirestorePipelineStage::DefineVariables(stage) => stage.try_into(),
        }
    }
}

impl TryFrom<FirestoreWhereStage> for Stage {
    type Error = FirestoreError;

    fn try_from(where_stage: FirestoreWhereStage) -> Result<Self, Self::Error> {
        Ok(stage(
            "where",
            vec![FirestoreExpr::from(where_stage.condition).try_into()?],
        ))
    }
}

impl TryFrom<FirestoreFieldsStage> for Stage {
    type Error = FirestoreError;

    fn try_from(select: FirestoreFieldsStage) -> Result<Self, Self::Error> {
        Ok(stage(
            "select",
            vec![selection_map("select", select.fields)?],
        ))
    }
}

impl TryFrom<FirestoreAddFieldsStage> for Stage {
    type Error = FirestoreError;

    fn try_from(add_fields: FirestoreAddFieldsStage) -> Result<Self, Self::Error> {
        Ok(stage(
            "add_fields",
            vec![selection_map("add_fields", add_fields.fields)?],
        ))
    }
}

impl From<FirestoreRemoveFieldsStage> for Stage {
    fn from(remove_fields: FirestoreRemoveFieldsStage) -> Self {
        stage(
            "remove_fields",
            remove_fields
                .fields
                .into_iter()
                .map(|path| wire_value(ValueType::FieldReferenceValue(path)))
                .collect(),
        )
    }
}

impl TryFrom<FirestoreSortStage> for Stage {
    type Error = FirestoreError;

    fn try_from(sort: FirestoreSortStage) -> Result<Self, Self::Error> {
        Ok(stage(
            "sort",
            sort.orderings
                .into_iter()
                .map(Value::try_from)
                .collect::<FirestoreResult<_>>()?,
        ))
    }
}

impl From<FirestoreLimitStage> for Stage {
    fn from(limit: FirestoreLimitStage) -> Self {
        stage(
            "limit",
            vec![wire_value(ValueType::IntegerValue(limit.limit.into()))],
        )
    }
}

impl From<FirestoreOffsetStage> for Stage {
    fn from(offset: FirestoreOffsetStage) -> Self {
        stage(
            "offset",
            vec![wire_value(ValueType::IntegerValue(offset.offset.into()))],
        )
    }
}

impl TryFrom<FirestoreRawStage> for Stage {
    type Error = FirestoreError;

    fn try_from(raw: FirestoreRawStage) -> Result<Self, Self::Error> {
        Ok(Stage {
            name: raw.name,
            args: raw
                .args
                .into_iter()
                .map(Value::try_from)
                .collect::<FirestoreResult<_>>()?,
            options: raw
                .options
                .into_iter()
                .map(|(name, expr)| Ok((name, expr.try_into()?)))
                .collect::<FirestoreResult<_>>()?,
        })
    }
}

pub(super) fn stage(name: &str, args: Vec<Value>) -> Stage {
    Stage {
        name: name.to_string(),
        args,
        options: HashMap::new(),
    }
}

/// A `collection` or `collection_group` stage, the only stages that take an index hint.
fn collection_stage(
    name: &str,
    args: Vec<Value>,
    force_index: Option<FirestoreIndexHint>,
) -> Stage {
    Stage {
        options: force_index
            .map(|hint| ("force_index".to_string(), hint.into()))
            .into_iter()
            .collect(),
        ..stage(name, args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pipeline::fixtures::{fixture_case, pipeline};
    use crate::pipeline_builder::FirestorePipelineInitialBuilder;
    use gcloud_sdk::google::firestore::v1::MapValue;

    const ENTERPRISE_DOCUMENTS: &str =
        "projects/latestbit/databases/firestore-rs-enterprise/documents";

    #[derive(serde::Serialize)]
    struct LiteralRow {
        a: i64,
        b: &'static str,
    }

    #[derive(serde::Serialize)]
    struct OptionalRow {
        present: i64,
        missing: Option<i64>,
        json_null: serde_json::Value,
    }

    #[test]
    fn a_literals_row_converts_as_a_core_document_does() {
        let row = OptionalRow {
            present: 1,
            missing: None,
            json_null: serde_json::Value::Null,
        };
        let core_fields = crate::firestore_document_from_serializable("rows/one", &row)
            .unwrap()
            .fields;

        let stage = Stage::try_from(FirestoreLiteralsSource::new(vec![row.into()])).unwrap();

        assert_eq!(
            stage.args,
            vec![wire_value(ValueType::MapValue(MapValue {
                fields: core_fields
            }))]
        );
    }

    fn builder() -> FirestorePipelineInitialBuilder<()> {
        FirestorePipelineInitialBuilder::new(())
    }

    #[test]
    fn pipelines_convert_to_the_captured_requests() {
        let books_index = FirestoreIndexId::new("CICAgJim14AK").unwrap();
        let captured = [
            (
                "stage-collection",
                None,
                builder().from("pipeline-books").into_pipeline(),
            ),
            (
                "stage-collection-group",
                None,
                builder()
                    .from("pipeline-books")
                    .all_descendants()
                    .limit(2)
                    .into_pipeline(),
            ),
            (
                "stage-collection-paths",
                Some("nested_collection"),
                builder()
                    .from("reviews")
                    .parent(format!(
                        "{ENTERPRISE_DOCUMENTS}/pipeline-restaurants/rest-01"
                    ))
                    .fields(|e| [e.field("__name__")])
                    .into_pipeline(),
            ),
            (
                "stage-database",
                None,
                builder().database().limit(2).into_pipeline(),
            ),
            (
                "stage-documents",
                None,
                builder()
                    .documents([
                        "pipeline-books/book-01",
                        "pipeline-books/book-10",
                        "pipeline-books/no-such-book",
                    ])
                    .into_pipeline(),
            ),
            (
                "stage-literals",
                None,
                builder()
                    .literals([LiteralRow { a: 1, b: "x" }, LiteralRow { a: 2, b: "y" }])
                    .into_pipeline(),
            ),
            (
                "stage-where-and-or",
                None,
                builder()
                    .from("pipeline-books")
                    .filter(|q| {
                        q.for_all([
                            q.field("genre").eq("Dystopian"),
                            q.for_any([
                                q.field("published").less_than(1950),
                                q.field("stock").greater_than(5),
                            ]),
                        ])
                    })
                    .into_pipeline(),
            ),
            (
                "stage-where-sort-offset-limit",
                None,
                builder()
                    .from("pipeline-books")
                    .filter(|q| q.field("rating").greater_than_or_equal(4.2))
                    .sort(|e| [e.field("rating").desc(), e.field("title").asc()])
                    .offset(1)
                    .limit(3)
                    .into_pipeline(),
            ),
            (
                "stage-select",
                None,
                builder()
                    .from("pipeline-books")
                    .fields(|e| {
                        [
                            FirestoreSelection::from(e.field("title")),
                            e.field("author").alias("author_name"),
                        ]
                    })
                    .limit(2)
                    .into_pipeline(),
            ),
            (
                "stage-add-remove-fields",
                None,
                builder()
                    .from("pipeline-books")
                    .add_fields(|e| {
                        [
                            e.field("published").less_than(1950).alias("is_classic"),
                            e.raw("to_upper", [e.field("title")]).alias("title_upper"),
                        ]
                    })
                    .remove_fields(["awards", "embedding", "tags"])
                    .limit(2)
                    .into_pipeline(),
            ),
            (
                "force-index",
                Some("primary"),
                builder()
                    .from("pipeline-books")
                    .force_index(FirestoreIndexHint::TableScan)
                    .filter(|q| q.field("genre").eq("Dystopian"))
                    .sort(|e| [e.field("rating").desc()])
                    .into_pipeline(),
            ),
            (
                "force-index",
                Some("cg-primary"),
                builder()
                    .from("pipeline-books")
                    .all_descendants()
                    .force_index(FirestoreIndexHint::TableScan)
                    .limit(1)
                    .into_pipeline(),
            ),
            (
                "force-index",
                Some("matching"),
                builder()
                    .from("pipeline-books")
                    .force_index(FirestoreIndexHint::Index(books_index))
                    .filter(|q| q.field("genre").eq("Dystopian"))
                    .sort(|e| [e.field("rating").desc()])
                    .into_pipeline(),
            ),
        ];

        for (fixture, case, model) in captured {
            assert_eq!(
                Pipeline::try_from(model).unwrap(),
                pipeline(&fixture_case(fixture, case)),
                "{fixture} {case:?}"
            );
        }
    }

    #[test]
    fn pipelines_that_cannot_be_sent_are_rejected_before_sending() {
        let rejected = [
            builder().literals([1]).into_pipeline(),
            builder()
                .from("pipeline-books")
                .fields(|e| [e.raw("to_upper", [e.field("title")])])
                .into_pipeline(),
            builder()
                .from("pipeline-books")
                .fields(|e| {
                    [
                        FirestoreSelection::from(e.field("title")),
                        e.field("author").alias("title"),
                    ]
                })
                .into_pipeline(),
            builder()
                .from("reviews")
                .parent("pipeline-restaurants/rest-01")
                .into_pipeline(),
            builder()
                .from("pipeline-restaurants/rest-01/reviews")
                .into_pipeline(),
        ];

        for model in rejected {
            let result = Pipeline::try_from(model.clone());
            assert!(
                matches!(result, Err(FirestoreError::InvalidParametersError(_))),
                "{model:?}: {result:?}"
            );
        }
    }
}
