use crate::cache::cache_filter_engine::{is_nan, value_order, CompareOp, ValueTypeRank};
use crate::errors::{FirestoreCacheError, FirestoreErrorPublicGenericDetails};
use crate::*;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::Value;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// A pipeline the cache answers itself, with Firestore's semantics.
///
/// [`new`](Self::new) accepts only the sources, stages and functions the cache evaluates as
/// Firestore does, and refuses everything else, so that a pipeline is either answered exactly or
/// left to Firestore. A value that Firestore fails the request on, or that the cache does not
/// order, refuses the pipeline when the evaluation meets it.
pub(crate) struct FirestoreCachePipeline<'a> {
    source: CachedSource,
    stages: Vec<CachedStage<'a>>,
}

#[derive(Debug)]
pub(crate) enum FirestoreCachePipelineMiss {
    Unsupported(String),
    IncompleteCollection(String),
}

pub(crate) enum FirestoreCachePipelineRows {
    All(Vec<FirestoreDocument>),
    /// The named documents the cache holds; the cache cannot tell whether it lacks the others or
    /// they do not exist.
    Held(Vec<FirestoreDocument>),
}

enum CachedSource {
    Collection(CachedCollectionSource),
    Documents(Vec<String>),
}

struct CachedCollectionSource {
    path: String,
    /// The full paths that the pipeline's first `where` pins `__name__` to, which the cache reads
    /// by ID instead of scanning the collection.
    pinned_names: Option<Vec<String>>,
}

enum CachedStage<'a> {
    Where(CachedCondition<'a>),
    Sort(Vec<CachedOrdering<'a>>),
    Limit(usize),
    Offset(usize),
    Fields(CachedFields<'a>),
    AddFields(Vec<CachedSelection<'a>>),
    RemoveFields(Vec<&'a str>),
}

struct CachedOrdering<'a> {
    operand: CachedOperand<'a>,
    direction: &'a FirestoreQueryDirection,
}

struct CachedFields<'a> {
    keeps_name: bool,
    selections: Vec<CachedSelection<'a>>,
}

struct CachedSelection<'a> {
    name: &'a str,
    operand: CachedOperand<'a>,
}

enum CachedOperand<'a> {
    Name,
    Field(&'a str),
    Constant(&'a ValueType),
}

/// An expression that is true, false or null for a document. Every function the cache evaluates
/// is one; an operand is one when it is used where a condition is expected.
enum CachedCondition<'a> {
    Not(Box<CachedCondition<'a>>),
    Logical(CachedLogical<'a>),
    Comparison(CompareOp, Box<[CachedOperand<'a>; 2]>),
    EqualAny {
        negated: bool,
        searched: CachedOperand<'a>,
        search_space: CachedOperand<'a>,
    },
    ArrayContains(CachedArrayContains, Box<[CachedOperand<'a>; 2]>),
    Exists(CachedOperand<'a>),
    IsAbsent(CachedOperand<'a>),
    Operand(CachedOperand<'a>),
}

/// `and` or `or`, which evaluate their conditions in order and only as far as the result is
/// decided, so that a later condition that would fail is never reached.
struct CachedLogical<'a> {
    operator: CachedLogicalOperator,
    conditions: Vec<CachedCondition<'a>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CachedLogicalOperator {
    And,
    Or,
}

#[derive(Clone, Copy)]
enum CachedArrayContains {
    Element,
    All,
    Any,
}

/// A value of an operand over one document: `None` for an absent field.
type Evaluated<'d> = Option<Cow<'d, ValueType>>;

const DOCUMENT_NAME_FIELD: &str = "__name__";

const NULL: ValueType = ValueType::NullValue(0);

impl FirestoreCachePipelineMiss {
    pub(crate) fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported(reason.into())
    }
}

impl Display for FirestoreCachePipelineMiss {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(reason) => f.write_str(reason),
            Self::IncompleteCollection(collection_path) => write!(
                f,
                "the cache does not hold every document of `{collection_path}`"
            ),
        }
    }
}

/// A pipeline that does not convert to a request is left to Firestore, which reports the error.
impl From<FirestoreError> for FirestoreCachePipelineMiss {
    fn from(error: FirestoreError) -> Self {
        Self::Unsupported(error.to_string())
    }
}

impl From<FirestoreCachePipelineMiss> for FirestoreError {
    fn from(miss: FirestoreCachePipelineMiss) -> Self {
        match miss {
            FirestoreCachePipelineMiss::Unsupported(reason) => {
                FirestoreError::CacheError(FirestoreCacheError::new(
                    FirestoreErrorPublicGenericDetails::new("CachePipelineUnsupported".into()),
                    format!(
                        "The cache cannot answer this pipeline: {reason}. Use \
                         `db.read_through_cache(&cache)` to run it on Firestore."
                    ),
                ))
            }
            FirestoreCachePipelineMiss::IncompleteCollection(collection_path) => {
                crate::cache_incomplete_collection_error(
                    &collection_path,
                    "the collection is not preloaded, or is being replayed",
                )
            }
        }
    }
}

impl<'a> FirestoreCachePipeline<'a> {
    /// `pipeline` is one that converts to a request, so its names and aliases are valid.
    pub(crate) fn new(
        pipeline: &'a FirestorePipeline,
        documents_path: &str,
    ) -> Result<Self, FirestoreCachePipelineMiss> {
        let source = match &pipeline.source {
            FirestorePipelineSource::Collection(source) => CachedSource::Collection(
                CachedCollectionSource::new(source, documents_path, pipeline.stages.first())?,
            ),
            FirestorePipelineSource::Documents(source) => {
                CachedSource::documents(source, documents_path)?
            }
            FirestorePipelineSource::CollectionGroup(_) => {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "a collection group spans collections the cache does not hold",
                ))
            }
            FirestorePipelineSource::Database
            | FirestorePipelineSource::Literals(_)
            | FirestorePipelineSource::Subcollection(_) => {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "the source is not a collection or documents",
                ))
            }
        };
        let mut sorted = false;
        let stages = pipeline
            .stages
            .iter()
            .map(|stage| {
                match stage {
                    FirestorePipelineStage::Sort(_) => sorted = true,
                    // Firestore returns unsorted documents in an order of its own.
                    FirestorePipelineStage::Limit(_) | FirestorePipelineStage::Offset(_)
                        if !sorted =>
                    {
                        return Err(FirestoreCachePipelineMiss::unsupported(
                            "a limit or offset that no sort comes before",
                        ))
                    }
                    _ => {}
                }
                CachedStage::new(stage)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { source, stages })
    }

    pub(crate) async fn read(
        &self,
        backend: &(dyn FirestoreCacheBackend + Send + Sync),
    ) -> FirestoreResult<Result<FirestoreCachePipelineRows, FirestoreCachePipelineMiss>> {
        match &self.source {
            CachedSource::Collection(source) => source.read(backend).await,
            CachedSource::Documents(paths) => {
                let held = by_name(held_documents(backend, paths).await?);
                Ok(Ok(if held.len() == paths.len() {
                    FirestoreCachePipelineRows::All(held)
                } else {
                    FirestoreCachePipelineRows::Held(held)
                }))
            }
        }
    }

    pub(crate) fn evaluate(
        &self,
        documents: Vec<FirestoreDocument>,
    ) -> Result<Vec<FirestoreDocument>, FirestoreCachePipelineMiss> {
        self.stages
            .iter()
            .try_fold(documents, |documents, stage| stage.apply(documents))
    }
}

impl CachedSource {
    fn documents(
        source: &FirestoreDocumentsSource,
        documents_path: &str,
    ) -> Result<Self, FirestoreCachePipelineMiss> {
        let mut paths: Vec<String> = Vec::with_capacity(source.paths.len());
        for path in &source.paths {
            let segments: Vec<&str> = path.split('/').collect();
            if !segments.len().is_multiple_of(2)
                || segments.iter().any(|segment| segment.is_empty())
            {
                return Err(FirestoreCachePipelineMiss::unsupported(format!(
                    "`{path}` is not a document path"
                )));
            }
            let full_path = format!("{documents_path}/{path}");
            if paths.contains(&full_path) {
                return Err(FirestoreCachePipelineMiss::unsupported(format!(
                    "`{path}` is named twice"
                )));
            }
            paths.push(full_path);
        }
        Ok(Self::Documents(paths))
    }
}

impl CachedCollectionSource {
    fn new(
        source: &FirestoreCollectionSource,
        documents_path: &str,
        first_stage: Option<&FirestorePipelineStage>,
    ) -> Result<Self, FirestoreCachePipelineMiss> {
        let FirestoreCollectionSource {
            collection_id: _,
            parent: _,
            force_index,
            search,
        } = source;
        if force_index.is_some() {
            return Err(FirestoreCachePipelineMiss::unsupported(
                "an index hint fails on Firestore when the index does not exist",
            ));
        }
        if search.is_some() {
            return Err(FirestoreCachePipelineMiss::unsupported(
                "a search needs a search index",
            ));
        }
        // The pipeline reads the collection of the database it runs on, whichever project and
        // database the parent names.
        let path = format!("{documents_path}/{}", source.path_in_database()?);
        let pinned_names = match first_stage {
            Some(FirestorePipelineStage::Where(stage)) => {
                Self::pinned_names(&stage.condition.0).map(|names| {
                    let mut in_collection: Vec<String> = Vec::with_capacity(names.len());
                    for name in names {
                        // A name in another collection matches no document of this one.
                        if crate::db::split_document_path(name).0 == path
                            && !in_collection.iter().any(|kept| kept == name)
                        {
                            in_collection.push(name.to_string());
                        }
                    }
                    in_collection
                })
            }
            _ => None,
        };
        Ok(Self { path, pinned_names })
    }

    async fn read(
        &self,
        backend: &(dyn FirestoreCacheBackend + Send + Sync),
    ) -> FirestoreResult<Result<FirestoreCachePipelineRows, FirestoreCachePipelineMiss>> {
        let held = match &self.pinned_names {
            Some(names) => {
                let held = held_documents(backend, names).await?;
                if held.len() == names.len() {
                    return Ok(Ok(FirestoreCachePipelineRows::All(by_name(held))));
                }
                Some(held)
            }
            None => None,
        };
        // A named document the cache lacks is known to be absent only when the cache holds the
        // whole collection.
        Ok(match (backend.list_all_docs(&self.path).await?, held) {
            (FirestoreCachedValue::UseCached(documents), _) => Ok(FirestoreCachePipelineRows::All(
                by_name(documents.try_collect().await?),
            )),
            (FirestoreCachedValue::SkipCache, Some(held)) => {
                Ok(FirestoreCachePipelineRows::Held(by_name(held)))
            }
            (FirestoreCachedValue::SkipCache, None) => Err(
                FirestoreCachePipelineMiss::IncompleteCollection(self.path.clone()),
            ),
        })
    }

    /// The names a condition requires `__name__` to be one of: an `equal` or `equal_any` of
    /// `__name__` with reference constants, alone or in a top-level `and`.
    fn pinned_names(condition: &FirestoreExpr) -> Option<Vec<&str>> {
        fn reference_name(value: &Value) -> Option<&str> {
            match &value.value_type {
                Some(ValueType::ReferenceValue(name)) => Some(name),
                _ => None,
            }
        }
        let FirestoreExpr::Function(function) = condition else {
            return None;
        };
        match (function.name.as_str(), function.args.as_slice()) {
            ("equal", [FirestoreExpr::Field(field), FirestoreExpr::Constant(constant)])
                if field == DOCUMENT_NAME_FIELD =>
            {
                reference_name(&constant.value).map(|name| vec![name])
            }
            ("equal_any", [FirestoreExpr::Field(field), FirestoreExpr::Constant(constant)])
                if field == DOCUMENT_NAME_FIELD =>
            {
                match &constant.value.value_type {
                    Some(ValueType::ArrayValue(array)) => {
                        array.values.iter().map(reference_name).collect()
                    }
                    _ => None,
                }
            }
            ("and", conditions) => conditions.iter().find_map(Self::pinned_names),
            _ => None,
        }
    }
}

async fn held_documents(
    backend: &(dyn FirestoreCacheBackend + Send + Sync),
    paths: &[String],
) -> FirestoreResult<Vec<FirestoreDocument>> {
    backend
        .get_docs_by_paths(paths)
        .await?
        .try_filter_map(|(_, document)| futures::future::ready(Ok(document)))
        .try_collect()
        .await
}

/// Documents in the order Firestore returns them from a source without a sort: by name.
fn by_name(mut documents: Vec<FirestoreDocument>) -> Vec<FirestoreDocument> {
    documents.sort_by(|left, right| left.name.split('/').cmp(right.name.split('/')));
    documents
}

impl<'a> CachedStage<'a> {
    fn new(stage: &'a FirestorePipelineStage) -> Result<Self, FirestoreCachePipelineMiss> {
        Ok(match stage {
            FirestorePipelineStage::Where(stage) => {
                CachedStage::Where(CachedCondition::new(&stage.condition.0)?)
            }
            FirestorePipelineStage::Sort(stage) => CachedStage::Sort(
                stage
                    .orderings
                    .iter()
                    .map(|ordering| {
                        Ok(CachedOrdering {
                            operand: CachedOperand::new(&ordering.expr)?,
                            direction: &ordering.direction,
                        })
                    })
                    .collect::<Result<_, FirestoreCachePipelineMiss>>()?,
            ),
            FirestorePipelineStage::Limit(stage) => {
                CachedStage::Limit(usize::try_from(stage.limit).unwrap_or(usize::MAX))
            }
            FirestorePipelineStage::Offset(stage) => {
                CachedStage::Offset(usize::try_from(stage.offset).unwrap_or(usize::MAX))
            }
            FirestorePipelineStage::Fields(stage) => {
                let mut keeps_name = false;
                let mut selections = Vec::with_capacity(stage.fields.len());
                for selection in &stage.fields {
                    match CachedSelection::new(selection, "fields")? {
                        CachedSelection {
                            name: DOCUMENT_NAME_FIELD,
                            ..
                        } => keeps_name = true,
                        selection => selections.push(selection),
                    }
                }
                CachedStage::Fields(CachedFields {
                    keeps_name,
                    selections,
                })
            }
            FirestorePipelineStage::AddFields(stage) => CachedStage::AddFields(
                stage
                    .fields
                    .iter()
                    .map(
                        |selection| match CachedSelection::new(selection, "add_fields")? {
                            CachedSelection {
                                name: DOCUMENT_NAME_FIELD,
                                ..
                            } => Err(FirestoreCachePipelineMiss::unsupported(
                                "add_fields of the document name",
                            )),
                            selection => Ok(selection),
                        },
                    )
                    .collect::<Result<_, _>>()?,
            ),
            FirestorePipelineStage::RemoveFields(stage) => CachedStage::RemoveFields(
                stage
                    .fields
                    .iter()
                    .map(|path| output_field_name(path))
                    .collect::<Result<_, _>>()?,
            ),
            FirestorePipelineStage::Raw(_)
            | FirestorePipelineStage::Aggregate(_)
            | FirestorePipelineStage::Distinct(_)
            | FirestorePipelineStage::FindNearest(_)
            | FirestorePipelineStage::Sample(_)
            | FirestorePipelineStage::Union(_)
            | FirestorePipelineStage::Unnest(_)
            | FirestorePipelineStage::ReplaceWith(_)
            | FirestorePipelineStage::DefineVariables(_) => {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "the cache evaluates only where, sort, limit, offset, fields, add_fields and \
                     remove_fields",
                ))
            }
        })
    }

    fn apply(
        &self,
        mut documents: Vec<FirestoreDocument>,
    ) -> Result<Vec<FirestoreDocument>, FirestoreCachePipelineMiss> {
        match self {
            CachedStage::Where(condition) => {
                let mut kept = Vec::with_capacity(documents.len());
                for document in documents {
                    if condition.holds(&document)? {
                        kept.push(document);
                    }
                }
                Ok(kept)
            }
            CachedStage::Sort(orderings) => {
                let order = Self::sorted_order(orderings, &documents)?;
                let mut slots: Vec<Option<FirestoreDocument>> =
                    documents.into_iter().map(Some).collect();
                Ok(order
                    .into_iter()
                    .filter_map(|index| slots[index].take())
                    .collect())
            }
            CachedStage::Limit(limit) => {
                documents.truncate(*limit);
                Ok(documents)
            }
            CachedStage::Offset(offset) => {
                documents.drain(..(*offset).min(documents.len()));
                Ok(documents)
            }
            CachedStage::Fields(fields) => Ok(documents
                .iter()
                .map(|document| FirestoreDocument {
                    name: if fields.keeps_name {
                        document.name.clone()
                    } else {
                        String::new()
                    },
                    fields: CachedSelection::evaluate_all(&fields.selections, document)
                        .into_iter()
                        .filter_map(|(name, value)| value.map(|value| (name.to_string(), value)))
                        .collect(),
                    create_time: None,
                    update_time: None,
                })
                .collect()),
            CachedStage::AddFields(selections) => Ok(documents
                .into_iter()
                .map(|mut document| {
                    for (name, value) in CachedSelection::evaluate_all(selections, &document) {
                        match value {
                            Some(value) => document.fields.insert(name.to_string(), value),
                            None => document.fields.remove(name),
                        };
                    }
                    document
                })
                .collect()),
            CachedStage::RemoveFields(names) => Ok(documents
                .into_iter()
                .map(|mut document| {
                    for name in names {
                        document.fields.remove(*name);
                    }
                    document
                })
                .collect()),
        }
    }

    /// The indexes of `documents` in sort order. The sort is stable, so that documents with equal
    /// keys keep the order the source read them in.
    fn sorted_order(
        orderings: &[CachedOrdering<'_>],
        documents: &[FirestoreDocument],
    ) -> Result<Vec<usize>, FirestoreCachePipelineMiss> {
        let keys = documents
            .iter()
            .map(|document| {
                orderings
                    .iter()
                    .map(|ordering| ordering.operand.evaluate(document))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for key in keys.iter().flatten().flatten() {
            if is_unordered(key) {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "a sort by an array, map, geo point or vector",
                ));
            }
        }
        let mut order: Vec<usize> = (0..documents.len()).collect();
        order.sort_by(|&left, &right| {
            orderings
                .iter()
                .zip(keys[left].iter().zip(&keys[right]))
                .map(|(ordering, (left, right))| {
                    let (left, right) = (
                        left.as_deref().unwrap_or(&NULL),
                        right.as_deref().unwrap_or(&NULL),
                    );
                    match ordering.direction {
                        FirestoreQueryDirection::Ascending => stored_order(left, right),
                        FirestoreQueryDirection::Descending => stored_order(right, left),
                    }
                })
                .find(|ordering| ordering.is_ne())
                .unwrap_or(Ordering::Equal)
        });
        Ok(order)
    }
}

impl<'a> CachedSelection<'a> {
    fn new(
        selection: &'a FirestoreSelection,
        stage: &str,
    ) -> Result<Self, FirestoreCachePipelineMiss> {
        let name = selection.name(stage)?;
        let operand = CachedOperand::new(&selection.expr)?;
        match (name, &operand) {
            (DOCUMENT_NAME_FIELD, CachedOperand::Name) => Ok(Self { name, operand }),
            (name, CachedOperand::Name | CachedOperand::Field(_)) => Ok(Self {
                name: output_field_name(name)?,
                operand,
            }),
            (_, CachedOperand::Constant(_)) => Err(FirestoreCachePipelineMiss::unsupported(
                format!("a constant in {stage}"),
            )),
        }
    }

    fn evaluate_all(
        selections: &[Self],
        document: &FirestoreDocument,
    ) -> Vec<(&'a str, Option<Value>)> {
        selections
            .iter()
            .map(|selection| {
                (
                    selection.name,
                    selection.operand.evaluate(document).map(|value| Value {
                        value_type: Some(value.into_owned()),
                    }),
                )
            })
            .collect()
    }
}

fn output_field_name(name: &str) -> Result<&str, FirestoreCachePipelineMiss> {
    if !CachedOperand::is_plain_segment(name) {
        return Err(FirestoreCachePipelineMiss::unsupported(format!(
            "`{name}` is not a top-level field name"
        )));
    }
    Ok(name)
}

impl<'a> CachedOperand<'a> {
    fn new(expr: &'a FirestoreExpr) -> Result<Self, FirestoreCachePipelineMiss> {
        match expr {
            FirestoreExpr::Field(path) if path == DOCUMENT_NAME_FIELD => Ok(Self::Name),
            FirestoreExpr::Field(path) => {
                // Firestore fails on an unquoted segment that is not a simple identifier, and
                // reads a quoted one or a reserved name such as `__create_time__` by rules of its
                // own.
                if !path.split('.').all(Self::is_plain_segment) {
                    return Err(FirestoreCachePipelineMiss::unsupported(format!(
                        "the field path `{path}`"
                    )));
                }
                Ok(Self::Field(path))
            }
            FirestoreExpr::Constant(constant) => match &constant.value.value_type {
                Some(value) if is_data(value) => Ok(Self::Constant(value)),
                _ => Err(FirestoreCachePipelineMiss::unsupported(
                    "a constant that holds an expression",
                )),
            },
            FirestoreExpr::Function(_) => Err(FirestoreCachePipelineMiss::unsupported(
                "a function where a field or constant is expected",
            )),
            FirestoreExpr::Variable(_)
            | FirestoreExpr::ArraySubquery(_)
            | FirestoreExpr::ScalarSubquery(_) => Err(FirestoreCachePipelineMiss::unsupported(
                "variables and subqueries",
            )),
        }
    }

    fn is_plain_segment(segment: &str) -> bool {
        is_simple_field_segment(segment) && !segment.starts_with("__")
    }

    fn evaluate<'d>(&'d self, document: &'d FirestoreDocument) -> Evaluated<'d> {
        match self {
            Self::Name => (!document.name.is_empty())
                .then(|| Cow::Owned(ValueType::ReferenceValue(document.name.clone()))),
            Self::Field(path) => firestore_doc_get_field_by_path(document, path).map(Cow::Borrowed),
            Self::Constant(value) => Some(Cow::Borrowed(*value)),
        }
    }
}

/// Whether `value` is data, rather than an expression a constant could smuggle in.
fn is_data(value: &ValueType) -> bool {
    let is_data_value = |value: &Value| value.value_type.as_ref().is_some_and(is_data);
    match value {
        ValueType::ArrayValue(array) => array.values.iter().all(is_data_value),
        ValueType::MapValue(map) => map.fields.values().all(is_data_value),
        ValueType::FieldReferenceValue(_)
        | ValueType::VariableReferenceValue(_)
        | ValueType::FunctionValue(_)
        | ValueType::PipelineValue(_) => false,
        ValueType::NullValue(_)
        | ValueType::BooleanValue(_)
        | ValueType::IntegerValue(_)
        | ValueType::DoubleValue(_)
        | ValueType::TimestampValue(_)
        | ValueType::StringValue(_)
        | ValueType::BytesValue(_)
        | ValueType::ReferenceValue(_)
        | ValueType::GeoPointValue(_) => true,
    }
}

impl<'a> CachedCondition<'a> {
    fn new(expr: &'a FirestoreExpr) -> Result<Self, FirestoreCachePipelineMiss> {
        let FirestoreExpr::Function(function) = expr else {
            return Ok(Self::Operand(CachedOperand::new(expr)?));
        };
        let comparison = |op| match function.args.as_slice() {
            [left, right] => Ok(Self::Comparison(
                op,
                Box::new([CachedOperand::new(left)?, CachedOperand::new(right)?]),
            )),
            _ => Err(Self::unknown_function(function)),
        };
        let membership = |negated| match function.args.as_slice() {
            [searched, search_space] => Ok(Self::EqualAny {
                negated,
                searched: CachedOperand::new(searched)?,
                search_space: CachedOperand::new(search_space)?,
            }),
            _ => Err(Self::unknown_function(function)),
        };
        let array_contains = |kind| match function.args.as_slice() {
            [array, searched] => Ok(Self::ArrayContains(
                kind,
                Box::new([CachedOperand::new(array)?, CachedOperand::new(searched)?]),
            )),
            _ => Err(Self::unknown_function(function)),
        };
        let logical = |operator| match function.args.as_slice() {
            [_, _, ..] => Ok(Self::Logical(CachedLogical {
                operator,
                conditions: function
                    .args
                    .iter()
                    .map(Self::new)
                    .collect::<Result<_, _>>()?,
            })),
            _ => Err(Self::unknown_function(function)),
        };
        match (function.name.as_str(), function.args.as_slice()) {
            ("equal", _) => comparison(CompareOp::Equal),
            ("not_equal", _) => comparison(CompareOp::NotEqual),
            ("less_than", _) => comparison(CompareOp::LessThan),
            ("less_than_or_equal", _) => comparison(CompareOp::LessThanOrEqual),
            ("greater_than", _) => comparison(CompareOp::GreaterThan),
            ("greater_than_or_equal", _) => comparison(CompareOp::GreaterThanOrEqual),
            ("equal_any", _) => membership(false),
            ("not_equal_any", _) => membership(true),
            ("array_contains", _) => array_contains(CachedArrayContains::Element),
            ("array_contains_all", _) => array_contains(CachedArrayContains::All),
            ("array_contains_any", _) => array_contains(CachedArrayContains::Any),
            ("and", _) => logical(CachedLogicalOperator::And),
            ("or", _) => logical(CachedLogicalOperator::Or),
            ("not", [condition]) => Ok(Self::Not(Box::new(Self::new(condition)?))),
            // Firestore accepts only a field here.
            ("exists", [field @ FirestoreExpr::Field(_)]) => {
                Ok(Self::Exists(CachedOperand::new(field)?))
            }
            ("is_absent", [field @ FirestoreExpr::Field(_)]) => {
                Ok(Self::IsAbsent(CachedOperand::new(field)?))
            }
            _ => Err(Self::unknown_function(function)),
        }
    }

    fn unknown_function(function: &FirestoreFunction) -> FirestoreCachePipelineMiss {
        FirestoreCachePipelineMiss::unsupported(format!(
            "the function `{}` of {} arguments",
            function.name,
            function.args.len()
        ))
    }

    /// Whether `where` keeps `document`: the condition is true. A top-level `and` keeps it when
    /// each of its conditions is true, as separate filters do, so that an operand that is not a
    /// boolean drops the document instead of failing.
    fn holds(&self, document: &FirestoreDocument) -> Result<bool, FirestoreCachePipelineMiss> {
        let is_true = |condition: &Self| match condition {
            Self::Operand(operand) => Ok(matches!(
                operand.evaluate(document).as_deref(),
                Some(ValueType::BooleanValue(true))
            )),
            condition => Ok(condition.evaluate(document)? == Some(true)),
        };
        match self {
            Self::Logical(CachedLogical {
                operator: CachedLogicalOperator::And,
                conditions,
            }) => {
                for condition in conditions {
                    if !is_true(condition)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            condition => is_true(condition),
        }
    }

    /// `None` is null.
    fn evaluate(
        &self,
        document: &FirestoreDocument,
    ) -> Result<Option<bool>, FirestoreCachePipelineMiss> {
        match self {
            Self::Not(condition) => Ok(condition.evaluate(document)?.map(|truth| !truth)),
            Self::Logical(logical) => logical.evaluate(document),
            Self::Comparison(op, operands) => {
                let [left, right] = &**operands;
                Ok(Some(Self::comparison_holds(
                    *op,
                    left.evaluate(document).as_deref(),
                    right.evaluate(document).as_deref(),
                )?))
            }
            Self::EqualAny {
                negated,
                searched,
                search_space,
            } => Ok(Some(
                Self::is_in(
                    searched.evaluate(document).as_deref(),
                    search_space.evaluate(document).as_deref(),
                )? != *negated,
            )),
            Self::ArrayContains(kind, operands) => {
                let [array, searched] = &**operands;
                kind.found(
                    array.evaluate(document).as_deref(),
                    searched.evaluate(document).as_deref(),
                )
            }
            Self::Exists(operand) => Ok(Some(operand.evaluate(document).is_some())),
            Self::IsAbsent(operand) => Ok(Some(operand.evaluate(document).is_none())),
            Self::Operand(operand) => truth(operand.evaluate(document).as_deref()),
        }
    }

    /// Absent equals absent and nothing else, null equals null, and values of different types are
    /// unequal and incomparable.
    fn comparison_holds(
        op: CompareOp,
        left: Option<&ValueType>,
        right: Option<&ValueType>,
    ) -> Result<bool, FirestoreCachePipelineMiss> {
        let ordering = match (left, right) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(_)) | (Some(_), None) => None,
            (Some(left), Some(right)) if op.is_range() => ordered(left, right)?,
            (Some(left), Some(right)) => equal(left, right).then_some(Ordering::Equal),
        };
        Ok(match ordering {
            Some(ordering) => op.holds_for(ordering),
            None => matches!(op, CompareOp::NotEqual),
        })
    }

    fn is_in(
        searched: Option<&ValueType>,
        search_space: Option<&ValueType>,
    ) -> Result<bool, FirestoreCachePipelineMiss> {
        let Some(ValueType::ArrayValue(search_space)) = search_space else {
            return Err(FirestoreCachePipelineMiss::unsupported(
                "equal_any of a search space that is not an array",
            ));
        };
        match searched {
            Some(searched) => contains(&search_space.values, searched),
            None => Ok(false),
        }
    }
}

impl CachedArrayContains {
    /// False for a value that is not an array, and `None`, which is null, for a null or absent
    /// one.
    fn found(
        self,
        array: Option<&ValueType>,
        searched: Option<&ValueType>,
    ) -> Result<Option<bool>, FirestoreCachePipelineMiss> {
        let array = match array {
            Some(ValueType::ArrayValue(array)) => &array.values,
            Some(ValueType::NullValue(_)) | None => return Ok(None),
            Some(_) => return Ok(Some(false)),
        };
        let found =
            match (self, searched) {
                (Self::Element, Some(searched)) => contains(array, searched)?,
                (Self::All, Some(ValueType::ArrayValue(searched))) => {
                    let mut all = true;
                    for searched in &searched.values {
                        all &= contains(array, data(searched)?)?;
                    }
                    all
                }
                (Self::Any, Some(ValueType::ArrayValue(searched))) => {
                    let mut any = false;
                    for searched in &searched.values {
                        any |= contains(array, data(searched)?)?;
                    }
                    any
                }
                _ => return Err(FirestoreCachePipelineMiss::unsupported(
                    "array_contains of an absent value, or of search values that are not an array",
                )),
            };
        Ok(Some(found))
    }
}

impl CachedLogical<'_> {
    /// True or false when a condition decides it, and otherwise null when a condition is null or
    /// absent: `and` of true and null is null, `and` of false and null is false.
    fn evaluate(
        &self,
        document: &FirestoreDocument,
    ) -> Result<Option<bool>, FirestoreCachePipelineMiss> {
        let decisive = self.operator == CachedLogicalOperator::Or;
        let mut undetermined = false;
        for condition in &self.conditions {
            match condition.evaluate(document)? {
                Some(truth) if truth == decisive => return Ok(Some(truth)),
                Some(_) => {}
                None => undetermined = true,
            }
        }
        Ok((!undetermined).then_some(!decisive))
    }
}

/// The truth of an operand used as a condition: a null or absent one is `None`, which is null,
/// and Firestore fails the request on a value that is not a boolean.
fn truth(value: Option<&ValueType>) -> Result<Option<bool>, FirestoreCachePipelineMiss> {
    match value {
        Some(ValueType::BooleanValue(flag)) => Ok(Some(*flag)),
        Some(ValueType::NullValue(_)) | None => Ok(None),
        Some(_) => Err(FirestoreCachePipelineMiss::unsupported(
            "a value that is not a boolean",
        )),
    }
}

/// A timestamp as Firestore stores and compares it, as a count of microseconds.
fn microseconds(timestamp: &gcloud_sdk::prost_types::Timestamp) -> i64 {
    timestamp
        .seconds
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(timestamp.nanos / 1_000))
}

/// The order of two values as stored: Firestore's, with timestamps compared at the microsecond
/// that Firestore stores them at.
fn stored_order(left: &ValueType, right: &ValueType) -> Ordering {
    match (left, right) {
        (ValueType::TimestampValue(left), ValueType::TimestampValue(right)) => {
            microseconds(left).cmp(&microseconds(right))
        }
        _ => value_order(left, right),
    }
}

/// The order of two values of one type, or `None` for values of different types and for NaN
/// against another number, which Firestore finds incomparable. NaN equals NaN.
fn comparable_order(left: &ValueType, right: &ValueType) -> Option<Ordering> {
    if ValueTypeRank::from(left) != ValueTypeRank::from(right) {
        return None;
    }
    match (is_nan(left), is_nan(right)) {
        (true, true) => Some(Ordering::Equal),
        (false, false) => Some(stored_order(left, right)),
        _ => None,
    }
}

/// Whether the cache leaves ordering values of this type to Firestore: no differential case pins
/// that the order of unequal arrays, maps, geo points and vectors matches Firestore's.
fn is_unordered(value: &ValueType) -> bool {
    matches!(
        ValueTypeRank::from(value),
        ValueTypeRank::GeoPoint | ValueTypeRank::Array | ValueTypeRank::Vector | ValueTypeRank::Map
    )
}

/// Firestore's `equal` of two present values: numbers by value, NaN equal to NaN, null equal to
/// null, and values of different types unequal.
fn equal(left: &ValueType, right: &ValueType) -> bool {
    comparable_order(left, right) == Some(Ordering::Equal)
}

/// How two present values order for the ordering comparisons, or `None` when Firestore finds
/// them incomparable. Arrays, maps and geo points are ordered only when equal, because the order
/// Firestore gives unequal ones is not known.
fn ordered(
    left: &ValueType,
    right: &ValueType,
) -> Result<Option<Ordering>, FirestoreCachePipelineMiss> {
    let order = comparable_order(left, right);
    if order.is_some_and(Ordering::is_ne) && is_unordered(left) {
        return Err(FirestoreCachePipelineMiss::unsupported(
            "an ordering of unequal arrays, maps or geo points",
        ));
    }
    Ok(order)
}

fn data(value: &Value) -> Result<&ValueType, FirestoreCachePipelineMiss> {
    value
        .value_type
        .as_ref()
        .ok_or_else(|| FirestoreCachePipelineMiss::unsupported("a value without a type"))
}

fn contains(values: &[Value], searched: &ValueType) -> Result<bool, FirestoreCachePipelineMiss> {
    for value in values {
        if equal(data(value)?, searched) {
            return Ok(true);
        }
    }
    Ok(false)
}
