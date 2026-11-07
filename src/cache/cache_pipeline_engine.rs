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
    /// The pipeline, or a value it meets, needs Firestore to evaluate it.
    Unsupported(String),
    /// The path of a collection the cache does not hold every document of.
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
    Where(CachedExpr<'a>),
    Sort(Vec<CachedOrdering<'a>>),
    Limit(usize),
    Offset(usize),
    Fields(CachedFields<'a>),
    AddFields(Vec<CachedSelection<'a>>),
    RemoveFields(Vec<&'a str>),
}

struct CachedOrdering<'a> {
    expr: CachedExpr<'a>,
    direction: &'a FirestoreQueryDirection,
}

struct CachedFields<'a> {
    /// Whether `__name__` is selected, which keeps the document's name in the result.
    keeps_name: bool,
    selections: Vec<CachedSelection<'a>>,
}

struct CachedSelection<'a> {
    name: &'a str,
    expr: CachedExpr<'a>,
}

enum CachedExpr<'a> {
    /// `__name__`: the document's name, as a reference.
    Name,
    Field(&'a str),
    Constant(&'a ValueType),
    Call(CachedCall<'a>),
    Logical(CachedLogical<'a>),
}

/// A call of a function, shaped by the number of arguments the function takes.
enum CachedCall<'a> {
    Unary(CachedUnary, Box<CachedExpr<'a>>),
    Binary(CachedBinary, Box<[CachedExpr<'a>; 2]>),
    StringConcat(Vec<CachedExpr<'a>>),
}

/// `and` or `or`, which evaluate their conditions in order and only as far as the result is
/// decided, so that a later condition that would fail is never reached.
struct CachedLogical<'a> {
    operator: CachedLogicalOperator,
    conditions: Vec<CachedExpr<'a>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CachedLogicalOperator {
    And,
    Or,
}

#[derive(Clone, Copy)]
enum CachedUnary {
    Not,
    Exists,
    IsAbsent,
    ToLower,
    ToUpper,
    CharLength,
}

#[derive(Clone, Copy)]
enum CachedBinary {
    Comparison(CachedComparison),
    EqualAny,
    NotEqualAny,
    ArrayContains(CachedArrayContains),
    Arithmetic(CachedArithmetic),
    TextTest(CachedTextTest),
}

#[derive(Clone, Copy)]
enum CachedComparison {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Copy)]
enum CachedArrayContains {
    Element,
    All,
    Any,
}

#[derive(Clone, Copy)]
enum CachedArithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
    Mod,
}

#[derive(Clone, Copy)]
enum CachedTextTest {
    StartsWith,
    EndsWith,
    Contains,
}

/// A value of an expression over one document: `None` for an absent field.
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

/// The error of a `read_cached_only` session for a pipeline the cache does not answer.
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
    /// Compiles `pipeline` for the database whose documents root is `documents_path`, or refuses
    /// it. `pipeline` is one that converts to a request, so its names and aliases are valid.
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
            // The path is built from these two.
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
    documents.sort_by(|left, right| ReferenceName(&left.name).cmp(&ReferenceName(&right.name)));
    documents
}

impl<'a> CachedStage<'a> {
    fn new(stage: &'a FirestorePipelineStage) -> Result<Self, FirestoreCachePipelineMiss> {
        Ok(match stage {
            FirestorePipelineStage::Where(stage) => {
                CachedStage::Where(CachedExpr::new(&stage.condition.0)?)
            }
            FirestorePipelineStage::Sort(stage) => CachedStage::Sort(
                stage
                    .orderings
                    .iter()
                    .map(|ordering| {
                        Ok(CachedOrdering {
                            expr: CachedExpr::new(&ordering.expr)?,
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
            CachedStage::Fields(fields) => documents
                .iter()
                .map(|document| {
                    Ok(FirestoreDocument {
                        name: if fields.keeps_name {
                            document.name.clone()
                        } else {
                            String::new()
                        },
                        fields: CachedSelection::evaluate_all(&fields.selections, document)?
                            .into_iter()
                            .filter_map(|(name, value)| {
                                value.map(|value| (name.to_string(), value))
                            })
                            .collect(),
                        create_time: None,
                        update_time: None,
                    })
                })
                .collect(),
            CachedStage::AddFields(selections) => documents
                .into_iter()
                .map(|mut document| {
                    for (name, value) in CachedSelection::evaluate_all(selections, &document)? {
                        match value {
                            Some(value) => document.fields.insert(name.to_string(), value),
                            None => document.fields.remove(name),
                        };
                    }
                    Ok(document)
                })
                .collect(),
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
        let values = documents
            .iter()
            .map(|document| {
                orderings
                    .iter()
                    .map(|ordering| ordering.expr.evaluate(document))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let keys = values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| CachedSortKey::try_from(value.as_deref()))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut order: Vec<usize> = (0..documents.len()).collect();
        order.sort_by(|&left, &right| {
            orderings
                .iter()
                .zip(keys[left].iter().zip(&keys[right]))
                .map(|(ordering, (left, right))| match ordering.direction {
                    FirestoreQueryDirection::Ascending => left.cmp(right),
                    FirestoreQueryDirection::Descending => right.cmp(left),
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
        let expr = CachedExpr::new(&selection.expr)?;
        match (name, &expr) {
            // `__name__` selects the document's name, not a field.
            (DOCUMENT_NAME_FIELD, CachedExpr::Name) => Ok(Self { name, expr }),
            (name, _) => Ok(Self {
                name: output_field_name(name)?,
                expr,
            }),
        }
    }

    fn evaluate_all(
        selections: &[Self],
        document: &FirestoreDocument,
    ) -> Result<Vec<(&'a str, Option<Value>)>, FirestoreCachePipelineMiss> {
        selections
            .iter()
            .map(|selection| {
                Ok((
                    selection.name,
                    selection.expr.evaluate(document)?.map(|value| Value {
                        value_type: Some(value.into_owned()),
                    }),
                ))
            })
            .collect()
    }
}

/// A top-level field name, the only kind of output name the cache writes.
fn output_field_name(name: &str) -> Result<&str, FirestoreCachePipelineMiss> {
    if name.is_empty() || name.contains('.') || name.contains('`') || name.starts_with("__") {
        return Err(FirestoreCachePipelineMiss::unsupported(format!(
            "`{name}` is not a top-level field name"
        )));
    }
    Ok(name)
}

/// A value as `sort` orders it: absent and null first, then booleans, numbers with NaN first,
/// timestamps, strings, bytes and references. The cache does not order the other types.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum CachedSortKey<'v> {
    Null,
    Boolean(bool),
    Number(CachedNumber),
    Timestamp(i64),
    String(&'v str),
    Bytes(&'v [u8]),
    Reference(ReferenceName<'v>),
}

impl<'v> CachedSortKey<'v> {
    fn new(value: &'v ValueType) -> Option<Self> {
        Some(match value {
            ValueType::NullValue(_) => Self::Null,
            ValueType::BooleanValue(flag) => Self::Boolean(*flag),
            ValueType::TimestampValue(timestamp) => Self::Timestamp(microseconds(timestamp)),
            ValueType::StringValue(text) => Self::String(text),
            ValueType::BytesValue(bytes) => Self::Bytes(bytes),
            ValueType::ReferenceValue(name) => Self::Reference(ReferenceName(name)),
            value => Self::Number(CachedNumber::new(value)?),
        })
    }

    /// How two keys order, or `None` when Firestore finds them incomparable: keys of different
    /// types, or NaN with another number.
    fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Number(left), Self::Number(right)) => left.compare(*right),
            _ if std::mem::discriminant(self) == std::mem::discriminant(other) => {
                Some(self.cmp(other))
            }
            _ => None,
        }
    }
}

impl<'v> TryFrom<Option<&'v ValueType>> for CachedSortKey<'v> {
    type Error = FirestoreCachePipelineMiss;

    /// An absent value sorts as null.
    fn try_from(value: Option<&'v ValueType>) -> Result<Self, Self::Error> {
        match value {
            None => Ok(Self::Null),
            Some(value) => Self::new(value).ok_or_else(|| {
                FirestoreCachePipelineMiss::unsupported(
                    "a sort by an array, map, geo point or vector",
                )
            }),
        }
    }
}

/// A reference, ordered by its path segments.
#[derive(PartialEq, Eq)]
struct ReferenceName<'v>(&'v str);

impl Ord for ReferenceName<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.split('/').cmp(other.0.split('/'))
    }
}

impl PartialOrd for ReferenceName<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
enum CachedNumber {
    Integer(i64),
    Double(f64),
}

impl CachedNumber {
    fn new(value: &ValueType) -> Option<Self> {
        match value {
            ValueType::IntegerValue(number) => Some(Self::Integer(*number)),
            ValueType::DoubleValue(number) => Some(Self::Double(*number)),
            _ => None,
        }
    }

    fn is_nan(self) -> bool {
        matches!(self, Self::Double(number) if number.is_nan())
    }

    /// How two numbers order, exactly and without rounding an integer to a double. NaN equals
    /// NaN and is incomparable with any other number.
    fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => Some(left.cmp(&right)),
            (Self::Double(left), Self::Double(right)) if left.is_nan() && right.is_nan() => {
                Some(Ordering::Equal)
            }
            (Self::Double(left), Self::Double(right)) => left.partial_cmp(&right),
            (Self::Integer(left), Self::Double(right)) => integer_with_double(left, right),
            (Self::Double(left), Self::Integer(right)) => {
                integer_with_double(right, left).map(Ordering::reverse)
            }
        }
    }

    fn as_double(self) -> f64 {
        match self {
            Self::Integer(number) => number as f64,
            Self::Double(number) => number,
        }
    }
}

/// `integer` against `double`, exactly: 2^53 + 1 is greater than the double 2^53.
fn integer_with_double(integer: i64, double: f64) -> Option<Ordering> {
    // 2^63, the first double above every i64.
    const I64_BOUND: f64 = 9_223_372_036_854_775_808.0;
    if double.is_nan() {
        return None;
    }
    if double >= I64_BOUND {
        return Some(Ordering::Less);
    }
    if double < -I64_BOUND {
        return Some(Ordering::Greater);
    }
    let whole = double.trunc();
    // In range, so the cast is exact.
    let whole_integer = whole as i64;
    Some(
        integer
            .cmp(&whole_integer)
            .then_with(|| whole.partial_cmp(&double).unwrap_or(Ordering::Equal)),
    )
}

/// Sort order among numbers: NaN first, equal to itself.
impl Ord for CachedNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_nan(), other.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.compare(*other).unwrap_or(Ordering::Equal),
        }
    }
}

impl PartialOrd for CachedNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CachedNumber {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for CachedNumber {}

impl<'a> CachedExpr<'a> {
    fn new(expr: &'a FirestoreExpr) -> Result<Self, FirestoreCachePipelineMiss> {
        match expr {
            FirestoreExpr::Field(path) if path == DOCUMENT_NAME_FIELD => Ok(CachedExpr::Name),
            FirestoreExpr::Field(path) => {
                // A quoted segment or a reserved name such as `__create_time__` is left to
                // Firestore, which parses field paths itself.
                if path.split('.').any(|segment| {
                    segment.is_empty() || segment.contains('`') || segment.starts_with("__")
                }) {
                    return Err(FirestoreCachePipelineMiss::unsupported(format!(
                        "the field path `{path}`"
                    )));
                }
                Ok(CachedExpr::Field(path))
            }
            FirestoreExpr::Constant(constant) => match &constant.value.value_type {
                Some(value) if is_data(value) => Ok(CachedExpr::Constant(value)),
                _ => Err(FirestoreCachePipelineMiss::unsupported(
                    "a constant that holds an expression",
                )),
            },
            FirestoreExpr::Function(function) => {
                let operator = match (function.name.as_str(), function.args.len()) {
                    ("and", 2..) => CachedLogicalOperator::And,
                    ("or", 2..) => CachedLogicalOperator::Or,
                    _ => return Ok(CachedExpr::Call(CachedCall::new(function)?)),
                };
                Ok(CachedExpr::Logical(CachedLogical {
                    operator,
                    conditions: function
                        .args
                        .iter()
                        .map(CachedExpr::new)
                        .collect::<Result<_, _>>()?,
                }))
            }
            FirestoreExpr::Variable(_)
            | FirestoreExpr::ArraySubquery(_)
            | FirestoreExpr::ScalarSubquery(_) => Err(FirestoreCachePipelineMiss::unsupported(
                "variables and subqueries",
            )),
        }
    }

    fn evaluate<'d>(
        &'d self,
        document: &'d FirestoreDocument,
    ) -> Result<Evaluated<'d>, FirestoreCachePipelineMiss> {
        Ok(match self {
            CachedExpr::Name => (!document.name.is_empty())
                .then(|| Cow::Owned(ValueType::ReferenceValue(document.name.clone()))),
            CachedExpr::Field(path) => {
                firestore_doc_get_field_by_path(document, path).map(Cow::Borrowed)
            }
            CachedExpr::Constant(value) => Some(Cow::Borrowed(*value)),
            CachedExpr::Call(call) => Some(Cow::Owned(call.evaluate(document)?)),
            CachedExpr::Logical(logical) => Some(Cow::Owned(logical.evaluate(document)?)),
        })
    }

    /// Whether `where` keeps `document`: the condition is true. A top-level `and` keeps it when
    /// each of its conditions is true, as separate filters do, so that a condition that is not a
    /// boolean drops the document instead of failing.
    fn holds(&self, document: &FirestoreDocument) -> Result<bool, FirestoreCachePipelineMiss> {
        let is_true = |condition: &CachedExpr<'_>| {
            Ok(matches!(
                condition.evaluate(document)?.as_deref(),
                Some(ValueType::BooleanValue(true))
            ))
        };
        match self {
            CachedExpr::Logical(CachedLogical {
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

impl<'a> CachedCall<'a> {
    fn new(function: &'a FirestoreFunction) -> Result<Self, FirestoreCachePipelineMiss> {
        use CachedArithmetic::{Add, Divide, Mod, Multiply, Subtract};
        use CachedArrayContains::{All, Any, Element};
        use CachedComparison::{
            Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
        };
        use CachedTextTest::{Contains, EndsWith, StartsWith};
        let args = function.args.as_slice();
        match (function.name.as_str(), args) {
            ("equal", [left, right]) => Self::binary(CachedBinary::Comparison(Equal), left, right),
            ("not_equal", [left, right]) => {
                Self::binary(CachedBinary::Comparison(NotEqual), left, right)
            }
            ("less_than", [left, right]) => {
                Self::binary(CachedBinary::Comparison(LessThan), left, right)
            }
            ("less_than_or_equal", [left, right]) => {
                Self::binary(CachedBinary::Comparison(LessThanOrEqual), left, right)
            }
            ("greater_than", [left, right]) => {
                Self::binary(CachedBinary::Comparison(GreaterThan), left, right)
            }
            ("greater_than_or_equal", [left, right]) => {
                Self::binary(CachedBinary::Comparison(GreaterThanOrEqual), left, right)
            }
            ("not", [arg]) => Self::unary(CachedUnary::Not, arg),
            // Firestore accepts only a field here.
            ("exists", [arg @ FirestoreExpr::Field(_)]) => Self::unary(CachedUnary::Exists, arg),
            ("is_absent", [arg @ FirestoreExpr::Field(_)]) => {
                Self::unary(CachedUnary::IsAbsent, arg)
            }
            ("equal_any", [left, right]) => Self::binary(CachedBinary::EqualAny, left, right),
            ("not_equal_any", [left, right]) => {
                Self::binary(CachedBinary::NotEqualAny, left, right)
            }
            ("array_contains", [left, right]) => {
                Self::binary(CachedBinary::ArrayContains(Element), left, right)
            }
            ("array_contains_all", [left, right]) => {
                Self::binary(CachedBinary::ArrayContains(All), left, right)
            }
            ("array_contains_any", [left, right]) => {
                Self::binary(CachedBinary::ArrayContains(Any), left, right)
            }
            ("add", [left, right]) => Self::binary(CachedBinary::Arithmetic(Add), left, right),
            ("subtract", [left, right]) => {
                Self::binary(CachedBinary::Arithmetic(Subtract), left, right)
            }
            ("multiply", [left, right]) => {
                Self::binary(CachedBinary::Arithmetic(Multiply), left, right)
            }
            ("divide", [left, right]) => {
                Self::binary(CachedBinary::Arithmetic(Divide), left, right)
            }
            ("mod", [left, right]) => Self::binary(CachedBinary::Arithmetic(Mod), left, right),
            ("to_lower", [arg]) => Self::unary(CachedUnary::ToLower, arg),
            ("to_upper", [arg]) => Self::unary(CachedUnary::ToUpper, arg),
            ("char_length", [arg]) => Self::unary(CachedUnary::CharLength, arg),
            ("starts_with", [left, right]) => {
                Self::binary(CachedBinary::TextTest(StartsWith), left, right)
            }
            ("ends_with", [left, right]) => {
                Self::binary(CachedBinary::TextTest(EndsWith), left, right)
            }
            ("string_contains", [left, right]) => {
                Self::binary(CachedBinary::TextTest(Contains), left, right)
            }
            ("string_concat" | "concat", [_, ..]) => Ok(Self::StringConcat(
                args.iter().map(CachedExpr::new).collect::<Result<_, _>>()?,
            )),
            (name, args) => Err(FirestoreCachePipelineMiss::unsupported(format!(
                "the function `{name}` of {} arguments",
                args.len()
            ))),
        }
    }

    fn unary(
        function: CachedUnary,
        arg: &'a FirestoreExpr,
    ) -> Result<Self, FirestoreCachePipelineMiss> {
        Ok(Self::Unary(function, Box::new(CachedExpr::new(arg)?)))
    }

    fn binary(
        function: CachedBinary,
        left: &'a FirestoreExpr,
        right: &'a FirestoreExpr,
    ) -> Result<Self, FirestoreCachePipelineMiss> {
        Ok(Self::Binary(
            function,
            Box::new([CachedExpr::new(left)?, CachedExpr::new(right)?]),
        ))
    }

    fn evaluate(
        &self,
        document: &FirestoreDocument,
    ) -> Result<ValueType, FirestoreCachePipelineMiss> {
        match self {
            Self::Unary(function, arg) => function.evaluate(arg.evaluate(document)?.as_deref()),
            Self::Binary(function, args) => {
                let [left, right] = &**args;
                function.evaluate(
                    left.evaluate(document)?.as_deref(),
                    right.evaluate(document)?.as_deref(),
                )
            }
            Self::StringConcat(args) => {
                let values = args
                    .iter()
                    .map(|arg| arg.evaluate(document))
                    .collect::<Result<Vec<_>, _>>()?;
                let texts = values
                    .iter()
                    .map(|value| operand(value.as_deref(), as_text, "a string"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(match texts.into_iter().collect::<Option<Vec<_>>>() {
                    Some(texts) => ValueType::StringValue(texts.concat()),
                    None => NULL,
                })
            }
        }
    }
}

impl CachedUnary {
    fn evaluate(self, value: Option<&ValueType>) -> Result<ValueType, FirestoreCachePipelineMiss> {
        match self {
            Self::Not => Ok(match truth(value)? {
                Some(truth) => ValueType::BooleanValue(!truth),
                None => NULL,
            }),
            Self::Exists => Ok(ValueType::BooleanValue(value.is_some())),
            Self::IsAbsent => Ok(ValueType::BooleanValue(value.is_none())),
            Self::ToLower => with_text(value, |text| {
                Ok(ValueType::StringValue(text.to_lowercase()))
            }),
            Self::ToUpper => with_text(value, |text| {
                Ok(ValueType::StringValue(text.to_uppercase()))
            }),
            Self::CharLength => with_text(value, |text| {
                i64::try_from(text.chars().count())
                    .map(ValueType::IntegerValue)
                    .map_err(|_| {
                        FirestoreCachePipelineMiss::unsupported("a string too long to count")
                    })
            }),
        }
    }
}

impl CachedBinary {
    fn evaluate(
        self,
        left: Option<&ValueType>,
        right: Option<&ValueType>,
    ) -> Result<ValueType, FirestoreCachePipelineMiss> {
        Ok(match self {
            Self::Comparison(comparison) => ValueType::BooleanValue(comparison.holds(left, right)?),
            Self::EqualAny => ValueType::BooleanValue(Self::is_in(left, right)?),
            Self::NotEqualAny => ValueType::BooleanValue(!Self::is_in(left, right)?),
            Self::ArrayContains(kind) => match kind.found(left, right)? {
                Some(found) => ValueType::BooleanValue(found),
                None => NULL,
            },
            Self::Arithmetic(arithmetic) => {
                let left = operand(left, CachedNumber::new, "a number")?;
                let right = operand(right, CachedNumber::new, "a number")?;
                match (left, right) {
                    (Some(left), Some(right)) => arithmetic.apply(left, right)?,
                    _ => NULL,
                }
            }
            Self::TextTest(test) => {
                let left = operand(left, as_text, "a string")?;
                let right = operand(right, as_text, "a string")?;
                match (left, right) {
                    (Some(text), Some(part)) => ValueType::BooleanValue(test.holds(text, part)),
                    _ => NULL,
                }
            }
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

impl CachedComparison {
    /// Absent equals absent and nothing else, null equals null, and values of different types are
    /// unequal and incomparable.
    fn holds(
        self,
        left: Option<&ValueType>,
        right: Option<&ValueType>,
    ) -> Result<bool, FirestoreCachePipelineMiss> {
        let ordering = match (left, right) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(_)) | (Some(_), None) => None,
            (Some(left), Some(right)) => match self {
                Self::Equal | Self::NotEqual => equal(left, right)?.then_some(Ordering::Equal),
                Self::LessThan
                | Self::LessThanOrEqual
                | Self::GreaterThan
                | Self::GreaterThanOrEqual => ordered(left, right)?,
            },
        };
        Ok(match self {
            Self::Equal => ordering == Some(Ordering::Equal),
            Self::NotEqual => ordering != Some(Ordering::Equal),
            Self::LessThan => ordering == Some(Ordering::Less),
            Self::LessThanOrEqual => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
            Self::GreaterThan => ordering == Some(Ordering::Greater),
            Self::GreaterThanOrEqual => {
                matches!(ordering, Some(Ordering::Greater | Ordering::Equal))
            }
        })
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

impl CachedArithmetic {
    /// Integers stay integers, and Firestore fails the request on an overflow or a division by
    /// zero. A double on either side makes a double.
    fn apply(
        self,
        left: CachedNumber,
        right: CachedNumber,
    ) -> Result<ValueType, FirestoreCachePipelineMiss> {
        Ok(match (left, right) {
            (CachedNumber::Integer(left), CachedNumber::Integer(right)) => ValueType::IntegerValue(
                match self {
                    Self::Add => left.checked_add(right),
                    Self::Subtract => left.checked_sub(right),
                    Self::Multiply => left.checked_mul(right),
                    Self::Divide => left.checked_div(right),
                    Self::Mod => left.checked_rem(right),
                }
                .ok_or_else(|| {
                    FirestoreCachePipelineMiss::unsupported(
                        "an integer overflow or a division by zero",
                    )
                })?,
            ),
            (left, right) => {
                let (left, right) = (left.as_double(), right.as_double());
                ValueType::DoubleValue(match self {
                    Self::Add => left + right,
                    Self::Subtract => left - right,
                    Self::Multiply => left * right,
                    Self::Divide => left / right,
                    Self::Mod => left % right,
                })
            }
        })
    }
}

impl CachedTextTest {
    fn holds(self, text: &str, part: &str) -> bool {
        match self {
            Self::StartsWith => text.starts_with(part),
            Self::EndsWith => text.ends_with(part),
            Self::Contains => text.contains(part),
        }
    }
}

impl CachedLogical<'_> {
    /// True or false when a condition decides it, and otherwise null when a condition is null or
    /// absent: `and` of true and null is null, `and` of false and null is false.
    fn evaluate(
        &self,
        document: &FirestoreDocument,
    ) -> Result<ValueType, FirestoreCachePipelineMiss> {
        let decisive = self.operator == CachedLogicalOperator::Or;
        let mut undetermined = false;
        for condition in &self.conditions {
            match truth(condition.evaluate(document)?.as_deref())? {
                Some(truth) if truth == decisive => return Ok(ValueType::BooleanValue(truth)),
                Some(_) => {}
                None => undetermined = true,
            }
        }
        Ok(if undetermined {
            NULL
        } else {
            ValueType::BooleanValue(!decisive)
        })
    }
}

/// An operand of a function that takes the type `extract` reads: a null or absent one makes the
/// function's result null, which is `None` here, and Firestore fails the request on a value of
/// any other type.
fn operand<'v, T>(
    value: Option<&'v ValueType>,
    extract: impl FnOnce(&'v ValueType) -> Option<T>,
    expected: &str,
) -> Result<Option<T>, FirestoreCachePipelineMiss> {
    match value {
        Some(ValueType::NullValue(_)) | None => Ok(None),
        Some(value) => extract(value).map(Some).ok_or_else(|| {
            FirestoreCachePipelineMiss::unsupported(format!("a value that is not {expected}"))
        }),
    }
}

fn as_text(value: &ValueType) -> Option<&str> {
    match value {
        ValueType::StringValue(text) => Some(text),
        _ => None,
    }
}

fn truth(value: Option<&ValueType>) -> Result<Option<bool>, FirestoreCachePipelineMiss> {
    operand(
        value,
        |value| match value {
            ValueType::BooleanValue(flag) => Some(*flag),
            _ => None,
        },
        "a boolean",
    )
}

fn with_text(
    value: Option<&ValueType>,
    function: impl FnOnce(&str) -> Result<ValueType, FirestoreCachePipelineMiss>,
) -> Result<ValueType, FirestoreCachePipelineMiss> {
    match operand(value, as_text, "a string")? {
        Some(text) => function(text),
        None => Ok(NULL),
    }
}

/// A timestamp as Firestore stores and compares it, as a count of microseconds.
fn microseconds(timestamp: &gcloud_sdk::prost_types::Timestamp) -> i64 {
    timestamp
        .seconds
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(timestamp.nanos / 1_000))
}

/// Firestore's `equal` of two present values: numbers by value, NaN equal to NaN, null equal to
/// null, and values of different types unequal.
fn equal(left: &ValueType, right: &ValueType) -> Result<bool, FirestoreCachePipelineMiss> {
    if let (Some(left), Some(right)) = (CachedSortKey::new(left), CachedSortKey::new(right)) {
        return Ok(left.compare(&right) == Some(Ordering::Equal));
    }
    Ok(match (left, right) {
        (ValueType::GeoPointValue(left), ValueType::GeoPointValue(right)) => {
            left.latitude == right.latitude && left.longitude == right.longitude
        }
        (ValueType::ArrayValue(left), ValueType::ArrayValue(right)) => {
            if left.values.len() != right.values.len() {
                return Ok(false);
            }
            for (left, right) in left.values.iter().zip(&right.values) {
                if !equal(data(left)?, data(right)?)? {
                    return Ok(false);
                }
            }
            true
        }
        (ValueType::MapValue(left), ValueType::MapValue(right)) => {
            if left.fields.len() != right.fields.len() {
                return Ok(false);
            }
            for (name, left) in &left.fields {
                match right.fields.get(name) {
                    Some(right) if equal(data(left)?, data(right)?)? => {}
                    _ => return Ok(false),
                }
            }
            true
        }
        _ => false,
    })
}

/// How two present values order for the ordering comparisons, or `None` when Firestore finds
/// them incomparable: values of different types, or NaN with another number.
fn ordered(
    left: &ValueType,
    right: &ValueType,
) -> Result<Option<Ordering>, FirestoreCachePipelineMiss> {
    if let (Some(left), Some(right)) = (CachedSortKey::new(left), CachedSortKey::new(right)) {
        return Ok(left.compare(&right));
    }
    Ok(match (left, right) {
        (ValueType::ArrayValue(_), ValueType::ArrayValue(_))
        | (ValueType::MapValue(_), ValueType::MapValue(_))
        | (ValueType::GeoPointValue(_), ValueType::GeoPointValue(_)) => {
            if !equal(left, right)? {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "an ordering of unequal arrays, maps or geo points",
                ));
            }
            Some(Ordering::Equal)
        }
        _ => None,
    })
}

fn data(value: &Value) -> Result<&ValueType, FirestoreCachePipelineMiss> {
    value
        .value_type
        .as_ref()
        .ok_or_else(|| FirestoreCachePipelineMiss::unsupported("a value without a type"))
}

fn contains(values: &[Value], searched: &ValueType) -> Result<bool, FirestoreCachePipelineMiss> {
    for value in values {
        if equal(data(value)?, searched)? {
            return Ok(true);
        }
    }
    Ok(false)
}
