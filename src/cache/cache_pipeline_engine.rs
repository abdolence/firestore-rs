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

/// Why the cache leaves a pipeline to Firestore.
#[derive(Debug)]
pub(crate) enum FirestoreCachePipelineMiss {
    /// The pipeline, or a value it meets, needs Firestore to evaluate it.
    Unsupported(String),
    /// The path of a collection the cache does not hold every document of.
    IncompleteCollection(String),
}

/// The documents a pipeline's source reads from the cache.
pub(crate) enum FirestoreCachePipelineRows {
    /// Every document the source reads.
    All(Vec<FirestoreDocument>),
    /// The documents named by the source that the cache holds, without those it does not.
    Held(Vec<FirestoreDocument>),
}

enum CachedSource {
    Collection(CachedCollectionSource),
    /// The documents at these full paths.
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
    /// A field by its dotted path.
    Field(&'a str),
    Constant(&'a ValueType),
    Call(CachedCall<'a>),
    Logical(CachedLogical<'a>),
}

struct CachedCall<'a> {
    function: CachedFunction,
    args: Vec<CachedExpr<'a>>,
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
enum CachedFunction {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Not,
    Exists,
    IsAbsent,
    EqualAny,
    NotEqualAny,
    ArrayContains,
    ArrayContainsAll,
    ArrayContainsAny,
    Add,
    Subtract,
    Multiply,
    Divide,
    Mod,
    ToLower,
    ToUpper,
    CharLength,
    StartsWith,
    EndsWith,
    StringContains,
    Like,
    StringConcat,
}

/// A value of an expression over one document: `None` for an absent field.
type Evaluated<'d> = Option<Cow<'d, ValueType>>;

const DOCUMENT_NAME_FIELD: &str = "__name__";

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
            collection_id,
            parent,
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
        let path = match parent {
            Some(parent) => {
                let parent = FirestoreReference(parent.clone());
                let parsed = parent.parse().map_err(|_| {
                    FirestoreCachePipelineMiss::unsupported("the parent is not a document path")
                })?;
                format!("{documents_path}/{}/{collection_id}", parsed.path())
            }
            None => format!("{documents_path}/{collection_id}"),
        };
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
                    match CachedSelection::new(selection)? {
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
                    .map(|selection| match CachedSelection::new(selection)? {
                        CachedSelection {
                            name: DOCUMENT_NAME_FIELD,
                            ..
                        } => Err(FirestoreCachePipelineMiss::unsupported(
                            "add_fields of the document name",
                        )),
                        selection => Ok(selection),
                    })
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
                let mut keyed = documents
                    .into_iter()
                    .map(|document| {
                        let keys = orderings
                            .iter()
                            .map(|ordering| {
                                CachedSortKey::try_from(
                                    ordering.expr.evaluate(&document)?.as_deref(),
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok((keys, document))
                    })
                    .collect::<Result<Vec<_>, FirestoreCachePipelineMiss>>()?;
                // Stable, so that documents with equal keys keep the order the source read them
                // in.
                keyed.sort_by(|(left, _), (right, _)| {
                    orderings
                        .iter()
                        .zip(left.iter().zip(right))
                        .map(|(ordering, (left, right))| match ordering.direction {
                            FirestoreQueryDirection::Ascending => left.cmp(right),
                            FirestoreQueryDirection::Descending => right.cmp(left),
                        })
                        .find(|ordering| ordering.is_ne())
                        .unwrap_or(Ordering::Equal)
                });
                Ok(keyed.into_iter().map(|(_, document)| document).collect())
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
}

impl<'a> CachedSelection<'a> {
    fn new(selection: &'a FirestoreSelection) -> Result<Self, FirestoreCachePipelineMiss> {
        let name = match (&selection.alias, &selection.expr) {
            (Some(alias), _) => alias.as_str(),
            (None, FirestoreExpr::Field(path)) => path.as_str(),
            (None, _) => {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "a computed expression without an alias",
                ))
            }
        };
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
enum CachedSortKey {
    Null,
    Boolean(bool),
    Number(CachedNumber),
    Timestamp((i64, i32)),
    String(String),
    Bytes(Vec<u8>),
    Reference(Vec<String>),
}

impl TryFrom<Option<&ValueType>> for CachedSortKey {
    type Error = FirestoreCachePipelineMiss;

    fn try_from(value: Option<&ValueType>) -> Result<Self, Self::Error> {
        Ok(match value {
            None | Some(ValueType::NullValue(_)) => Self::Null,
            Some(ValueType::BooleanValue(flag)) => Self::Boolean(*flag),
            Some(ValueType::TimestampValue(timestamp)) => Self::Timestamp(microseconds(timestamp)),
            Some(ValueType::StringValue(text)) => Self::String(text.clone()),
            Some(ValueType::BytesValue(bytes)) => Self::Bytes(bytes.clone()),
            Some(ValueType::ReferenceValue(name)) => {
                Self::Reference(name.split('/').map(str::to_string).collect())
            }
            Some(value) => Self::Number(CachedNumber::new(value).ok_or_else(|| {
                FirestoreCachePipelineMiss::unsupported(
                    "a sort by an array, map, geo point or vector",
                )
            })?),
        })
    }
}

/// An integer or a double, compared by value as Firestore compares them.
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

    /// The number as a double, an integer rounded to the nearest one.
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
        use CachedFunction::*;
        let args = function.args.as_slice();
        let cached = match (function.name.as_str(), args) {
            ("equal", [_, _]) => Equal,
            ("not_equal", [_, _]) => NotEqual,
            ("less_than", [_, _]) => LessThan,
            ("less_than_or_equal", [_, _]) => LessThanOrEqual,
            ("greater_than", [_, _]) => GreaterThan,
            ("greater_than_or_equal", [_, _]) => GreaterThanOrEqual,
            ("not", [_]) => Not,
            // Firestore accepts only a field here.
            ("exists", [FirestoreExpr::Field(_)]) => Exists,
            ("is_absent", [FirestoreExpr::Field(_)]) => IsAbsent,
            ("equal_any", [_, _]) => EqualAny,
            ("not_equal_any", [_, _]) => NotEqualAny,
            ("array_contains", [_, _]) => ArrayContains,
            ("array_contains_all", [_, _]) => ArrayContainsAll,
            ("array_contains_any", [_, _]) => ArrayContainsAny,
            ("add", [_, _]) => Add,
            ("subtract", [_, _]) => Subtract,
            ("multiply", [_, _]) => Multiply,
            ("divide", [_, _]) => Divide,
            ("mod", [_, _]) => Mod,
            ("to_lower", [_]) => ToLower,
            ("to_upper", [_]) => ToUpper,
            ("char_length", [_]) => CharLength,
            ("starts_with", [_, _]) => StartsWith,
            ("ends_with", [_, _]) => EndsWith,
            ("string_contains", [_, _]) => StringContains,
            ("like", [_, _]) => Like,
            ("string_concat" | "concat", [_, ..]) => StringConcat,
            (name, args) => {
                return Err(FirestoreCachePipelineMiss::unsupported(format!(
                    "the function `{name}` of {} arguments",
                    args.len()
                )))
            }
        };
        Ok(Self {
            function: cached,
            args: args.iter().map(CachedExpr::new).collect::<Result<_, _>>()?,
        })
    }

    fn evaluate(
        &self,
        document: &FirestoreDocument,
    ) -> Result<ValueType, FirestoreCachePipelineMiss> {
        use CachedFunction::*;
        let args: Vec<Evaluated<'_>> = self
            .args
            .iter()
            .map(|arg| arg.evaluate(document))
            .collect::<Result<_, _>>()?;
        let function = self.function;
        Ok(match (function, args.as_slice()) {
            (
                Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual,
                [left, right],
            ) => ValueType::BooleanValue(function.compares(left.as_deref(), right.as_deref())?),
            (Not, [condition]) => match truth(condition.as_deref())? {
                Some(truth) => ValueType::BooleanValue(!truth),
                None => ValueType::NullValue(0),
            },
            (Exists, [field]) => ValueType::BooleanValue(field.is_some()),
            (IsAbsent, [field]) => ValueType::BooleanValue(field.is_none()),
            (EqualAny | NotEqualAny, [searched, search_space]) => {
                let Some(ValueType::ArrayValue(search_space)) = search_space.as_deref() else {
                    return Err(FirestoreCachePipelineMiss::unsupported(
                        "equal_any of a search space that is not an array",
                    ));
                };
                let found = match searched.as_deref() {
                    Some(searched) => contains(&search_space.values, searched)?,
                    None => false,
                };
                ValueType::BooleanValue(found == matches!(function, EqualAny))
            }
            (ArrayContains | ArrayContainsAll | ArrayContainsAny, [array, searched]) => {
                match function.array_contains(array.as_deref(), searched.as_deref())? {
                    Some(found) => ValueType::BooleanValue(found),
                    None => ValueType::NullValue(0),
                }
            }
            (Add | Subtract | Multiply | Divide | Mod, [left, right]) => {
                match (Operand::from(left), Operand::from(right)) {
                    (Operand::Number(left), Operand::Number(right)) => {
                        function.arithmetic(left, right)?
                    }
                    (Operand::NotANumber, _) | (_, Operand::NotANumber) => {
                        return Err(FirestoreCachePipelineMiss::unsupported(
                            "arithmetic of a value that is not a number",
                        ))
                    }
                    _ => ValueType::NullValue(0),
                }
            }
            (
                ToLower | ToUpper | CharLength | StartsWith | EndsWith | StringContains | Like
                | StringConcat,
                args,
            ) => {
                let mut texts = Vec::with_capacity(args.len());
                let mut null = false;
                for arg in args {
                    match arg.as_deref() {
                        Some(ValueType::StringValue(text)) => texts.push(text.as_str()),
                        Some(ValueType::NullValue(_)) | None => null = true,
                        Some(_) => {
                            return Err(FirestoreCachePipelineMiss::unsupported(
                                "a string function of a value that is not a string",
                            ))
                        }
                    }
                }
                if null {
                    ValueType::NullValue(0)
                } else {
                    function.text(&texts)?
                }
            }
            _ => {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "a function with another number of arguments",
                ))
            }
        })
    }
}

/// An operand of an arithmetic function: a null or absent one makes the result null, and
/// Firestore fails the request on any other value that is not a number.
enum Operand {
    Number(CachedNumber),
    Null,
    NotANumber,
}

impl From<&Evaluated<'_>> for Operand {
    fn from(value: &Evaluated<'_>) -> Self {
        match value.as_deref() {
            Some(ValueType::NullValue(_)) | None => Operand::Null,
            Some(value) => CachedNumber::new(value).map_or(Operand::NotANumber, Operand::Number),
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
            ValueType::NullValue(0)
        } else {
            ValueType::BooleanValue(!decisive)
        })
    }
}

impl CachedFunction {
    /// A comparison: absent equals absent and nothing else, null equals null, and values of
    /// different types are unequal and incomparable.
    fn compares(
        self,
        left: Option<&ValueType>,
        right: Option<&ValueType>,
    ) -> Result<bool, FirestoreCachePipelineMiss> {
        let ordering = match (self, left, right) {
            (_, None, None) => Some(Ordering::Equal),
            (_, None, Some(_)) | (_, Some(_), None) => None,
            (Self::Equal | Self::NotEqual, Some(left), Some(right)) => {
                equal(left, right)?.then_some(Ordering::Equal)
            }
            (_, Some(left), Some(right)) => ordered(left, right)?,
        };
        Ok(match self {
            Self::Equal => ordering == Some(Ordering::Equal),
            Self::NotEqual => ordering != Some(Ordering::Equal),
            Self::LessThan => ordering == Some(Ordering::Less),
            Self::LessThanOrEqual => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
            Self::GreaterThan => ordering == Some(Ordering::Greater),
            _ => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
        })
    }

    /// `array_contains`, `_all` and `_any`: false for a value that is not an array, and `None`,
    /// which is null, for a null or absent one.
    fn array_contains(
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
                (Self::ArrayContains, Some(searched)) => contains(array, searched)?,
                (Self::ArrayContainsAll, Some(ValueType::ArrayValue(searched))) => {
                    let mut all = true;
                    for searched in &searched.values {
                        all &= contains(array, data(searched)?)?;
                    }
                    all
                }
                (Self::ArrayContainsAny, Some(ValueType::ArrayValue(searched))) => {
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

    /// Integers stay integers, and Firestore fails the request on an overflow or a division by
    /// zero. A double on either side makes a double.
    fn arithmetic(
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
                    _ => left.checked_rem(right),
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
                    _ => left % right,
                })
            }
        })
    }

    fn text(self, texts: &[&str]) -> Result<ValueType, FirestoreCachePipelineMiss> {
        Ok(match (self, texts) {
            (Self::ToLower, [text]) => ValueType::StringValue(text.to_lowercase()),
            (Self::ToUpper, [text]) => ValueType::StringValue(text.to_uppercase()),
            (Self::CharLength, [text]) => {
                ValueType::IntegerValue(i64::try_from(text.chars().count()).map_err(|_| {
                    FirestoreCachePipelineMiss::unsupported("a string too long to count")
                })?)
            }
            (Self::StartsWith, [text, prefix]) => ValueType::BooleanValue(text.starts_with(prefix)),
            (Self::EndsWith, [text, suffix]) => ValueType::BooleanValue(text.ends_with(suffix)),
            (Self::StringContains, [text, part]) => ValueType::BooleanValue(text.contains(part)),
            (Self::Like, [text, pattern]) => ValueType::BooleanValue(like(text, pattern)?),
            (Self::StringConcat, texts) => ValueType::StringValue(texts.concat()),
            _ => {
                return Err(FirestoreCachePipelineMiss::unsupported(
                    "a string function with another number of arguments",
                ))
            }
        })
    }
}

/// A condition's truth: `None` for null or absent. Firestore fails the request on any other
/// value.
fn truth(value: Option<&ValueType>) -> Result<Option<bool>, FirestoreCachePipelineMiss> {
    match value {
        Some(ValueType::BooleanValue(value)) => Ok(Some(*value)),
        Some(ValueType::NullValue(_)) | None => Ok(None),
        Some(_) => Err(FirestoreCachePipelineMiss::unsupported(
            "a logical function of a value that is not a boolean",
        )),
    }
}

/// A timestamp as Firestore stores and compares it, to the microsecond.
fn microseconds(timestamp: &gcloud_sdk::prost_types::Timestamp) -> (i64, i32) {
    (timestamp.seconds, timestamp.nanos / 1_000)
}

/// Firestore's `equal` of two present values: numbers by value, NaN equal to NaN, null equal to
/// null, and values of different types unequal.
fn equal(left: &ValueType, right: &ValueType) -> Result<bool, FirestoreCachePipelineMiss> {
    if let (Some(left), Some(right)) = (CachedNumber::new(left), CachedNumber::new(right)) {
        return Ok(left.compare(right) == Some(Ordering::Equal));
    }
    Ok(match (left, right) {
        (ValueType::NullValue(_), ValueType::NullValue(_)) => true,
        (ValueType::BooleanValue(left), ValueType::BooleanValue(right)) => left == right,
        (ValueType::TimestampValue(left), ValueType::TimestampValue(right)) => {
            microseconds(left) == microseconds(right)
        }
        (ValueType::StringValue(left), ValueType::StringValue(right)) => left == right,
        (ValueType::BytesValue(left), ValueType::BytesValue(right)) => left == right,
        (ValueType::ReferenceValue(left), ValueType::ReferenceValue(right)) => left == right,
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
    if let (Some(left), Some(right)) = (CachedNumber::new(left), CachedNumber::new(right)) {
        return Ok(left.compare(right));
    }
    Ok(match (left, right) {
        (ValueType::NullValue(_), ValueType::NullValue(_)) => Some(Ordering::Equal),
        (ValueType::BooleanValue(left), ValueType::BooleanValue(right)) => Some(left.cmp(right)),
        (ValueType::TimestampValue(left), ValueType::TimestampValue(right)) => {
            Some(microseconds(left).cmp(&microseconds(right)))
        }
        (ValueType::StringValue(left), ValueType::StringValue(right)) => Some(left.cmp(right)),
        (ValueType::BytesValue(left), ValueType::BytesValue(right)) => Some(left.cmp(right)),
        (ValueType::ReferenceValue(left), ValueType::ReferenceValue(right)) => {
            Some(left.split('/').cmp(right.split('/')))
        }
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

/// Firestore's `like`: `%` matches any run of characters, `_` one character, and a backslash
/// makes the `%`, `_` or backslash after it literal. Firestore fails on a pattern that ends in a
/// backslash, and the cache leaves a backslash before any other character to Firestore.
fn like(text: &str, pattern: &str) -> Result<bool, FirestoreCachePipelineMiss> {
    enum Token {
        Any,
        One,
        Literal(char),
    }
    let mut tokens = Vec::new();
    let mut pattern = pattern.chars();
    while let Some(character) = pattern.next() {
        tokens.push(match character {
            '%' => Token::Any,
            '_' => Token::One,
            '\\' => match pattern.next() {
                Some(escaped @ ('%' | '_' | '\\')) => Token::Literal(escaped),
                _ => {
                    return Err(FirestoreCachePipelineMiss::unsupported(
                        "a like pattern with a backslash that escapes no wildcard",
                    ))
                }
            },
            character => Token::Literal(character),
        });
    }
    let text: Vec<char> = text.chars().collect();
    // matched[i]: the tokens so far match the first i characters.
    let mut matched = vec![false; text.len() + 1];
    matched[0] = true;
    for token in &tokens {
        let mut next = vec![false; text.len() + 1];
        match token {
            Token::Any => {
                let mut reached = false;
                for (index, slot) in next.iter_mut().enumerate() {
                    reached |= matched[index];
                    *slot = reached;
                }
            }
            Token::One => next[1..].copy_from_slice(&matched[..text.len()]),
            Token::Literal(character) => {
                for (index, slot) in next[1..].iter_mut().enumerate() {
                    *slot = matched[index] && text[index] == *character;
                }
            }
        }
        matched = next;
    }
    Ok(matched[text.len()])
}
