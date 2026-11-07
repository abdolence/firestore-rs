//! Differential tests of the pipelines a cache answers: each pipeline runs on Firestore and on a
//! cache of the same documents, and the two must return the same rows, or both fail.
//!
//! The documents live in the Enterprise edition database `GCP_ENTERPRISE_DATABASE_ID`, in
//! collections that only this binary writes. They hold the values where pipeline semantics are
//! easy to get wrong: null next to an absent field, integers next to doubles of the same value,
//! NaN, and every value type.

use crate::common::setup_enterprise_unseeded;
use firestore::errors::FirestoreError;
use firestore::gcloud_sdk::google::firestore::v1::value::ValueType;
use firestore::gcloud_sdk::google::firestore::v1::{ArrayValue, Document, MapValue, Value};
use firestore::gcloud_sdk::prost_types::Timestamp;
use firestore::pipeline_expr_builder::FirestorePipelineExprBuilder;
use firestore::*;
use futures::TryStreamExt;
use std::collections::HashMap;
use tokio::sync::OnceCell;

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// The preloaded collection of edge cases.
const EDGE: &str = "pipeline-cache-edge";
/// A collection the cache fills lazily, through reads by ID.
const SESSIONS: &str = "pipeline-cache-sessions";
/// A preloaded collection under the edge case `d01`.
const CHILDREN: &str = "pipeline-cache-children";

static SEEDED: OnceCell<Result<(), String>> = OnceCell::const_new();

fn value(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
    }
}

fn int(number: i64) -> Value {
    value(ValueType::IntegerValue(number))
}

fn double(number: f64) -> Value {
    value(ValueType::DoubleValue(number))
}

fn string(text: &str) -> Value {
    value(ValueType::StringValue(text.to_string()))
}

fn boolean(flag: bool) -> Value {
    value(ValueType::BooleanValue(flag))
}

fn null() -> Value {
    value(ValueType::NullValue(0))
}

fn array(values: Vec<Value>) -> Value {
    value(ValueType::ArrayValue(ArrayValue { values }))
}

fn map(fields: Vec<(&str, Value)>) -> Value {
    value(ValueType::MapValue(MapValue {
        fields: fields
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    }))
}

fn document_reference(db: &FirestoreDb, collection: &str, id: &str) -> Value {
    value(ValueType::ReferenceValue(format!(
        "{}/{collection}/{id}",
        db.get_documents_path()
    )))
}

fn constant(value: Value) -> FirestoreExpr {
    FirestoreExpr::Constant(FirestoreValue::from(value))
}

fn field(path: &str) -> FirestoreExpr {
    FirestoreExpr::field(path)
}

/// The edge cases, one document each. `rank` orders them totally and `k` names the kind of `v`.
fn edge_documents(db: &FirestoreDb) -> Vec<(&'static str, Vec<(&'static str, Value)>)> {
    let first = document_reference(db, EDGE, "d01");
    vec![
        (
            "d01",
            vec![
                ("k", string("int")),
                ("v", int(1)),
                ("n", int(7)),
                ("s", string("Hello")),
                ("tags", array(vec![string("a"), string("b")])),
                ("b", boolean(true)),
            ],
        ),
        (
            "d02",
            vec![
                ("k", string("double")),
                ("v", double(1.0)),
                ("n", double(2.5)),
                ("s", string("hello")),
                ("tags", array(vec![string("b")])),
                ("b", boolean(false)),
            ],
        ),
        (
            "d03",
            vec![
                ("k", string("int")),
                ("v", int(2)),
                ("n", int(-7)),
                ("s", string("Café")),
                ("tags", array(vec![])),
                ("b", null()),
            ],
        ),
        (
            "d04",
            vec![
                ("k", string("double")),
                ("v", double(2.5)),
                ("n", int(0)),
                ("s", string("ÉCOLE")),
                ("tags", array(vec![null()])),
            ],
        ),
        (
            "d05",
            vec![
                ("k", string("nan")),
                ("v", double(f64::NAN)),
                ("n", double(f64::NAN)),
                ("s", string("straße")),
                ("tags", array(vec![string("a"), int(1)])),
            ],
        ),
        (
            "d06",
            vec![
                ("k", string("zero")),
                ("v", double(-0.0)),
                ("n", double(-0.0)),
                ("s", string("100%")),
                ("tags", array(vec![int(1), double(2.0)])),
            ],
        ),
        (
            "d07",
            vec![
                ("k", string("string")),
                ("v", string("a")),
                ("n", null()),
                ("s", string("a_c")),
                ("tags", null()),
            ],
        ),
        (
            "d08",
            vec![
                ("k", string("string")),
                ("v", string("B")),
                ("s", string("")),
                ("tags", string("a")),
            ],
        ),
        (
            "d09",
            vec![
                ("k", string("bool")),
                ("v", boolean(true)),
                ("n", int(9_007_199_254_740_993)),
                ("s", null()),
                ("tags", array(vec![string("x"), array(vec![string("a")])])),
                ("b", string("yes")),
            ],
        ),
        (
            "d10",
            vec![
                ("k", string("null")),
                ("v", null()),
                ("n", double(9_007_199_254_740_992.0)),
            ],
        ),
        (
            "d11",
            vec![
                ("k", string("absent")),
                ("n", int(i64::MAX)),
                ("s", int(5)),
                ("tags", array(vec![int(2)])),
            ],
        ),
        (
            "d12",
            vec![
                ("k", string("timestamp")),
                (
                    "v",
                    value(ValueType::TimestampValue(Timestamp {
                        seconds: 1_704_067_200,
                        nanos: 500,
                    })),
                ),
                ("n", int(i64::MIN)),
                ("s", string("e\u{301}")),
            ],
        ),
        (
            "d13",
            vec![
                ("k", string("reference")),
                ("v", first),
                ("n", int(1)),
                ("s", string("İstanbul")),
            ],
        ),
        (
            "d14",
            vec![
                ("k", string("array")),
                ("v", array(vec![int(1), int(2)])),
                ("n", int(2)),
                ("s", string("ΣΑΣ")),
            ],
        ),
        (
            "d15",
            vec![
                ("k", string("map")),
                ("v", map(vec![("x", int(1))])),
                ("n", int(3)),
                ("s", string("  pad  ")),
            ],
        ),
        (
            "d16",
            vec![
                ("k", string("bool")),
                ("v", boolean(false)),
                ("n", int(4)),
                ("s", string("Hello World")),
            ],
        ),
        (
            "d17",
            vec![
                ("k", string("bigint")),
                ("v", int(9_007_199_254_740_993)),
                ("n", int(5)),
                ("s", string("Ab")),
            ],
        ),
        (
            "d18",
            vec![
                ("k", string("bigdouble")),
                ("v", double(9_007_199_254_740_992.0)),
                ("n", int(6)),
                ("s", string("aB")),
            ],
        ),
        (
            "d19",
            vec![
                ("k", string("bytes")),
                ("v", value(ValueType::BytesValue(b"ab".to_vec()))),
                ("n", double(1.5)),
                ("s", string("x%y")),
            ],
        ),
        (
            "d20",
            vec![
                ("k", string("geo")),
                (
                    "v",
                    value(ValueType::GeoPointValue(
                        firestore::gcloud_sdk::google::r#type::LatLng {
                            latitude: 1.0,
                            longitude: 2.0,
                        },
                    )),
                ),
                ("n", int(10)),
                ("s", string("a\\b")),
            ],
        ),
    ]
}

fn session_documents() -> Vec<(&'static str, Vec<(&'static str, Value)>)> {
    ["s1", "s2", "s3"]
        .into_iter()
        .map(|id| (id, vec![("user", string(&format!("user-of-{id}")))]))
        .collect()
}

fn child_documents() -> Vec<(&'static str, Vec<(&'static str, Value)>)> {
    vec![("c1", vec![("x", int(1))]), ("c2", vec![("x", int(2))])]
}

/// The path of the children's parent document.
fn children_parent(db: &FirestoreDb) -> String {
    format!("{}/{EDGE}/d01", db.get_documents_path())
}

/// Writes every document by its fixed ID, with `rank` set from its position, once per test binary
/// and before any cache preloads them.
async fn ensure_seeded(db: &FirestoreDb) -> TestResult {
    SEEDED
        .get_or_init(|| async {
            let documents_path = db.get_documents_path();
            let collections = [
                (documents_path.clone(), EDGE, edge_documents(db)),
                (documents_path.clone(), SESSIONS, session_documents()),
                (children_parent(db), CHILDREN, child_documents()),
            ];
            for (parent, collection, documents) in collections {
                for (rank, (id, fields)) in documents.into_iter().enumerate() {
                    let mut fields: HashMap<String, Value> = fields
                        .into_iter()
                        .map(|(name, value)| (name.to_string(), value))
                        .collect();
                    fields.insert("rank".to_string(), int(rank as i64 + 1));
                    let document = Document {
                        name: format!("{parent}/{collection}/{id}"),
                        fields,
                        ..Document::default()
                    };
                    db.fluent()
                        .update()
                        .in_col(collection)
                        .document(document)
                        .execute()
                        .await
                        .map_err(|err| err.to_string())?;
                }
            }
            Ok(())
        })
        .await
        .clone()
        .map_err(Into::into)
}

/// A client with the seeded collections, and a memory cache that preloads the edge cases and the
/// children, and fills the sessions lazily.
async fn setup(
) -> Result<(FirestoreDb, FirestoreMemoryCache), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup_enterprise_unseeded().await?;
    ensure_seeded(&db).await?;
    let cache = FirestoreCache::memory(&db)
        .preloaded_collection(EDGE)
        .collection(SESSIONS)
        .collection_with(CHILDREN, |c| c.parent(children_parent(&db)).preload_all())
        .build()
        .await?;
    Ok((db, cache))
}

/// What a case returns on Firestore.
#[derive(Clone, Copy, PartialEq)]
enum Expect {
    /// Rows, compared in the order they arrive.
    RowsInOrder,
    /// Rows, compared in any order.
    Rows,
    /// A failure of the whole request.
    Failure,
}

/// Runs cases on Firestore and on the cache, and collects every disagreement, so that one run
/// reports all of them.
struct Differential {
    db: FirestoreDb,
    cache: FirestoreMemoryCache,
    disagreements: Vec<String>,
}

impl Differential {
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (db, cache) = setup().await?;
        Ok(Self {
            db,
            cache,
            disagreements: Vec::new(),
        })
    }

    fn pipeline(
        &self,
    ) -> firestore::pipeline_builder::FirestorePipelineInitialBuilder<
        firestore::pipeline_builder::FirestorePipelineDbContext<'_, FirestoreDb>,
    > {
        self.db.fluent().pipeline().select()
    }

    fn edge_where(&self, condition: FirestoreBoolExpr) -> FirestorePipeline {
        self.pipeline()
            .from(EDGE)
            .filter(|q| q.condition(condition))
            .into_pipeline()
    }

    /// The case returns the same rows from the cache as from Firestore, or, when it is expected to
    /// fail, fails on both.
    async fn same(&mut self, case: &str, expect: Expect, query: impl Into<Query>) {
        let query = query.into();
        let firestore = query.run(&self.db).await;
        let cached = query.run(&self.db.read_cached_only(&self.cache)).await;
        let disagreement = match (firestore, cached) {
            (Served::Firestore(_), _) if expect == Expect::Failure => {
                Some("Firestore returned rows, where the case expects a failure".to_string())
            }
            (Served::Firestore(expected), Served::Cache(found)) => {
                let (expected, found) = (canonical_rows(&expected, expect), canonical_rows(&found, expect));
                (expected != found)
                    .then(|| format!("Firestore returned {expected:#?}\nthe cache returned {found:#?}"))
            }
            (Served::Failed(err), Served::Failed(FirestoreError::CacheError(_))) => (expect
                != Expect::Failure)
                .then(|| format!("both failed, Firestore with {err}")),
            (Served::Failed(err), Served::Cache(found)) => Some(format!(
                "Firestore failed with {err}\nthe cache returned {:#?}",
                canonical_rows(&found, expect)
            )),
            (Served::Firestore(expected), Served::Failed(err)) => Some(format!(
                "Firestore returned {:#?}\nthe cache failed with {err}",
                canonical_rows(&expected, expect)
            )),
            (_, Served::Firestore(_)) => {
                Some("the cached-only session was answered by Firestore".to_string())
            }
            (Served::Failed(firestore_err), Served::Failed(err)) => Some(format!(
                "Firestore failed with {firestore_err}\nthe cache failed with {err}, not a cache error"
            )),
            (Served::Cache(_), _) => {
                Some("Firestore answered as though it were the cache".to_string())
            }
        };
        if let Some(disagreement) = disagreement {
            self.disagreements.push(format!("{case}: {disagreement}"));
        }
    }

    /// The cache does not answer the case: a cached-only session fails with a cache error, and a
    /// read-through session returns what Firestore returns, from Firestore.
    async fn refused(&mut self, case: &str, query: impl Into<Query>) {
        let cached_db = self.db.read_cached_only(&self.cache);
        let through_db = self.db.read_through_cache(&self.cache);
        self.refused_by(case, &cached_db, &through_db, query.into())
            .await;
    }

    async fn refused_by(
        &mut self,
        case: &str,
        cached_db: &FirestoreDb,
        through_db: &FirestoreDb,
        query: Query,
    ) {
        match query.run(cached_db).await {
            Served::Failed(FirestoreError::CacheError(_)) => {}
            Served::Failed(err) => self
                .disagreements
                .push(format!("{case}: the cached-only session failed with {err}")),
            _ => self
                .disagreements
                .push(format!("{case}: the cached-only session returned rows")),
        }
        let expected = query.run(&self.db).await;
        let through = query.run(through_db).await;
        match (expected, through) {
            (Served::Firestore(expected), Served::Firestore(found)) => {
                let (expected, found) = (
                    canonical_rows(&expected, Expect::Rows),
                    canonical_rows(&found, Expect::Rows),
                );
                if expected != found {
                    self.disagreements.push(format!(
                        "{case}: read-through returned {found:#?}, Firestore {expected:#?}"
                    ));
                }
            }
            (_, Served::Cache(_)) => self
                .disagreements
                .push(format!("{case}: read-through was answered by the cache")),
            (Served::Failed(_), Served::Failed(_)) => {}
            _ => self
                .disagreements
                .push(format!("{case}: read-through disagrees with Firestore")),
        }
    }

    async fn finish(self) -> TestResult {
        self.cache.shutdown().await?;
        assert!(
            self.disagreements.is_empty(),
            "{} cases disagree:\n\n{}",
            self.disagreements.len(),
            self.disagreements.join("\n\n")
        );
        Ok(())
    }
}

/// What a case runs: a pipeline, or a SQL statement, which runs as one.
#[derive(Clone)]
enum Query {
    Pipeline(FirestorePipeline),
    #[cfg(feature = "sql")]
    Sql(&'static str),
}

impl From<FirestorePipeline> for Query {
    fn from(pipeline: FirestorePipeline) -> Self {
        Query::Pipeline(pipeline)
    }
}

impl Query {
    async fn run(&self, db: &FirestoreDb) -> Served {
        let stream = match self {
            Query::Pipeline(pipeline) => {
                db.fluent()
                    .pipeline()
                    .select()
                    .from_pipeline(pipeline.clone())
                    .stream_query_with_metadata()
                    .await
            }
            #[cfg(feature = "sql")]
            Query::Sql(statement) => {
                db.fluent()
                    .sql(*statement)
                    .stream_query_with_metadata()
                    .await
            }
        };
        match stream {
            Ok(stream) => match stream.try_collect::<Vec<_>>().await {
                Ok(items) => Served::from(items),
                Err(err) => Served::Failed(err),
            },
            Err(err) => Served::Failed(err),
        }
    }
}

/// Where the rows of a run came from. A cached result carries no execution time.
enum Served {
    Firestore(Vec<FirestoreDocument>),
    Cache(Vec<FirestoreDocument>),
    Failed(FirestoreError),
}

impl From<Vec<FirestoreWithPipelineMetadata<FirestoreDocument>>> for Served {
    fn from(items: Vec<FirestoreWithPipelineMetadata<FirestoreDocument>>) -> Self {
        let from_firestore = items
            .iter()
            .any(|item| item.metadata.execution_time.is_some());
        let documents = items.into_iter().filter_map(|item| item.document).collect();
        if from_firestore {
            Served::Firestore(documents)
        } else {
            Served::Cache(documents)
        }
    }
}

/// The rows as text that tells an integer from a double and keeps NaN equal to itself.
fn canonical_rows(documents: &[FirestoreDocument], expect: Expect) -> Vec<String> {
    let mut canonical: Vec<String> = documents
        .iter()
        .map(|document| {
            let mut fields: Vec<String> = document
                .fields
                .iter()
                .map(|(name, value)| format!("{name}: {}", canonical_value(value)))
                .collect();
            fields.sort();
            format!(
                "{} created {:?} updated {:?} {{{}}}",
                document.name,
                document.create_time,
                document.update_time,
                fields.join(", ")
            )
        })
        .collect();
    if expect != Expect::RowsInOrder {
        canonical.sort();
    }
    canonical
}

fn canonical_value(value: &Value) -> String {
    match &value.value_type {
        Some(ValueType::IntegerValue(number)) => format!("{number}i"),
        Some(ValueType::DoubleValue(number)) => format!("{number:?}d"),
        Some(ValueType::ArrayValue(array)) => format!(
            "[{}]",
            array
                .values
                .iter()
                .map(canonical_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Some(ValueType::MapValue(map)) => {
            let mut fields: Vec<String> = map
                .fields
                .iter()
                .map(|(name, value)| format!("{name}: {}", canonical_value(value)))
                .collect();
            fields.sort();
            format!("{{{}}}", fields.join(", "))
        }
        other => format!("{other:?}"),
    }
}

/// Constants of every kind of `v`, for the comparisons.
fn comparison_constants(db: &FirestoreDb) -> Vec<(&'static str, Value)> {
    vec![
        ("int 1", int(1)),
        ("double 1.0", double(1.0)),
        ("double 2.5", double(2.5)),
        ("double 0.0", double(0.0)),
        ("NaN", double(f64::NAN)),
        ("int 2^53+1", int(9_007_199_254_740_993)),
        ("double 2^53", double(9_007_199_254_740_992.0)),
        ("string a", string("a")),
        ("string B", string("B")),
        ("true", boolean(true)),
        ("null", null()),
        (
            "timestamp",
            value(ValueType::TimestampValue(Timestamp {
                seconds: 1_704_067_200,
                nanos: 500,
            })),
        ),
        (
            "timestamp 999 nanoseconds later",
            value(ValueType::TimestampValue(Timestamp {
                seconds: 1_704_067_200,
                nanos: 999,
            })),
        ),
        (
            "timestamp a microsecond later",
            value(ValueType::TimestampValue(Timestamp {
                seconds: 1_704_067_200,
                nanos: 1_000,
            })),
        ),
        ("reference d01", document_reference(db, EDGE, "d01")),
        ("bytes", value(ValueType::BytesValue(b"ab".to_vec()))),
    ]
}

#[tokio::test]
async fn comparisons_match_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    type Comparison = fn(FirestoreExpr, FirestoreExpr) -> FirestoreBoolExpr;
    let comparisons: [(&str, Comparison); 6] = [
        ("equal", |left, right| left.equal(right)),
        ("not_equal", |left, right| left.not_equal(right)),
        ("less_than", |left, right| left.less_than(right)),
        ("less_than_or_equal", |left, right| {
            left.less_than_or_equal(right)
        }),
        ("greater_than", |left, right| left.greater_than(right)),
        ("greater_than_or_equal", |left, right| {
            left.greater_than_or_equal(right)
        }),
    ];

    for (name, comparison) in comparisons {
        for (label, compared) in comparison_constants(&differential.db) {
            let pipeline = differential.edge_where(comparison(field("v"), constant(compared)));
            differential
                .same(&format!("v {name} {label}"), Expect::Rows, pipeline)
                .await;
        }
        let absent_with_absent =
            differential.edge_where(comparison(field("missing"), field("also_missing")));
        differential
            .same(
                &format!("absent {name} absent"),
                Expect::Rows,
                absent_with_absent,
            )
            .await;
        let null_with_absent =
            differential.edge_where(comparison(constant(null()), field("missing")));
        differential
            .same(
                &format!("null {name} absent"),
                Expect::Rows,
                null_with_absent,
            )
            .await;
        let value_with_itself = differential.edge_where(comparison(field("v"), field("v")));
        differential
            .same(&format!("v {name} v"), Expect::Rows, value_with_itself)
            .await;
        // `n` holds i64::MAX, i64::MIN and 2^53 + 1, which a double cannot hold.
        for (label, bound) in [
            ("double 2^63", double(9_223_372_036_854_775_808.0)),
            ("double -2^63", double(-9_223_372_036_854_775_808.0)),
            ("double 2^53", double(9_007_199_254_740_992.0)),
            ("int 2^53+1", int(9_007_199_254_740_993)),
        ] {
            let pipeline = differential.edge_where(comparison(field("n"), constant(bound)));
            differential
                .same(&format!("n {name} {label}"), Expect::Rows, pipeline)
                .await;
        }
    }

    differential.finish().await
}

#[tokio::test]
async fn logic_matches_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    let e = FirestorePipelineExprBuilder;
    let flag = || FirestoreBoolExpr::unchecked(field("b"));
    let is_int = || field("k").equal("int");
    let cases = [
        (
            "and of comparisons",
            e.and([is_int(), field("v").greater_than(1)]),
        ),
        ("or of comparisons", e.or([is_int(), field("v").equal("a")])),
        ("not of a comparison", e.not(field("v").equal(1))),
        (
            "and with a field",
            e.and([flag(), e.constant(true).equal(true)]),
        ),
        (
            "and with false first",
            e.and([field("v").equal(false), flag()]),
        ),
        (
            "or with true first",
            e.or([field("rank").greater_than(0), flag()]),
        ),
        ("a field as the condition", flag()),
        (
            "a string field as the condition",
            FirestoreBoolExpr::unchecked(field("s")),
        ),
        (
            "nested and in or",
            e.or([
                e.and([is_int(), field("n").greater_than(0)]),
                field("k").equal("double"),
            ]),
        ),
    ];
    for (case, condition) in cases {
        let pipeline = differential.edge_where(condition);
        differential.same(case, Expect::Rows, pipeline).await;
    }

    for (case, condition) in [
        ("not of a field", e.not(flag())),
        ("or with a field", e.or([flag(), field("v").equal(1)])),
    ] {
        let pipeline = differential.edge_where(condition);
        differential.same(case, Expect::Failure, pipeline).await;
    }

    let without_string_flag =
        |condition: FirestoreBoolExpr| e.and([field("k").not_equal("bool"), condition]);
    for (case, condition) in [
        (
            "and with a flag",
            e.and([flag(), field("rank").greater_than(0)]),
        ),
        ("or with a flag", e.or([flag(), field("rank").less_than(0)])),
        ("not of a flag", e.not(flag())),
        ("a flag as the condition", flag()),
    ] {
        let pipeline = differential.edge_where(without_string_flag(condition));
        differential
            .same(&format!("{case}, booleans only"), Expect::Rows, pipeline)
            .await;
    }

    differential.finish().await
}

/// The value a condition outputs, which tells false from null where a filter cannot.
#[tokio::test]
async fn condition_values_match_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    let e = FirestorePipelineExprBuilder;
    let flag = || FirestoreBoolExpr::unchecked(field("b"));
    let null_condition = || FirestoreBoolExpr::unchecked(constant(null()));
    let logical = [
        ("not of a flag", e.not(flag())),
        (
            "and of a flag and true",
            e.and([flag(), e.constant(true).equal(true)]),
        ),
        (
            "and of a flag and false",
            e.and([flag(), e.constant(true).equal(false)]),
        ),
        (
            "or of a flag and true",
            e.or([flag(), e.constant(true).equal(true)]),
        ),
        (
            "or of a flag and false",
            e.or([flag(), e.constant(true).equal(false)]),
        ),
        ("and of null and a flag", e.and([null_condition(), flag()])),
        ("not of null", e.not(null_condition())),
    ];
    for (case, condition) in logical {
        let pipeline = differential
            .pipeline()
            .from(EDGE)
            .filter(|q| q.field("k").not_equal("bool"))
            .add_fields(|_| [condition.alias("result")])
            .into_pipeline();
        differential.same(case, Expect::Rows, pipeline).await;
    }
    let everywhere = [
        ("array_contains", field("tags").array_contains("a")),
        (
            "array_contains_any",
            field("tags").array_contains_any(constant(array(vec![string("a")]))),
        ),
        (
            "array_contains_all",
            field("tags").array_contains_all(constant(array(vec![string("a")]))),
        ),
        (
            "equal_any",
            field("v").equal_any(constant(array(vec![int(1), null()]))),
        ),
        (
            "not_equal_any",
            field("v").not_equal_any(constant(array(vec![int(1)]))),
        ),
        ("equal null", field("v").equal(constant(null()))),
        ("less_than", field("v").less_than(2)),
        ("exists", field("v").exists()),
        ("is_absent", field("v").is_absent()),
    ];
    for (case, condition) in everywhere {
        let pipeline = differential
            .pipeline()
            .from(EDGE)
            .add_fields(|_| [condition.alias("result")])
            .into_pipeline();
        differential
            .same(&format!("value of {case}"), Expect::Rows, pipeline)
            .await;
    }

    differential.finish().await
}

#[tokio::test]
async fn membership_matches_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    let first = document_reference(&differential.db, EDGE, "d01");
    let fifth = document_reference(&differential.db, EDGE, "d05");
    let cases = [
        (
            "equal_any of mixed values",
            field("v").equal_any(constant(array(vec![int(1), string("a"), null()]))),
        ),
        (
            "equal_any of NaN",
            field("v").equal_any(constant(array(vec![double(f64::NAN)]))),
        ),
        (
            "not_equal_any of mixed values",
            field("v").not_equal_any(constant(array(vec![int(1), null()]))),
        ),
        (
            "equal_any of an empty array",
            field("v").equal_any(constant(array(vec![]))),
        ),
        (
            "not_equal_any of an empty array",
            field("v").not_equal_any(constant(array(vec![]))),
        ),
        (
            "equal_any of names",
            field("__name__").equal_any(constant(array(vec![first.clone(), fifth]))),
        ),
        ("exists", field("v").exists()),
        ("is_absent", field("v").is_absent()),
        ("exists of a nested field", field("v.x").exists()),
        ("equal of a nested field", field("v.x").equal(1)),
        ("equal of a name", field("__name__").equal(constant(first))),
        (
            "array_contains of a string",
            field("tags").array_contains("a"),
        ),
        (
            "array_contains of null",
            field("tags").array_contains(constant(null())),
        ),
        (
            "array_contains of a double",
            field("tags").array_contains(2.0),
        ),
        (
            "array_contains of an array",
            field("tags").array_contains(constant(array(vec![string("a")]))),
        ),
        (
            "array_contains_any",
            field("tags").array_contains_any(constant(array(vec![string("a"), int(1)]))),
        ),
        (
            "array_contains_all",
            field("tags").array_contains_all(constant(array(vec![string("a"), string("b")]))),
        ),
        (
            "array_contains_all of an empty array",
            field("tags").array_contains_all(constant(array(vec![]))),
        ),
    ];
    for (case, condition) in cases {
        let pipeline = differential.edge_where(condition);
        differential.same(case, Expect::Rows, pipeline).await;
    }
    let not_an_array = differential.edge_where(field("v").equal_any(field("s")));
    differential
        .same(
            "equal_any of a field that is not an array",
            Expect::Failure,
            not_an_array,
        )
        .await;

    differential.finish().await
}

#[tokio::test]
async fn arithmetic_matches_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    type Operation = fn(FirestoreExpr) -> FirestoreExpr;
    // `n` holds i64::MAX and i64::MIN, which overflow, and an integer division by zero fails.
    let operations: [(&str, Operation, Expect, Expect); 9] = [
        ("add 1", |n| n.add(1), Expect::Rows, Expect::Failure),
        ("add 0.5", |n| n.add(0.5), Expect::Rows, Expect::Rows),
        (
            "subtract 1",
            |n| n.subtract(1),
            Expect::Rows,
            Expect::Failure,
        ),
        (
            "multiply 2",
            |n| n.multiply(2),
            Expect::Rows,
            Expect::Failure,
        ),
        ("divide 2", |n| n.divide(2), Expect::Rows, Expect::Rows),
        ("divide 2.0", |n| n.divide(2.0), Expect::Rows, Expect::Rows),
        (
            "divide 0",
            |n| n.divide(0),
            Expect::Failure,
            Expect::Failure,
        ),
        ("mod 3", |n| n.modulo(3), Expect::Rows, Expect::Rows),
        ("mod 0.0", |n| n.modulo(0.0), Expect::Rows, Expect::Rows),
    ];
    for (name, operation, small, every) in operations {
        let in_range = differential
            .pipeline()
            .from(EDGE)
            .filter(|q| q.field("rank").less_than_or_equal(8))
            .add_fields(|_| [operation(field("n")).alias("result")])
            .into_pipeline();
        differential
            .same(&format!("{name} of small numbers"), small, in_range)
            .await;
        let everything = differential
            .pipeline()
            .from(EDGE)
            .add_fields(|_| [operation(field("n")).alias("result")])
            .into_pipeline();
        differential
            .same(&format!("{name} of every n"), every, everything)
            .await;
    }
    let of_absent = differential
        .pipeline()
        .from(EDGE)
        .add_fields(|_| [field("missing").add(1).alias("result")])
        .into_pipeline();
    differential
        .same("add of an absent field", Expect::Rows, of_absent)
        .await;
    let in_filter = differential.edge_where(field("n").multiply(2).greater_than(5));
    differential
        .same(
            "multiply in a filter over every n",
            Expect::Failure,
            in_filter,
        )
        .await;

    differential.finish().await
}

#[tokio::test]
async fn strings_match_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    type Function = fn(FirestoreExpr) -> FirestoreExpr;
    let functions: [(&str, Function); 14] = [
        ("to_lower", FirestoreExpr::to_lower),
        ("to_upper", FirestoreExpr::to_upper),
        ("char_length", FirestoreExpr::char_length),
        ("starts_with", |s| s.starts_with("He").into()),
        ("ends_with", |s| s.ends_with("lo").into()),
        ("string_contains", |s| s.string_contains("l").into()),
        ("like with %", |s| s.like("%l_o%").into()),
        ("like with an escape", |s| s.like("100\\%").into()),
        ("like of a literal backslash", |s| s.like("a\\\\b").into()),
        ("like with _ over a combining mark", |s| s.like("e_").into()),
        ("like with regex characters", |s| s.like("%[a-z].%").into()),
        ("string_concat", |s| s.string_concat(["!"])),
        ("concat", |s| s.concat(["!"])),
        ("concat with null", |s| s.concat([constant(null())])),
    ];
    for (name, function) in functions {
        let strings = differential
            .pipeline()
            .from(EDGE)
            .filter(|q| q.field("k").not_equal("absent"))
            .add_fields(|_| [function(field("s")).alias("result")])
            .into_pipeline();
        differential
            .same(&format!("{name} of strings"), Expect::Rows, strings)
            .await;
        let everything = differential
            .pipeline()
            .from(EDGE)
            .add_fields(|_| [function(field("s")).alias("result")])
            .into_pipeline();
        differential
            .same(&format!("{name} of every s"), Expect::Failure, everything)
            .await;
    }
    let trailing_backslash = differential.edge_where(
        FirestorePipelineExprBuilder.and([field("k").equal("int"), field("s").like("a\\")]),
    );
    differential
        .same(
            "like with a trailing backslash",
            Expect::Failure,
            trailing_backslash,
        )
        .await;
    let lower_like = differential.edge_where(field("s").to_lower().like("%é%"));
    differential
        .same("like of to_lower", Expect::Failure, lower_like)
        .await;

    differential.finish().await
}

#[tokio::test]
async fn stages_match_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    let sortable = || {
        field("k").not_equal_any(constant(array(vec![
            string("array"),
            string("map"),
            string("geo"),
        ])))
    };

    let cases = [
        (
            "sort by v ascending",
            differential
                .pipeline()
                .from(EDGE)
                .filter(|q| q.condition(sortable()))
                .sort(|e| [e.field("v").asc(), e.field("rank").asc()])
                .into_pipeline(),
        ),
        (
            "sort by v descending",
            differential
                .pipeline()
                .from(EDGE)
                .filter(|q| q.condition(sortable()))
                .sort(|e| [e.field("v").desc(), e.field("rank").desc()])
                .into_pipeline(),
        ),
        (
            "sort by s",
            differential
                .pipeline()
                .from(EDGE)
                .sort(|e| [e.field("s").asc(), e.field("rank").asc()])
                .into_pipeline(),
        ),
        (
            "sort by a computed value",
            differential
                .pipeline()
                .from(EDGE)
                .filter(|q| q.field("rank").less_than_or_equal(8))
                .sort(|e| [e.field("n").multiply(-1).asc(), e.field("rank").asc()])
                .into_pipeline(),
        ),
        (
            "sort, offset and limit",
            differential
                .pipeline()
                .from(EDGE)
                .sort(|e| [e.field("rank").desc()])
                .offset(2)
                .limit(3)
                .into_pipeline(),
        ),
        (
            "offset past the end",
            differential
                .pipeline()
                .from(EDGE)
                .sort(|e| [e.field("rank").asc()])
                .offset(100)
                .into_pipeline(),
        ),
        (
            "fields",
            differential
                .pipeline()
                .from(EDGE)
                .fields(|e| {
                    [
                        FirestoreSelection::from(e.field("v")),
                        e.field("s").alias("text"),
                        e.field("rank").add(100).alias("shifted"),
                        FirestoreSelection::from(e.field("missing")),
                    ]
                })
                .sort(|e| [e.field("shifted").asc()])
                .into_pipeline(),
        ),
        (
            "fields with the name",
            differential
                .pipeline()
                .from(EDGE)
                .fields(|e| [e.field("__name__"), e.field("rank")])
                .sort(|e| [e.field("rank").asc()])
                .into_pipeline(),
        ),
        (
            "fields with the name aliased",
            differential
                .pipeline()
                .from(EDGE)
                .fields(|e| {
                    [
                        e.field("__name__").alias("id"),
                        e.field("rank").alias("rank"),
                    ]
                })
                .sort(|e| [e.field("rank").asc()])
                .into_pipeline(),
        ),
        (
            "where after fields",
            differential
                .pipeline()
                .from(EDGE)
                .fields(|e| [e.field("v").alias("value"), e.field("rank").alias("rank")])
                .filter(|q| q.field("value").greater_than(1))
                .into_pipeline(),
        ),
        (
            "add_fields",
            differential
                .pipeline()
                .from(EDGE)
                .add_fields(|e| {
                    [
                        e.field("rank").multiply(10).alias("rank"),
                        e.field("v").alias("copy"),
                        e.field("missing").alias("s"),
                    ]
                })
                .into_pipeline(),
        ),
        (
            "remove_fields",
            differential
                .pipeline()
                .from(EDGE)
                .remove_fields(["v", "missing"])
                .into_pipeline(),
        ),
    ];
    for (case, pipeline) in cases {
        let rows = if case.starts_with("sort")
            || case.starts_with("limit")
            || case.starts_with("fields")
        {
            Expect::RowsInOrder
        } else {
            Expect::Rows
        };
        differential.same(case, rows, pipeline).await;
    }

    differential.finish().await
}

#[tokio::test]
async fn sources_match_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    let second = document_reference(&differential.db, EDGE, "d02");
    let elsewhere = document_reference(&differential.db, SESSIONS, "s1");
    let edge_path = |id: &str| format!("{EDGE}/{id}");

    let cases = [
        (
            "documents",
            differential
                .pipeline()
                .documents([edge_path("d03"), edge_path("d01"), edge_path("nope")])
                .into_pipeline(),
        ),
        (
            "documents with stages",
            differential
                .pipeline()
                .documents([edge_path("d03"), edge_path("d01")])
                .filter(|q| q.field("v").greater_than(1))
                .into_pipeline(),
        ),
        (
            "a collection under a document",
            differential
                .pipeline()
                .from(CHILDREN)
                .parent(children_parent(&differential.db))
                .filter(|q| q.field("x").greater_than(1))
                .into_pipeline(),
        ),
        (
            "where by name",
            differential.edge_where(field("__name__").equal(constant(second.clone()))),
        ),
        (
            "where by a name in another collection",
            differential.edge_where(field("__name__").equal(constant(elsewhere))),
        ),
        (
            "where by name and value",
            differential.edge_where(FirestorePipelineExprBuilder.and([
                field("__name__").equal_any(constant(array(vec![
                    second,
                    document_reference(&differential.db, EDGE, "d03"),
                    document_reference(&differential.db, EDGE, "nope"),
                ]))),
                field("v").greater_than(1),
            ])),
        ),
    ];
    for (case, pipeline) in cases {
        differential.same(case, Expect::Rows, pipeline).await;
    }
    let ordered = differential
        .pipeline()
        .documents([edge_path("d03"), edge_path("d01"), edge_path("d02")])
        .into_pipeline();
    differential
        .same("documents in the order given", Expect::RowsInOrder, ordered)
        .await;

    differential.finish().await
}

#[tokio::test]
async fn reads_by_name_of_a_lazy_collection_use_what_the_cache_holds() -> TestResult {
    let (db, cache) = setup().await?;
    let through_db = db.read_through_cache(&cache);
    let cached_db = db.read_cached_only(&cache);
    let session = |id: &str| format!("{SESSIONS}/{id}");
    let by_documents = |db: &FirestoreDb, ids: &[&str]| {
        db.fluent()
            .pipeline()
            .select()
            .documents(ids.iter().map(|id| session(id)))
            .into_pipeline()
    };
    let run = |db: FirestoreDb, pipeline: FirestorePipeline| async move {
        let items = db
            .fluent()
            .pipeline()
            .select()
            .from_pipeline(pipeline)
            .stream_query_with_metadata()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        FirestoreResult::Ok(Served::from(items))
    };
    let ids = |served: &Served| match served {
        Served::Firestore(documents) | Served::Cache(documents) => {
            let mut ids: Vec<String> = documents
                .iter()
                .map(|document| document.name.rsplit('/').next().unwrap().to_string())
                .collect();
            ids.sort();
            ids
        }
        Served::Failed(err) => panic!("failed: {err}"),
    };

    let _: Option<FirestoreDocument> = through_db
        .fluent()
        .select()
        .by_id_in(SESSIONS)
        .one("s1")
        .await?;

    let held = run(through_db.clone(), by_documents(&db, &["s1"])).await?;
    assert!(matches!(held, Served::Cache(_)));
    assert_eq!(ids(&held), ["s1"]);

    let partly_held = run(through_db.clone(), by_documents(&db, &["s1", "s2"])).await?;
    assert!(matches!(partly_held, Served::Firestore(_)));
    assert_eq!(ids(&partly_held), ["s1", "s2"]);

    let only_held = run(cached_db.clone(), by_documents(&db, &["s1", "s2"])).await?;
    assert!(matches!(only_held, Served::Cache(_)));
    assert_eq!(ids(&only_held), ["s1"]);

    let by_name = db
        .fluent()
        .pipeline()
        .select()
        .from(SESSIONS)
        .filter(|q| {
            q.field("__name__").eq(FirestoreReference(format!(
                "{}/{}",
                db.get_documents_path(),
                session("s1")
            )))
        })
        .into_pipeline();
    let held_by_name = run(through_db.clone(), by_name).await?;
    assert!(matches!(held_by_name, Served::Cache(_)));
    assert_eq!(ids(&held_by_name), ["s1"]);

    let scan = db
        .fluent()
        .pipeline()
        .select()
        .from(SESSIONS)
        .into_pipeline();
    assert!(matches!(
        run(cached_db, scan.clone()).await,
        Err(FirestoreError::CacheError(_))
    ));
    let scanned = run(through_db, scan).await?;
    assert!(matches!(scanned, Served::Firestore(_)));
    assert_eq!(ids(&scanned), ["s1", "s2", "s3"]);

    cache.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn pipelines_the_cache_does_not_evaluate_go_to_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    let cases = [
        (
            "aggregate",
            differential
                .pipeline()
                .from(EDGE)
                .aggregate(|e| [e.count_all().alias("count")])
                .into_pipeline(),
        ),
        (
            "distinct",
            differential
                .pipeline()
                .from(EDGE)
                .distinct(|e| [e.field("k")])
                .into_pipeline(),
        ),
        (
            "collection group",
            differential
                .pipeline()
                .from(EDGE)
                .all_descendants()
                .into_pipeline(),
        ),
        (
            "literals",
            differential
                .pipeline()
                .literals([serde_json::json!({"a": 1})])
                .into_pipeline(),
        ),
        (
            "force_index",
            differential
                .pipeline()
                .from(EDGE)
                .force_index(FirestoreIndexHint::TableScan)
                .into_pipeline(),
        ),
        (
            "raw stage",
            differential
                .pipeline()
                .from(EDGE)
                .raw_stage(FirestoreRawStage::new("limit".to_string()).with_args(vec![1.into()]))
                .into_pipeline(),
        ),
        (
            "a function the cache does not evaluate",
            differential.edge_where(field("s").regex_contains("^H")),
        ),
        (
            "sort by values of every type",
            differential
                .pipeline()
                .from(EDGE)
                .sort(|e| [e.field("v").asc(), e.field("rank").asc()])
                .into_pipeline(),
        ),
        (
            "limit before sort",
            differential
                .pipeline()
                .from(EDGE)
                .limit(4)
                .sort(|e| [e.field("rank").desc()])
                .into_pipeline(),
        ),
        (
            "limit without sort",
            differential.pipeline().from(EDGE).limit(3).into_pipeline(),
        ),
        (
            "fields with a nested path",
            differential
                .pipeline()
                .from(EDGE)
                .fields(|e| [e.field("v.x"), e.field("rank")])
                .into_pipeline(),
        ),
        (
            "remove_fields of a nested path",
            differential
                .pipeline()
                .from(EDGE)
                .remove_fields(["v.x"])
                .into_pipeline(),
        ),
        (
            "like with an escaped letter",
            differential.edge_where(
                FirestorePipelineExprBuilder
                    .and([field("k").equal("int"), field("s").like("a\\b")]),
            ),
        ),
        (
            "documents that name one twice",
            differential
                .pipeline()
                .documents([format!("{EDGE}/d01"), format!("{EDGE}/d01")])
                .into_pipeline(),
        ),
        (
            "documents that name a collection",
            differential.pipeline().documents([EDGE]).into_pipeline(),
        ),
        (
            "a collection the cache does not hold",
            differential
                .pipeline()
                .from("pipeline-cache-uncached")
                .into_pipeline(),
        ),
    ];
    for (case, pipeline) in cases {
        differential.refused(case, pipeline).await;
    }

    let explained = differential.pipeline().from(EDGE).into_pipeline();
    let cached_db = differential.db.read_cached_only(&differential.cache);
    assert!(matches!(
        cached_db
            .fluent()
            .pipeline()
            .select()
            .from_pipeline(explained.clone())
            .explain()
            .query()
            .await,
        Err(FirestoreError::CacheError(_))
    ));
    let through_items = differential
        .db
        .read_through_cache(&differential.cache)
        .fluent()
        .pipeline()
        .select()
        .from_pipeline(explained)
        .explain()
        .stream_query_with_metadata()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert!(through_items
        .iter()
        .any(|item| item.metadata.explain_stats.is_some()));

    let read_time = FirestoreInstant::now()
        .checked_sub(jiff::SignedDuration::from_secs(5))
        .unwrap();
    let transaction_db = differential.db.clone();
    let transaction = transaction_db.begin_transaction().await?;
    let selectors = [
        (
            "in a transaction",
            FirestoreConsistencySelector::Transaction(transaction.transaction_id().clone()),
        ),
        (
            "at a read time",
            FirestoreConsistencySelector::ReadTime(read_time),
        ),
    ];
    for (case, selector) in selectors {
        let cached_db = differential
            .db
            .read_cached_only(&differential.cache)
            .clone_with_consistency_selector(selector.clone());
        let through_db = differential
            .db
            .read_through_cache(&differential.cache)
            .clone_with_consistency_selector(selector);
        let pipeline = differential.pipeline().from(EDGE).into_pipeline();
        differential
            .refused_by(case, &cached_db, &through_db, pipeline.into())
            .await;
    }
    transaction.rollback().await?;

    differential.finish().await
}

#[cfg(feature = "sql")]
#[tokio::test]
async fn sql_reads_match_firestore() -> TestResult {
    let mut differential = Differential::new().await?;
    let statements = [
        "SELECT * FROM `pipeline-cache-edge` WHERE v > 1",
        "SELECT * FROM `pipeline-cache-edge` WHERE v IS NULL",
        "SELECT * FROM `pipeline-cache-edge` WHERE v IS NOT NULL",
        "SELECT * FROM `pipeline-cache-edge` WHERE v != 1",
        "SELECT * FROM `pipeline-cache-edge` WHERE v IN (1, 'a', 2.5)",
        "SELECT * FROM `pipeline-cache-edge` WHERE n BETWEEN 0 AND 5",
        "SELECT * FROM `pipeline-cache-edge` WHERE NOT (k = 'int') AND rank < 10",
        "SELECT * FROM `pipeline-cache-edge` WHERE __name__ = 'd01'",
        "SELECT * FROM `pipeline-cache-edge` WHERE __name__ IN ('d01', 'd02', 'nope')",
        "SELECT * FROM `pipeline-cache-edge` WHERE k IN ('int', 'double') AND s LIKE 'H%'",
        "SELECT * FROM `pipeline-cache-edge` WHERE k = 'string' AND s ILIKE 'A%'",
        "SELECT * FROM `pipeline-cache-edge` WHERE k = 'int' AND 'a' IN UNNEST(tags)",
        "SELECT s, LOWER(s) AS lower, s || '!' AS shout FROM `pipeline-cache-edge` WHERE k = 'string'",
        "SELECT * EXCEPT (v, tags) FROM `pipeline-cache-edge` WHERE rank <= 3",
        "SELECT __name__, rank FROM `pipeline-cache-edge` ORDER BY rank DESC LIMIT 3 OFFSET 1",
    ];
    for statement in statements {
        let expect = if statement.contains("ORDER BY") {
            Expect::RowsInOrder
        } else {
            Expect::Rows
        };
        differential
            .same(statement, expect, Query::Sql(statement))
            .await;
    }
    // `s` holds a number in one document, and `like` fails on it; `tags` holds a string in one.
    for statement in [
        "SELECT * FROM `pipeline-cache-edge` WHERE s LIKE 'H%'",
        "SELECT * FROM `pipeline-cache-edge` WHERE 'a' IN UNNEST(tags)",
    ] {
        differential
            .same(statement, Expect::Failure, Query::Sql(statement))
            .await;
    }
    for statement in [
        "SELECT k, COUNT(*) AS count FROM `pipeline-cache-edge` GROUP BY k",
        "SELECT DISTINCT k FROM `pipeline-cache-edge`",
    ] {
        differential.refused(statement, Query::Sql(statement)).await;
    }

    differential.finish().await
}
