use crate::common::setup_enterprise;
use firestore::gcloud_sdk::google::firestore::v1::{value::ValueType, Value};
use firestore::pipeline_expr_builder::FirestorePipelineExprBuilder;
use firestore::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BOOKS: &str = "pipeline-books";

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Book {
    title: String,
}

/// A row with a null and a NaN, which a JSON literal cannot hold.
#[derive(Serialize)]
struct SpecialValues {
    number: i64,
    #[serde(with = "firestore::serialize_as_null")]
    nothing: Option<i64>,
    not_a_number: f64,
}

/// The one row that `outputs` computes from the literal `row`; nothing is written.
async fn evaluate<R, FN>(
    row: R,
    outputs: FN,
) -> Result<Json, Box<dyn std::error::Error + Send + Sync>>
where
    R: Serialize,
    FN: FnOnce(FirestorePipelineExprBuilder) -> Vec<FirestoreSelection>,
{
    let db = setup_enterprise().await?;
    let mut rows: Vec<Json> = db
        .fluent()
        .pipeline()
        .literals([row])
        .select(outputs)
        .obj()
        .query()
        .await?;

    assert_eq!(rows.len(), 1);
    Ok(rows.remove(0))
}

fn null() -> FirestoreExpr {
    FirestoreExpr::constant(FirestoreValue::from(Value {
        value_type: Some(ValueType::NullValue(0)),
    }))
}

#[tokio::test]
async fn an_empty_and_keeps_every_document_and_an_empty_or_none() -> TestResult {
    let db = setup_enterprise().await?;

    let every_book: Vec<Book> = db
        .fluent()
        .pipeline()
        .from(BOOKS)
        .filter(|q| q.condition(q.values().and([])))
        .obj()
        .query()
        .await?;
    let no_book: Vec<Book> = db
        .fluent()
        .pipeline()
        .from(BOOKS)
        .filter(|q| q.condition(q.values().or([])))
        .obj()
        .query()
        .await?;

    assert_eq!(every_book.len(), 11);
    assert!(no_book.is_empty());
    Ok(())
}

#[tokio::test]
async fn arithmetic_functions_compute_numbers() -> TestResult {
    let row = json!({
        "a": 7, "b": 2, "one": 1, "zero": 0, "eight": 8, "nine": 9, "thousand": 1000,
        "negative": -3, "price": 2.567, "fraction": 1.8,
    });

    let computed = evaluate(row, |e| {
        vec![
            e.field("a").add(e.field("b")).alias("add"),
            e.field("a").subtract(e.field("b")).alias("subtract"),
            e.field("a").multiply(e.field("b")).alias("multiply"),
            e.field("a").divide(e.field("b")).alias("divide"),
            e.field("a").modulo(e.field("b")).alias("modulo"),
            e.field("negative").abs().alias("abs"),
            e.field("fraction").ceil().alias("ceil"),
            e.field("fraction").floor().alias("floor"),
            e.field("price").round().alias("round"),
            e.field("price")
                .round_with_places(2)
                .alias("round_with_places"),
            e.field("price").trunc().alias("trunc"),
            e.field("price")
                .trunc_with_places(1)
                .alias("trunc_with_places"),
            e.field("a").pow(e.field("b")).alias("pow"),
            e.field("nine").sqrt().alias("sqrt"),
            e.field("zero").exp().alias("exp"),
            e.field("one").ln().alias("ln"),
            e.field("thousand").log10().alias("log10"),
            e.field("one").log().alias("log"),
            e.field("eight").log_with_base(2).alias("log_with_base"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "add": 9, "subtract": 5, "multiply": 14, "divide": 3, "modulo": 1, "abs": 3,
            "ceil": 2.0, "floor": 1.0, "round": 3.0, "round_with_places": 2.57,
            "trunc": 2.0, "trunc_with_places": 2.5, "pow": 49.0, "sqrt": 3.0, "exp": 1.0,
            "ln": 0.0, "log10": 3.0, "log": 0.0, "log_with_base": 3.0,
        })
    );
    Ok(())
}

#[tokio::test]
async fn string_functions_compute_strings() -> TestResult {
    let row = json!({
        "text": "Hello, World", "padded": "  hi  ", "accent": "é", "digits": "ab12cd34",
        "csv": "a,b", "parts": ["a", "b"], "wrapped": "xxaxx",
    });

    let computed = evaluate(row, |e| {
        vec![
            e.field("text").char_length().alias("char_length"),
            e.field("accent").byte_length().alias("byte_length"),
            e.field("text").starts_with("Hello").alias("starts_with"),
            e.field("text").ends_with("World").alias("ends_with"),
            e.field("text")
                .string_contains("lo, W")
                .alias("string_contains"),
            e.field("text").like("Hello%").alias("like"),
            e.field("digits")
                .regex_contains("[0-9]+")
                .alias("regex_contains"),
            e.field("digits").regex_match("[a-z]+").alias("regex_match"),
            e.field("digits").regex_find("[0-9]+").alias("regex_find"),
            e.field("digits")
                .regex_find_all("[0-9]+")
                .alias("regex_find_all"),
            e.field("text")
                .string_concat([" ", "!"])
                .alias("string_concat"),
            e.field("text")
                .string_index_of("o")
                .alias("string_index_of"),
            e.field("text").to_upper().alias("to_upper"),
            e.field("text").to_lower().alias("to_lower"),
            e.field("text").string_reverse().alias("string_reverse"),
            e.field("csv").string_repeat(2).alias("string_repeat"),
            e.field("text")
                .string_replace_all("l", "L")
                .alias("string_replace_all"),
            e.field("text")
                .string_replace_one("l", "L")
                .alias("string_replace_one"),
            e.field("text").substring(7).alias("substring"),
            e.field("text")
                .substring_with_length(0, 5)
                .alias("substring_with_length"),
            e.field("padded").trim().alias("trim"),
            e.field("wrapped").trim_with("x").alias("trim_with"),
            e.field("padded").ltrim().alias("ltrim"),
            e.field("wrapped").ltrim_with("x").alias("ltrim_with"),
            e.field("padded").rtrim().alias("rtrim"),
            e.field("wrapped").rtrim_with("x").alias("rtrim_with"),
            e.field("csv").split().alias("split"),
            e.field("text")
                .split_with_delimiter(", ")
                .alias("split_with_delimiter"),
            e.field("parts").join("-").alias("join"),
            e.field("parts")
                .join_with_null_text("-", "NULL")
                .alias("join_with_null_text"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "char_length": 12, "byte_length": 2, "starts_with": true, "ends_with": true,
            "string_contains": true, "like": true, "regex_contains": true, "regex_match": false,
            "regex_find": "12", "regex_find_all": ["12", "34"], "string_concat": "Hello, World !",
            "string_index_of": 4, "to_upper": "HELLO, WORLD", "to_lower": "hello, world",
            "string_reverse": "dlroW ,olleH", "string_repeat": "a,ba,b",
            "string_replace_all": "HeLLo, WorLd", "string_replace_one": "HeLlo, World",
            "substring": "World", "substring_with_length": "Hello", "trim": "hi",
            "trim_with": "a", "ltrim": "hi  ", "ltrim_with": "axx", "rtrim": "  hi",
            "rtrim_with": "xxa", "split": ["a", "b"], "split_with_delimiter": ["Hello", "World"],
            "join": "a-b", "join_with_null_text": "a-b",
        })
    );
    Ok(())
}

#[tokio::test]
async fn array_functions_compute_arrays() -> TestResult {
    let computed = evaluate(json!({"arr": [1, 2, 3, 2]}), |e| {
        vec![
            e.field("arr").array_concat([vec![9]]).alias("array_concat"),
            e.field("arr").array_contains(3).alias("array_contains"),
            e.field("arr")
                .array_contains_all(vec![1, 2])
                .alias("array_contains_all"),
            e.field("arr")
                .array_contains_any(vec![9, 3])
                .alias("array_contains_any"),
            e.field("arr").array_length().alias("array_length"),
            e.field("arr").array_get(-2).alias("array_get"),
            e.field("arr").offset(0).alias("offset"),
            e.field("arr").array_first().alias("array_first"),
            e.field("arr").array_last().alias("array_last"),
            e.field("arr").array_first_n(2).alias("array_first_n"),
            e.field("arr").array_last_n(2).alias("array_last_n"),
            e.field("arr")
                .array_index_of(2, FirestoreSearchDirection::First)
                .alias("array_index_of_first"),
            e.field("arr")
                .array_index_of(2, FirestoreSearchDirection::Last)
                .alias("array_index_of_last"),
            e.field("arr")
                .array_index_of_all(2)
                .alias("array_index_of_all"),
            e.field("arr").array_reverse().alias("array_reverse"),
            e.field("arr").array_slice(1, 2).alias("array_slice"),
            e.field("arr")
                .array_filter("n", |n| n.greater_than(1))
                .alias("array_filter"),
            e.field("arr")
                .array_transform("n", |n| n.multiply(10))
                .alias("array_transform"),
            e.field("arr").array_sum().alias("array_sum"),
            e.field("arr").array_maximum().alias("array_maximum"),
            e.field("arr").array_minimum().alias("array_minimum"),
            e.field("arr").array_maximum_n(2).alias("array_maximum_n"),
            e.field("arr").array_minimum_n(2).alias("array_minimum_n"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "array_concat": [1, 2, 3, 2, 9], "array_contains": true,
            "array_contains_all": true, "array_contains_any": true, "array_length": 4,
            "array_get": 3, "offset": 1, "array_first": 1, "array_last": 2,
            "array_first_n": [1, 2], "array_last_n": [3, 2], "array_index_of_first": 1,
            "array_index_of_last": 3, "array_index_of_all": [1, 3],
            "array_reverse": [2, 3, 2, 1], "array_slice": [2, 3], "array_filter": [2, 3, 2],
            "array_transform": [10, 20, 30, 20], "array_sum": 8, "array_maximum": 3,
            "array_minimum": 1, "array_maximum_n": [3, 2], "array_minimum_n": [1, 2],
        })
    );
    Ok(())
}

#[tokio::test]
async fn map_functions_compute_maps() -> TestResult {
    let mut computed = evaluate(json!({"m": {"a": 1, "b": 2}}), |e| {
        vec![
            e.field("m").map_get("a").alias("map_get"),
            e.field("m").get_field("b").alias("get_field"),
            e.field("m").map_set([("c", 3)]).alias("map_set"),
            e.field("m").map_remove(["a"]).alias("map_remove"),
            e.field("m")
                .map_merge([e.map([("c", 3)])])
                .alias("map_merge"),
            e.field("m").map_keys().alias("map_keys"),
            e.field("m").map_values().alias("map_values"),
            e.field("m").map_entries().alias("map_entries"),
        ]
    })
    .await?;

    // The request map is built from a HashMap, so the order its keys and values come back
    // in is not fixed; the results are sorted before comparing.
    computed["map_keys"]
        .as_array_mut()
        .expect("map_keys is an array")
        .sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    computed["map_values"]
        .as_array_mut()
        .expect("map_values is an array")
        .sort_by_key(|a| a.as_i64());
    computed["map_entries"]
        .as_array_mut()
        .expect("map_entries is an array")
        .sort_by(|a, b| a["k"].as_str().cmp(&b["k"].as_str()));

    assert_eq!(
        computed,
        json!({
            "map_get": 1, "get_field": 2, "map_set": {"a": 1, "b": 2, "c": 3},
            "map_remove": {"b": 2}, "map_merge": {"a": 1, "b": 2, "c": 3},
            "map_keys": ["a", "b"], "map_values": [1, 2],
            "map_entries": [{"k": "a", "v": 1}, {"k": "b", "v": 2}],
        })
    );
    Ok(())
}

#[tokio::test]
async fn generic_functions_work_on_strings_and_arrays() -> TestResult {
    let computed = evaluate(json!({"text": "ab", "arr": [1, 2]}), |e| {
        vec![
            e.field("text").concat(["cd"]).alias("concat_string"),
            e.field("arr").concat([vec![3]]).alias("concat_array"),
            e.field("text").length().alias("length_string"),
            e.field("arr").length().alias("length_array"),
            e.field("text").reverse().alias("reverse_string"),
            e.field("arr").reverse().alias("reverse_array"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "concat_string": "abcd", "concat_array": [1, 2, 3], "length_string": 2,
            "length_array": 2, "reverse_string": "ba", "reverse_array": [2, 1],
        })
    );
    Ok(())
}

#[tokio::test]
async fn conditional_functions_choose_between_values() -> TestResult {
    let row = json!({"n": 1, "text": "ab"});

    let computed = evaluate(row, |e| {
        vec![
            e.field("n").cmp(5).alias("cmp"),
            e.field("n").equal_any(vec![1, 2]).alias("equal_any"),
            e.field("n").is_in(vec![1, 2]).alias("is_in"),
            e.field("n")
                .not_equal_any(vec![1, 2])
                .alias("not_equal_any"),
            e.field("n").is_not_in(vec![1, 2]).alias("is_not_in"),
            null().if_null(3).alias("if_null"),
            e.field("missing").if_absent(3).alias("if_absent"),
            e.field("n").divide(0).if_error(-1).alias("if_error"),
            e.field("missing").is_absent().alias("is_absent_of_missing"),
            e.field("n").is_absent().alias("is_absent_of_present"),
            e.field("n").divide(0).is_error().alias("is_error"),
            e.field("n").exists().alias("exists_of_present"),
            e.field("missing").exists().alias("exists_of_missing"),
            e.conditional(e.field("n").eq(1), "yes", "no")
                .alias("conditional"),
            e.switch_on([(e.field("n").eq(2), "two"), (e.field("n").eq(1), "one")])
                .alias("switch_on"),
            e.switch_on_with_default([(e.field("n").eq(2), "two")], "other")
                .alias("switch_on_with_default"),
            e.coalesce([e.field("missing"), e.field("text")])
                .alias("coalesce"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "cmp": -1, "equal_any": true, "is_in": true, "not_equal_any": false,
            "is_not_in": false, "if_null": 3, "if_absent": 3, "if_error": -1,
            "is_absent_of_missing": true, "is_absent_of_present": false, "is_error": true,
            "exists_of_present": true, "exists_of_missing": false, "conditional": "yes",
            "switch_on": "one", "switch_on_with_default": "other", "coalesce": "ab",
        })
    );
    Ok(())
}

#[tokio::test]
async fn null_and_nan_tests_tell_a_value_from_an_absent_field() -> TestResult {
    let row = SpecialValues {
        number: 1,
        nothing: None,
        not_a_number: f64::NAN,
    };

    let computed = evaluate(row, |e| {
        vec![
            e.field("nothing").is_null().alias("null_is_null"),
            e.field("number").is_null().alias("number_is_null"),
            e.field("missing").is_null().alias("missing_is_null"),
            e.field("nothing").is_not_null().alias("null_is_not_null"),
            e.field("number").is_not_null().alias("number_is_not_null"),
            e.field("missing")
                .is_not_null()
                .alias("missing_is_not_null"),
            e.field("not_a_number").is_nan().alias("nan_is_nan"),
            e.field("number").is_nan().alias("number_is_nan"),
            e.field("not_a_number").is_not_nan().alias("nan_is_not_nan"),
            e.field("number").is_not_nan().alias("number_is_not_nan"),
            e.field("missing").is_not_nan().alias("missing_is_not_nan"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "null_is_null": true, "number_is_null": false, "missing_is_null": false,
            "null_is_not_null": false, "number_is_not_null": true, "missing_is_not_null": false,
            "nan_is_nan": true, "number_is_nan": false, "nan_is_not_nan": false,
            "number_is_not_nan": true, "missing_is_not_nan": false,
        })
    );
    Ok(())
}

#[tokio::test]
async fn constructors_build_documents_and_values() -> TestResult {
    let row = json!({"a": 1, "b": 2});

    let computed = evaluate(row.clone(), |e| {
        vec![
            e.array([e.field("a"), e.field("b")]).alias("array"),
            e.map([("sum", e.field("a").add(e.field("b")))])
                .alias("map"),
            e.greatest([e.field("a"), e.field("b")]).alias("greatest"),
            e.least([e.field("a"), e.field("b")]).alias("least"),
            e.current_document().alias("current_document"),
            e.current_timestamp()
                .type_of()
                .alias("current_timestamp_type"),
            e.rand()
                .greater_than_or_equal(0.0)
                .alias("rand_at_least_zero"),
            e.rand().less_than(1.0).alias("rand_below_one"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "array": [1, 2], "map": {"sum": 3}, "greatest": 2, "least": 1,
            "current_document": row, "current_timestamp_type": "timestamp",
            "rand_at_least_zero": true, "rand_below_one": true,
        })
    );
    Ok(())
}

#[tokio::test]
async fn timestamp_functions_compute_timestamps() -> TestResult {
    let row = json!({"seconds": 100000, "micros": 5000000, "millis": 5000});

    let computed = evaluate(row, |e| {
        let timestamp = || e.field("seconds").unix_seconds_to_timestamp();
        vec![
            timestamp()
                .timestamp_add(FirestoreTimestampUnit::Day, 1)
                .alias("timestamp_add"),
            timestamp()
                .timestamp_subtract(FirestoreTimestampUnit::Hour, 3)
                .alias("timestamp_subtract"),
            timestamp()
                .timestamp_diff(
                    e.constant(0).unix_seconds_to_timestamp(),
                    FirestoreTimestampUnit::Hour,
                )
                .alias("timestamp_diff"),
            timestamp()
                .timestamp_trunc(FirestoreTimestampGranularity::Day)
                .alias("timestamp_trunc"),
            timestamp()
                .timestamp_trunc(FirestoreTimestampGranularity::WeekStartingMonday)
                .alias("timestamp_trunc_week"),
            timestamp()
                .timestamp_trunc_with_time_zone(FirestoreTimestampGranularity::Day, "Asia/Tokyo")
                .alias("timestamp_trunc_with_time_zone"),
            timestamp()
                .timestamp_extract(FirestoreTimestampPart::DayOfWeek)
                .alias("timestamp_extract"),
            timestamp()
                .timestamp_extract_with_time_zone(FirestoreTimestampPart::Hour, "Asia/Tokyo")
                .alias("timestamp_extract_with_time_zone"),
            timestamp()
                .timestamp_to_unix_micros()
                .alias("timestamp_to_unix_micros"),
            timestamp()
                .timestamp_to_unix_millis()
                .alias("timestamp_to_unix_millis"),
            timestamp()
                .timestamp_to_unix_seconds()
                .alias("timestamp_to_unix_seconds"),
            e.field("micros")
                .unix_micros_to_timestamp()
                .alias("unix_micros_to_timestamp"),
            e.field("millis")
                .unix_millis_to_timestamp()
                .alias("unix_millis_to_timestamp"),
            timestamp().alias("unix_seconds_to_timestamp"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "timestamp_add": "1970-01-03T03:46:40Z",
            "timestamp_subtract": "1970-01-02T00:46:40Z",
            "timestamp_diff": 27,
            "timestamp_trunc": "1970-01-02T00:00:00Z",
            "timestamp_trunc_week": "1969-12-29T00:00:00Z",
            "timestamp_trunc_with_time_zone": "1970-01-01T15:00:00Z",
            "timestamp_extract": 6,
            "timestamp_extract_with_time_zone": 12,
            "timestamp_to_unix_micros": 100_000_000_000_i64,
            "timestamp_to_unix_millis": 100_000_000,
            "timestamp_to_unix_seconds": 100000,
            "unix_micros_to_timestamp": "1970-01-01T00:00:05Z",
            "unix_millis_to_timestamp": "1970-01-01T00:00:05Z",
            "unix_seconds_to_timestamp": "1970-01-02T03:46:40Z",
        })
    );
    Ok(())
}

#[tokio::test]
async fn type_functions_name_and_test_value_types() -> TestResult {
    let computed = evaluate(json!({"n": 1, "text": "ab", "arr": [1]}), |e| {
        vec![
            e.field("n").type_of().alias("type_of"),
            e.field("n")
                .is_type(FirestoreValueType::Int64)
                .alias("is_int64"),
            e.field("n")
                .is_type(FirestoreValueType::Number)
                .alias("is_number"),
            e.field("text")
                .is_type(FirestoreValueType::Number)
                .alias("text_is_number"),
            e.field("arr")
                .is_type(FirestoreValueType::Array)
                .alias("is_array"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "type_of": "int64", "is_int64": true, "is_number": true, "text_is_number": false,
            "is_array": true,
        })
    );
    Ok(())
}

#[tokio::test]
async fn vector_functions_compare_vectors() -> TestResult {
    let computed = evaluate(json!({}), |e| {
        let vector = |components: [f64; 2]| e.constant(FirestoreVector::new(components.to_vec()));
        vec![
            vector([1.0, 0.0])
                .cosine_distance(vector([0.0, 1.0]))
                .alias("cosine_distance"),
            vector([1.0, 2.0])
                .dot_product(vector([3.0, 4.0]))
                .alias("dot_product"),
            vector([0.0, 0.0])
                .euclidean_distance(vector([3.0, 4.0]))
                .alias("euclidean_distance"),
            vector([1.0, 0.0]).vector_length().alias("vector_length"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "cosine_distance": 1.0, "dot_product": 11.0, "euclidean_distance": 5.0,
            "vector_length": 2,
        })
    );
    Ok(())
}

#[tokio::test]
async fn reference_functions_take_apart_a_document_reference() -> TestResult {
    let db = setup_enterprise().await?;
    let documents = db.get_documents_path().clone();
    let book = format!("{documents}/{BOOKS}/book-01");

    let computed = evaluate(json!({}), |e| {
        let reference = || e.constant(FirestoreReference::new(book.clone()));
        vec![
            reference().document_id().alias("document_id"),
            reference().collection_id().alias("collection_id"),
            reference().parent().alias("parent"),
            reference().reference_slice(0, 2).alias("reference_slice"),
        ]
    })
    .await?;

    assert_eq!(
        computed,
        json!({
            "document_id": "book-01", "collection_id": BOOKS, "parent": documents,
            "reference_slice": book,
        })
    );
    Ok(())
}
