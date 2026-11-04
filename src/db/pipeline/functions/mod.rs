mod arithmetic;

mod array;
pub use array::*;

mod bitwise;

mod comparison;

mod conditional;

mod generic;

mod logical;

mod map;

mod reference;

mod string;

mod timestamp;
pub use timestamp::*;

mod types;
pub use types::*;

mod vector;

#[cfg(test)]
mod tests {
    use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
    use crate::{
        FirestoreExpr, FirestoreSearchDirection, FirestoreTimestampGranularity,
        FirestoreTimestampPart, FirestoreTimestampUnit, FirestoreValueType,
    };
    use gcloud_sdk::google::firestore::v1::value::ValueType;

    /// Renders an expression as the call it sends: `name(arg, ...)`, a field as `$path`, a
    /// variable as `%name` and a string in quotes.
    fn rendered(expr: &FirestoreExpr) -> String {
        match expr {
            FirestoreExpr::Field(path) => format!("${path}"),
            FirestoreExpr::Variable(name) => format!("%{name}"),
            FirestoreExpr::Constant(value) => match &value.value.value_type {
                Some(ValueType::StringValue(text)) => format!("{text:?}"),
                Some(ValueType::IntegerValue(number)) => number.to_string(),
                Some(ValueType::DoubleValue(number)) => number.to_string(),
                Some(ValueType::NullValue(_)) => "null".to_string(),
                other => format!("{other:?}"),
            },
            FirestoreExpr::Function(function) => format!(
                "{}({})",
                function.name,
                function
                    .args
                    .iter()
                    .map(rendered)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            FirestoreExpr::ArraySubquery(_) => "array(<pipeline>)".to_string(),
            FirestoreExpr::ScalarSubquery(_) => "scalar(<pipeline>)".to_string(),
        }
    }

    fn assert_calls(calls: Vec<FirestoreExpr>, expected: &[&str]) {
        let calls: Vec<String> = calls.iter().map(rendered).collect();
        assert_eq!(calls, expected);
    }

    fn field(path: &str) -> FirestoreExpr {
        FirestoreExpr::field(path)
    }

    #[test]
    fn arithmetic_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("a").add(1),
                field("a").modulo(3),
                field("a").round_with_places(2),
                field("a").trunc(),
                field("a").log_with_base(2),
                field("a").pow(2),
            ],
            &[
                "add($a, 1)",
                "mod($a, 3)",
                "round($a, 2)",
                "trunc($a)",
                "log($a, 2)",
                "pow($a, 2)",
            ],
        );
    }

    #[test]
    fn string_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("s").substring(1),
                field("s").substring_with_length(1, 3),
                field("s").trim_with("x"),
                field("s").split(),
                field("s").split_with_delimiter(","),
                field("s").string_concat(["b", "c"]),
                field("s").like("a%").into(),
                field("s").string_replace_all("a", "b"),
                field("s").join("-"),
                field("s").join_with_null_text("-", "none"),
            ],
            &[
                "substring($s, 1)",
                "substring($s, 1, 3)",
                "trim($s, \"x\")",
                "split($s)",
                "split($s, \",\")",
                "string_concat($s, \"b\", \"c\")",
                "like($s, \"a%\")",
                "string_replace_all($s, \"a\", \"b\")",
                "join($s, \"-\")",
                "join($s, \"-\", \"none\")",
            ],
        );
    }

    #[test]
    fn array_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("a").array_index_of(2, FirestoreSearchDirection::Last),
                field("a").array_slice(1, 2),
                field("a").array_sum(),
                field("a").array_maximum_n(2),
                field("a").array_concat([field("b")]),
                field("a").array_filter("e", |e| e.greater_than(1)),
                field("a").array_transform("e", |e| e.multiply(10)),
            ],
            &[
                "array_index_of($a, 2, \"last\")",
                "array_slice($a, 1, 2)",
                "sum($a)",
                "maximum_n($a, 2)",
                "array_concat($a, $b)",
                "array_filter($a, \"e\", greater_than(%e, 1))",
                "array_transform($a, \"e\", multiply(%e, 10))",
            ],
        );
    }

    #[test]
    fn map_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("m").map_set([("k", 1), ("j", 2)]),
                field("m").map_remove(["k"]),
                field("m").map_merge([field("n")]),
                field("m").get_field("k"),
            ],
            &[
                "map_set($m, \"k\", 1, \"j\", 2)",
                "map_remove($m, \"k\")",
                "map_merge($m, $n)",
                "get_field($m, \"k\")",
            ],
        );
    }

    #[test]
    fn generic_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("a").concat(["b", "c"]),
                field("a").length(),
                field("a").reverse(),
            ],
            &["concat($a, \"b\", \"c\")", "length($a)", "reverse($a)"],
        );
    }

    #[test]
    fn conditional_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("a").cmp(5),
                field("a").is_in(field("b")).into(),
                field("a").is_not_null().into(),
                field("a").is_nan().into(),
                field("a").if_null(0),
                field("a").is_absent().into(),
            ],
            &[
                "cmp($a, 5)",
                "equal_any($a, $b)",
                "and(exists($a), not_equal($a, null))",
                "equal($a, NaN)",
                "if_null($a, 0)",
                "is_absent($a)",
            ],
        );
    }

    #[test]
    fn timestamp_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("t").timestamp_add(FirestoreTimestampUnit::Day, 1),
                field("t").timestamp_diff(field("start"), FirestoreTimestampUnit::Hour),
                field("t").timestamp_trunc(FirestoreTimestampGranularity::WeekStartingMonday),
                field("t").timestamp_extract_with_time_zone(
                    FirestoreTimestampPart::DayOfWeek,
                    "Asia/Tokyo",
                ),
                field("n").unix_millis_to_timestamp(),
            ],
            &[
                "timestamp_add($t, \"day\", 1)",
                "timestamp_diff($t, $start, \"hour\")",
                "timestamp_trunc($t, \"week(monday)\")",
                "timestamp_extract($t, \"dayofweek\", \"Asia/Tokyo\")",
                "unix_millis_to_timestamp($n)",
            ],
        );
    }

    #[test]
    fn type_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("v").type_of(),
                field("v").is_type(FirestoreValueType::GeoPoint).into(),
                field("v").is_type(FirestoreValueType::BsonTimestamp).into(),
            ],
            &[
                "type($v)",
                "is_type($v, \"geo_point\")",
                "is_type($v, \"bson_timestamp\")",
            ],
        );
    }

    #[test]
    fn vector_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("v").cosine_distance(field("w")),
                field("v").vector_length(),
            ],
            &["cosine_distance($v, $w)", "vector_length($v)"],
        );
    }

    #[test]
    fn reference_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![
                field("r").reference_slice(0, 2),
                field("r").parent().collection_id(),
            ],
            &["reference_slice($r, 0, 2)", "collection_id(parent($r))"],
        );
    }

    #[test]
    fn bitwise_calls_use_the_service_names_and_argument_order() {
        assert_calls(
            vec![field("n").bit_not(), field("n").bit_left_shift(2)],
            &["bit_not($n)", "bit_left_shift($n, 2)"],
        );
    }

    #[test]
    fn constructor_calls_use_the_service_names_and_argument_order() {
        let e = FirestorePipelineExprBuilder;

        assert_calls(
            vec![
                e.conditional(field("a").eq(1), "yes", "no"),
                e.switch_on([(field("a").eq(1), "one"), (field("a").eq(2), "two")]),
                e.switch_on_with_default([(field("a").eq(1), "one")], "other"),
                e.coalesce([field("a"), field("b")]),
                e.array([1, 2]),
                e.map([("k", 1)]),
                e.greatest([1, 2]),
                e.least([1, 2]),
                e.current_timestamp(),
                e.current_document(),
                e.rand(),
            ],
            &[
                "conditional(equal($a, 1), \"yes\", \"no\")",
                "switch_on(equal($a, 1), \"one\", equal($a, 2), \"two\")",
                "switch_on(equal($a, 1), \"one\", \"other\")",
                "coalesce($a, $b)",
                "array(1, 2)",
                "map(\"k\", 1)",
                "maximum(1, 2)",
                "minimum(1, 2)",
                "current_timestamp()",
                "current_document()",
                "rand()",
            ],
        );
    }
}
