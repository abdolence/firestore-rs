use crate::errors::{
    FirestoreError, FirestoreInvalidParametersPublicDetails, FirestoreSqlLocation,
};
use crate::pipeline_builder::FirestorePipelineInitialBuilder;
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreBoolExpr, FirestoreCollectionGroupSource, FirestoreCollectionSource,
    FirestoreDocumentsSource, FirestoreExpr, FirestorePipeline, FirestorePipelineSource,
    FirestorePipelineStage, FirestoreReference, FirestoreResult, FirestoreSelection,
    FirestoreSqlOptions, FirestoreSqlParameters, FirestoreSqlStatement,
};

use serde_json::json;
use std::collections::HashMap;

const DOCUMENTS: &str = "projects/latestbit/databases/firestore-rs-enterprise/documents";

fn translate(
    sql: &str,
    parameters: &FirestoreSqlParameters,
    options: &FirestoreSqlOptions,
) -> FirestoreResult<FirestorePipeline> {
    match FirestoreSqlStatement::translate_on(DOCUMENTS, sql, parameters, options)? {
        FirestoreSqlStatement::Query(pipeline) => Ok(pipeline),
    }
}

fn translated(sql: &str) -> FirestorePipeline {
    translate(
        sql,
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::new(),
    )
    .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn refused_with(
    sql: &str,
    parameters: &FirestoreSqlParameters,
    options: &FirestoreSqlOptions,
) -> FirestoreInvalidParametersPublicDetails {
    match translate(sql, parameters, options) {
        Err(FirestoreError::InvalidParametersError(error)) => error.public,
        other => panic!("{sql}: {other:?}"),
    }
}

fn refused(sql: &str) -> FirestoreInvalidParametersPublicDetails {
    refused_with(
        sql,
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::new(),
    )
}

/// The condition of the one `WHERE` the statement translates to.
fn condition(sql: &str) -> FirestoreExpr {
    match translated(sql).stages.as_slice() {
        [FirestorePipelineStage::Where(stage)] => stage.condition.clone().into(),
        stages => panic!("{sql}: {stages:?}"),
    }
}

/// The expression of the one column the statement selects.
fn column(sql: &str) -> FirestoreExpr {
    match translated(sql).stages.as_slice() {
        [FirestorePipelineStage::Fields(stage)] => match stage.fields.as_slice() {
            [selection] => selection.expr.clone(),
            fields => panic!("{sql}: {fields:?}"),
        },
        stages => panic!("{sql}: {stages:?}"),
    }
}

fn fluent() -> FirestorePipelineInitialBuilder<()> {
    FirestorePipelineInitialBuilder::new(())
}

fn field(path: &str) -> FirestoreExpr {
    FirestoreExpr::field(path)
}

fn reference(path: &str) -> FirestoreExpr {
    FirestoreExpr::constant(FirestoreReference(format!("{DOCUMENTS}/{path}")))
}

fn location(line: u64, column: u64) -> Option<FirestoreSqlLocation> {
    Some(FirestoreSqlLocation::new(line, column))
}

#[test]
fn a_select_translates_to_the_fluent_pipeline() {
    let sql = "SELECT title, author AS writer FROM `pipeline-books` \
               WHERE genre = 'Dystopian' AND rating >= 4.0 \
               ORDER BY rating DESC LIMIT 3 OFFSET 1";

    let expected = fluent()
        .from("pipeline-books")
        .filter(|q| {
            q.for_all([
                q.field("genre").eq("Dystopian"),
                q.field("rating").greater_than_or_equal(4.0),
            ])
        })
        .sort(|e| [e.field("rating").desc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("title")),
                e.field("author").alias("writer"),
            ]
        })
        .offset(1)
        .limit(3)
        .into_pipeline();

    assert_eq!(translated(sql), expected);
}

#[test]
fn sources_translate_to_their_pipeline_sources() {
    let sources = [
        (
            // sqlparser splits a backticked name at its dots, which belong to the ID here.
            "SELECT * FROM `restaurants/r.1/reviews`",
            FirestorePipelineSource::Collection(
                FirestoreCollectionSource::new("reviews".to_string())
                    .with_parent(format!("{DOCUMENTS}/restaurants/r.1")),
            ),
        ),
        (
            "SELECT * FROM collection_group('reviews')",
            FirestorePipelineSource::CollectionGroup(FirestoreCollectionGroupSource::new(
                "reviews".to_string(),
            )),
        ),
        (
            "SELECT * FROM documents('books/b1', 'books/b2')",
            FirestorePipelineSource::Documents(FirestoreDocumentsSource::new(vec![
                "books/b1".to_string(),
                "books/b2".to_string(),
            ])),
        ),
        (
            "SELECT * FROM database()",
            FirestorePipelineSource::Database,
        ),
    ];

    for (sql, source) in sources {
        assert_eq!(translated(sql), FirestorePipeline::new(source), "{sql}");
    }
}

#[test]
fn an_id_only_filter_reads_the_documents_by_path() {
    assert_eq!(
        translated("SELECT title FROM books WHERE __name__ = 'b1'"),
        fluent()
            .documents(["books/b1"])
            .fields(|e| [e.field("title")])
            .into_pipeline()
    );
    assert_eq!(
        translated("SELECT * FROM books WHERE __name__ IN ('b1', 'books/b2', 'b1')"),
        fluent().documents(["books/b1", "books/b2"]).into_pipeline()
    );
}

#[test]
fn a_name_compared_with_a_string_compares_a_reference() {
    let values = FirestorePipelineExprBuilder;
    let full_path = format!(
        "SELECT * FROM collection_group('reviews') WHERE '{DOCUMENTS}/r/r1/reviews/v1' < __name__"
    );
    let compared = [
        (
            "SELECT * FROM books WHERE __name__ = 'b1' AND stock > 0",
            values
                .and([
                    field("__name__").equal(reference("books/b1")),
                    field("stock").greater_than(0),
                ])
                .into(),
        ),
        (
            "SELECT * FROM books WHERE __name__ = 'archive/b1'",
            field("__name__").equal(reference("archive/b1")).into(),
        ),
        (
            full_path.as_str(),
            reference("r/r1/reviews/v1")
                .less_than(field("__name__"))
                .into(),
        ),
    ];

    for (sql, expected) in compared {
        assert_eq!(condition(sql), expected, "{sql}");
    }
}

#[test]
fn parameters_bind_by_name_and_by_position_in_text_order() {
    let named = translate(
        "SELECT title FROM books WHERE genre = @genre",
        &FirestoreSqlParameters::new().param("genre", "Dystopian"),
        &FirestoreSqlOptions::new(),
    );
    // The `?` of the projection comes first in the text, though the filter translates first.
    let positional = translate(
        "SELECT ? AS label FROM books WHERE stock > ? LIMIT ?",
        &FirestoreSqlParameters::new().params([json!("low"), json!(5), json!(2)]),
        &FirestoreSqlOptions::new(),
    );

    assert_eq!(
        named.unwrap(),
        fluent()
            .from("books")
            .filter(|q| q.field("genre").eq("Dystopian"))
            .fields(|e| [e.field("title")])
            .into_pipeline()
    );
    assert_eq!(
        positional.unwrap(),
        fluent()
            .from("books")
            .filter(|q| q.field("stock").greater_than(5))
            .fields(|e| [e.constant("low").alias("label")])
            .limit(2)
            .into_pipeline()
    );
}

#[test]
fn parameters_must_match_their_bindings_both_ways() {
    let options = FirestoreSqlOptions::new();
    let mismatched = [
        (
            "SELECT * FROM books WHERE genre = @genre",
            FirestoreSqlParameters::new(),
            "@genre",
            location(1, 35),
        ),
        (
            "SELECT * FROM books",
            FirestoreSqlParameters::new().param("genre", "Dystopian"),
            "@genre",
            None,
        ),
        (
            "SELECT * FROM books WHERE genre = @genre AND stock > ?",
            FirestoreSqlParameters::new()
                .param("genre", "Dystopian")
                .params([1]),
            "?",
            location(1, 54),
        ),
        (
            "SELECT * FROM books WHERE stock > $1",
            FirestoreSqlParameters::new(),
            "$1",
            location(1, 35),
        ),
        (
            "SELECT * FROM books WHERE flags = @flags",
            FirestoreSqlParameters::new().param("flags", HashMap::from([(true, 1)])),
            "@flags",
            location(1, 35),
        ),
        (
            "SELECT * FROM books WHERE stock > ? AND pages > ?",
            FirestoreSqlParameters::new().params([1]),
            "?",
            location(1, 49),
        ),
    ];

    for (sql, parameters, field, location) in mismatched {
        let refusal = refused_with(sql, &parameters, &options);
        assert_eq!(
            (refusal.field.as_str(), refusal.location),
            (field, location),
            "{sql}"
        );
    }
}

#[test]
fn text_must_hold_exactly_one_statement() {
    for sql in ["", "SELECT * FROM books; SELECT * FROM reviews"] {
        assert_eq!(refused(sql).field, "sql", "{sql:?}");
    }
}

#[test]
fn text_that_does_not_parse_carries_sqlparser_s_location() {
    let unparsed = [
        ("SELECT * FROM books WHERE stock = = 1", location(1, 35)),
        ("SELECT * FROM books\nWHERE title = 'Dune", location(2, 16)),
    ];

    for (sql, location) in unparsed {
        let refusal = refused(sql);
        assert_eq!(
            (refusal.field.as_str(), refusal.location),
            ("sql", location),
            "{sql:?}"
        );
    }
}

#[test]
fn an_unmapped_table_option_is_refused_rather_than_dropped() {
    let refusal = refused("SELECT * FROM books TABLESAMPLE SYSTEM (10 PERCENT)");

    assert_eq!(
        (refusal.field.as_str(), refusal.location),
        ("TABLESAMPLE", location(1, 15))
    );
}

#[test]
fn constructs_without_a_translation_are_refused_by_name() {
    let refusals = [
        ("SELECT genre FROM books GROUP BY genre", "GROUP BY"),
        ("SELECT DISTINCT genre FROM books", "DISTINCT"),
        (
            "SELECT title FROM books UNION ALL SELECT title FROM archive",
            "UNION",
        ),
        (
            "SELECT b.title FROM books AS b JOIN reviews AS r ON r.book = b.__name__",
            "JOIN",
        ),
        ("SELECT * FROM books, reviews", "JOIN"),
        (
            "WITH recent AS (SELECT * FROM books) SELECT * FROM recent",
            "WITH",
        ),
        (
            "SELECT * FROM books WHERE stock = (SELECT stock FROM archive)",
            "a subquery",
        ),
        ("SELECT * FROM books WHERE 'sf' IN UNNEST(tags)", "UNNEST"),
        (
            "SELECT COUNT(*) AS n FROM books",
            "the aggregate function count",
        ),
        (
            "SELECT ROW_NUMBER() OVER () AS n FROM books",
            "a window function",
        ),
        ("SELECT CAST(stock AS STRING) AS s FROM books", "CAST"),
        (
            "SELECT * FROM books WHERE document_matches('dune')",
            "search",
        ),
        (
            "SELECT * FROM books WHERE title LIKE 'a!%' ESCAPE '!'",
            "ESCAPE",
        ),
        ("FROM books |> WHERE stock = 0", "pipe syntax"),
        ("DELETE FROM books WHERE stock = 0", "DELETE"),
        ("SELECT LOWER(title) FROM books", "AS"),
        ("SELECT title FROM books ORDER BY 1", "ORDER BY"),
        ("SELECT 1 AS one", "FROM"),
        ("SELECT * FROM @books", "@books"),
        ("SELECT * FROM `books/b1`", "collection"),
    ];

    for (sql, construct) in refusals {
        assert_eq!(refused(sql).field, construct, "{sql}");
    }
}

#[test]
fn options_refuse_what_they_guard() {
    let parameters = FirestoreSqlParameters::new();
    let refusals = [
        (
            "DELETE FROM books WHERE stock = 0",
            FirestoreSqlOptions::read_only(),
            "read_only",
        ),
        (
            "SELECT * FROM database()",
            FirestoreSqlOptions::read_only(),
            "database",
        ),
        (
            "SELECT * FROM collection_group('reviews')",
            FirestoreSqlOptions::new().refuse_collection_groups(),
            "collection_group",
        ),
        (
            "SELECT * FROM `secrets/s1/books`",
            FirestoreSqlOptions::new().with_collections(["books"]),
            "collections",
        ),
        (
            "SELECT * FROM documents('secrets/s1')",
            FirestoreSqlOptions::new().with_collections(["books"]),
            "collections",
        ),
        (
            "SELECT * FROM database()",
            FirestoreSqlOptions::new().with_collections(["books"]),
            "database",
        ),
        (
            "SELECT * FROM books",
            FirestoreSqlOptions::new().with_max_length(10),
            "max_length",
        ),
    ];

    for (sql, options, field) in refusals {
        assert_eq!(
            refused_with(sql, &parameters, &options).field,
            field,
            "{sql}"
        );
    }
}

#[test]
fn the_row_cap_lowers_a_limit_or_adds_one() {
    let options = FirestoreSqlOptions::read_only().with_max_rows(5);
    let capped = [
        ("SELECT * FROM books LIMIT 10", 5),
        ("SELECT * FROM books LIMIT 3", 3),
        ("SELECT title FROM books", 5),
    ];

    for (sql, limit) in capped {
        let pipeline = translate(sql, &FirestoreSqlParameters::new(), &options).unwrap();
        assert!(
            matches!(
                pipeline.stages.last(),
                Some(FirestorePipelineStage::Limit(stage)) if stage.limit == limit
            ),
            "{sql}: {pipeline:?}"
        );
    }
}

#[test]
fn deep_nesting_is_refused_without_overflowing_a_2_mib_stack() {
    let nested = [
        format!("SELECT * FROM books WHERE {}stock = 0", "NOT ".repeat(1000)),
        format!(
            "SELECT {}title{} AS t FROM books",
            "lower(".repeat(1000),
            ")".repeat(1000)
        ),
        format!(
            "SELECT * FROM books WHERE {}stock{} = 0",
            "(".repeat(1000),
            ")".repeat(1000)
        ),
        format!(
            "SELECT * FROM books WHERE stock = {}1{}",
            "(SELECT ".repeat(200),
            ")".repeat(200)
        ),
        format!(
            "SELECT * FROM books WHERE stock = {}",
            vec!["1"; 8000].join("+")
        ),
    ];
    let conjuncts = vec!["stock = 1"; 1000].join(" AND ");

    // `refused` panics for a statement that translates, and an overflow aborts the process.
    let flattened = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            for sql in &nested {
                refused(sql);
            }
            translate(
                &format!("SELECT * FROM books WHERE {conjuncts}"),
                &FirestoreSqlParameters::new(),
                &FirestoreSqlOptions::new(),
            )
            .is_ok()
        })
        .unwrap()
        .join()
        .unwrap();

    assert!(flattened);
}

#[test]
fn predicates_translate_to_pipeline_functions() {
    let values = FirestorePipelineExprBuilder;
    let predicates = [
        ("stock IS NULL", field("stock").is_null().into()),
        ("stock IS NOT NULL", field("stock").is_not_null().into()),
        ("stock IS TRUE", field("stock").equal(true).into()),
        (
            "stock IS NOT DISTINCT FROM pages",
            field("stock").equal(field("pages")).into(),
        ),
        ("stock <> 1", field("stock").not_equal(1).into()),
        (
            "stock IN (1, 2)",
            field("stock").equal_any(vec![1, 2]).into(),
        ),
        (
            "stock NOT IN (1, pages)",
            field("stock")
                .not_equal_any(values.array([FirestoreExpr::constant(1), field("pages")]))
                .into(),
        ),
        (
            "stock BETWEEN 1 AND 2",
            values
                .and([
                    field("stock").greater_than_or_equal(1),
                    field("stock").less_than_or_equal(2),
                ])
                .into(),
        ),
        (
            "stock NOT BETWEEN 1 AND 2",
            values
                .or([field("stock").less_than(1), field("stock").greater_than(2)])
                .into(),
        ),
        (
            "title NOT LIKE 'a%'",
            values.not(field("title").like("a%")).into(),
        ),
        (
            "title ILIKE 'a%'",
            field("title")
                .to_lower()
                .like(FirestoreExpr::constant("a%").to_lower())
                .into(),
        ),
        (
            "NOT sold_out OR reprinted OR signed",
            values
                .or([
                    values.not(FirestoreBoolExpr::unchecked(field("sold_out"))),
                    FirestoreBoolExpr::unchecked(field("reprinted")),
                    FirestoreBoolExpr::unchecked(field("signed")),
                ])
                .into(),
        ),
    ];

    for (predicate, expected) in predicates {
        let sql = format!("SELECT * FROM books WHERE {predicate}");
        assert_eq!(condition(&sql), expected, "{predicate}");
    }
}

#[test]
fn values_translate_to_pipeline_values() {
    let values = FirestorePipelineExprBuilder;
    let translated_values = [
        ("-9223372036854775808", FirestoreExpr::constant(i64::MIN)),
        ("1.5e3", FirestoreExpr::constant(1500.0)),
        (
            "\"double quoted\"",
            FirestoreExpr::constant("double quoted"),
        ),
        ("NULL", FirestoreExpr::constant(None::<bool>)),
        ("[1, 2]", FirestoreExpr::constant(vec![1, 2])),
        (
            "-stock",
            FirestoreExpr::constant(0).subtract(field("stock")),
        ),
        ("stock % 2", field("stock").modulo(2)),
        (
            "'[' || title || ']'",
            FirestoreExpr::constant("[").concat([field("title"), FirestoreExpr::constant("]")]),
        ),
        (
            "CASE WHEN stock > 1 THEN 'many' END",
            values.switch_on_with_default([(field("stock").greater_than(1), "many")], None::<bool>),
        ),
        (
            "CASE stock WHEN 1 THEN 'one' ELSE 'other' END",
            values.switch_on_with_default([(field("stock").equal(1), "one")], "other"),
        ),
    ];

    for (value, expected) in translated_values {
        let sql = format!("SELECT {value} AS v FROM books");
        assert_eq!(column(&sql), expected, "{value}");
    }
}

#[test]
fn functions_pass_through_or_follow_the_alias_table() {
    let values = FirestorePipelineExprBuilder;
    let calls = [
        (
            "FROBNICATE(title)",
            FirestoreExpr::raw("frobnicate", [field("title")]),
        ),
        (
            "CURRENT_TIMESTAMP()",
            FirestoreExpr::raw("current_timestamp", Vec::<FirestoreExpr>::new()),
        ),
        (
            "COALESCE(rating, 1)",
            values.coalesce([field("rating"), FirestoreExpr::constant(1)]),
        ),
        ("LOWER(title)", field("title").to_lower()),
        (
            "STRPOS(title, 'a')",
            field("title").string_index_of("a").add(1),
        ),
        (
            "SUBSTR(title, 2, 3)",
            field("title").substring_with_length(1, 3),
        ),
        (
            "SUBSTRING(title FROM pages)",
            field("title").substring(field("pages").subtract(1)),
        ),
        ("TRIM(title)", field("title").trim()),
        (
            "TRIM(LEADING 'a' FROM title)",
            field("title").ltrim_with("a"),
        ),
        ("CEIL(rating)", field("rating").ceil()),
    ];

    for (call, expected) in calls {
        let sql = format!("SELECT {call} AS v FROM books");
        assert_eq!(column(&sql), expected, "{call}");
    }
}

#[test]
fn columns_resolve_to_field_paths() {
    let columns = [
        ("SELECT b.title AS v FROM books AS b", field("title")),
        ("SELECT books.title AS v FROM books", field("title")),
        ("SELECT awards.hugo AS v FROM books", field("awards.hugo")),
        ("SELECT `first.last` AS v FROM books", field("`first.last`")),
        (
            "SELECT awards['nebula'] AS v FROM books",
            field("awards").map_get("nebula"),
        ),
        (
            "SELECT tags[OFFSET(0)] AS v FROM books",
            field("tags").array_get(0),
        ),
        (
            "SELECT tags[ORDINAL(1)] AS v FROM books",
            field("tags").array_get(0),
        ),
        (
            "SELECT awards.history[0].year AS v FROM books",
            field("awards.history").array_get(0).map_get("year"),
        ),
    ];

    for (sql, expected) in columns {
        assert_eq!(column(sql), expected, "{sql}");
    }
}

#[test]
fn order_by_resolves_output_names_and_moves_nulls() {
    let values = FirestorePipelineExprBuilder;
    let score = || field("rating").multiply(2);
    let missing = |expr: FirestoreExpr| {
        FirestoreExpr::from(values.or([expr.clone().is_absent(), expr.is_null()]))
    };

    let pipeline = translated(
        "SELECT rating * 2 AS score FROM books ORDER BY score DESC NULLS FIRST, title NULLS LAST",
    );

    assert_eq!(
        pipeline,
        fluent()
            .from("books")
            .sort(|_| {
                [
                    missing(score()).desc(),
                    score().desc(),
                    missing(field("title")).asc(),
                    field("title").asc(),
                ]
            })
            .fields(|_| [score().alias("score")])
            .into_pipeline()
    );
}

#[test]
fn wildcards_keep_every_field() {
    let wildcards = [
        (
            "SELECT * FROM books",
            fluent().from("books").into_pipeline(),
        ),
        (
            "SELECT b.* FROM books AS b",
            fluent().from("books").into_pipeline(),
        ),
        (
            "SELECT * EXCEPT (embedding, tags) FROM books",
            fluent()
                .from("books")
                .remove_fields(["embedding", "tags"])
                .into_pipeline(),
        ),
        (
            "SELECT *, rating * 2 AS score FROM books",
            fluent()
                .from("books")
                .add_fields(|e| [e.field("rating").multiply(2).alias("score")])
                .into_pipeline(),
        ),
    ];

    for (sql, expected) in wildcards {
        assert_eq!(translated(sql), expected, "{sql}");
    }
}
