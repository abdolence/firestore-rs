use crate::errors::{
    FirestoreError, FirestoreInvalidParametersPublicDetails, FirestoreSqlLocation,
};
use crate::pipeline_builder::FirestorePipelineInitialBuilder;
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::pipeline_stage_builder::FirestorePipelineNestedContext;
use crate::{
    FirestoreBoolExpr, FirestoreCollectionGroupSource, FirestoreCollectionId,
    FirestoreCollectionSource, FirestoreDocumentsSource, FirestoreExpr, FirestoreGeoPoint,
    FirestoreInsertWrite, FirestoreLatLng, FirestoreListCollectionIdsParams, FirestorePipeline,
    FirestorePipelineSource, FirestorePipelineStage, FirestorePipelineWrite,
    FirestorePipelineWriteParams, FirestorePipelineWriteTransaction, FirestoreReference,
    FirestoreResult, FirestoreSelection, FirestoreSqlOptions, FirestoreSqlParameters,
    FirestoreSqlStatement, FirestoreUnionStage, FirestoreUpdateWrite, FirestoreUpsertWrite,
    FirestoreWriteCollection,
};

use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;

const DOCUMENTS: &str = "projects/latestbit/databases/firestore-rs-enterprise/documents";

fn translate_statement(
    sql: &str,
    parameters: &FirestoreSqlParameters,
    options: &FirestoreSqlOptions,
) -> FirestoreResult<FirestoreSqlStatement> {
    FirestoreSqlStatement::translate_on(DOCUMENTS, sql, parameters, options)
}

fn translate(
    sql: &str,
    parameters: &FirestoreSqlParameters,
    options: &FirestoreSqlOptions,
) -> FirestoreResult<FirestorePipeline> {
    match translate_statement(sql, parameters, options)? {
        FirestoreSqlStatement::Query(pipeline) => Ok(pipeline),
        other => panic!("{sql}: {other:?}"),
    }
}

fn translated_write(sql: &str) -> FirestorePipelineWriteParams {
    translated_write_with(
        sql,
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::new(),
    )
}

fn translated_write_with(
    sql: &str,
    parameters: &FirestoreSqlParameters,
    options: &FirestoreSqlOptions,
) -> FirestorePipelineWriteParams {
    match translate_statement(sql, parameters, options) {
        Ok(FirestoreSqlStatement::Write(write)) => *write,
        other => panic!("{sql}: {other:?}"),
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
    match translate_statement(sql, parameters, options) {
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
            "SELECT * FROM books WHERE __name__ IN ('b1', 'books/b2')",
            field("__name__")
                .equal_any(vec![
                    FirestoreReference(format!("{DOCUMENTS}/books/b1")),
                    FirestoreReference(format!("{DOCUMENTS}/books/b2")),
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
fn a_refused_concatenation_reports_its_first_operand() {
    let refusal = refused("SELECT title || author AS x FROM books GROUP BY genre");

    assert_eq!(
        (refusal.field.as_str(), refusal.location),
        ("GROUP BY", location(1, 8))
    );
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
        (
            "SELECT b.title FROM books AS b JOIN reviews AS r ON r.book = b.__name__",
            "JOIN",
        ),
        (
            "SELECT ROW_NUMBER() OVER () AS n FROM books",
            "a window function",
        ),
        ("SELECT CAST(stock AS STRING) AS s FROM books", "CAST"),
        (
            "SELECT * FROM books WHERE title LIKE 'a!%' ESCAPE '!'",
            "ESCAPE",
        ),
        ("FROM books |> WHERE stock = 0", "pipe syntax"),
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
            FirestoreSqlOptions::new()
                .with_collections([FirestoreCollectionId::from_static("books")]),
            "collections",
        ),
        (
            "SELECT * FROM documents('secrets/s1')",
            FirestoreSqlOptions::new()
                .with_collections([FirestoreCollectionId::from_static("books")]),
            "collections",
        ),
        (
            "SELECT * FROM database()",
            FirestoreSqlOptions::new()
                .with_collections([FirestoreCollectionId::from_static("books")]),
            "database",
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
fn a_statement_over_16_kib_is_refused() {
    let sql = format!(
        "SELECT * FROM books WHERE title = '{}'",
        "a".repeat(16 * 1024)
    );
    assert_eq!(refused(&sql).field, "max_length");
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
            field("title").substring(positive_to_zero_based(field("pages"))),
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

#[test]
fn group_by_translates_to_the_fluent_aggregate() {
    let sql =
        "SELECT genre, COUNT(*) AS books, AVG(rating) AS mean, COUNT(DISTINCT author) AS authors \
               FROM books WHERE stock > 0 GROUP BY genre HAVING COUNT(*) > 1 ORDER BY books DESC";

    let expected = fluent()
        .from("books")
        .filter(|q| q.field("stock").greater_than(0))
        .aggregate_grouped(
            |e| [e.field("genre")],
            |e| {
                [
                    e.count_all().alias("books"),
                    e.field("rating").average().alias("mean"),
                    e.field("author").count_distinct().alias("authors"),
                ]
            },
        )
        .filter(|q| q.field("books").greater_than(1))
        .sort(|e| [e.field("books").desc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("genre")),
                e.field("books").alias("books"),
                e.field("mean").alias("mean"),
                e.field("authors").alias("authors"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated(sql), expected);
}

#[test]
fn a_computed_group_and_an_unselected_aggregate_get_hidden_names_that_the_projection_drops() {
    let sql = "SELECT LOWER(genre) AS kind, SUM(pages) AS pages FROM books \
               GROUP BY LOWER(genre) HAVING AVG(rating) > 4.3 ORDER BY kind";

    let expected = fluent()
        .from("books")
        .aggregate_grouped(
            |e| [e.field("genre").to_lower().alias("kind")],
            |e| {
                [
                    e.field("pages").sum().alias("pages"),
                    e.field("rating").average().alias("_agg0"),
                ]
            },
        )
        .filter(|q| q.field("_agg0").greater_than(4.3))
        .sort(|e| [e.field("kind").asc()])
        .fields(|e| {
            [
                e.field("kind").alias("kind"),
                e.field("pages").alias("pages"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated(sql), expected);
}

#[test]
fn aggregates_without_group_by_aggregate_all_documents_and_compose_in_expressions() {
    let sql = "SELECT COUNT(*) AS total, SUM(stock) / COUNT(*) AS per_book FROM books";

    let expected = fluent()
        .from("books")
        .aggregate(|e| {
            [
                e.count_all().alias("total"),
                e.field("stock").sum().alias("_agg0"),
            ]
        })
        .fields(|e| {
            [
                e.field("total").alias("total"),
                e.field("_agg0").divide(e.field("total")).alias("per_book"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated(sql), expected);
}

#[test]
fn select_distinct_translates_to_the_distinct_stage_before_the_sort() {
    let sql = "SELECT DISTINCT genre, LOWER(author) AS writer FROM books ORDER BY writer";

    let expected = fluent()
        .from("books")
        .distinct(|e| {
            [
                FirestoreSelection::from(e.field("genre")),
                e.field("author").to_lower().alias("writer"),
            ]
        })
        .sort(|e| [e.field("writer").asc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("genre")),
                e.field("writer").alias("writer"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated(sql), expected);
}

#[test]
fn union_appends_the_branch_and_distinct_deduplicates_by_the_output_names() {
    let union_all = "SELECT title AS name FROM books UNION ALL SELECT name FROM restaurants \
                     ORDER BY name LIMIT 5";
    let union_distinct = "SELECT title AS name FROM books UNION SELECT name FROM restaurants";

    let books = || {
        fluent()
            .from("books")
            .fields(|e| [e.field("title").alias("name")])
    };
    let restaurants = |p: FirestorePipelineInitialBuilder<FirestorePipelineNestedContext>| {
        p.from("restaurants").fields(|e| [e.field("name")])
    };

    assert_eq!(
        translated(union_all),
        books()
            .union(restaurants)
            .sort(|e| [e.field("name").asc()])
            .limit(5)
            .into_pipeline()
    );
    assert_eq!(
        translated(union_distinct),
        books()
            .union(restaurants)
            .distinct(|e| [e.field("name")])
            .into_pipeline()
    );
}

#[test]
fn a_text_search_condition_is_lifted_into_the_source() {
    let sql = "SELECT name, score() AS relevance FROM restaurants \
               WHERE document_matches(@query) AND cuisine = 'breakfast' \
               ORDER BY score() DESC LIMIT 20";

    let expected = fluent()
        .from("restaurants")
        .search(|s| {
            s.text(|_| "waffles -bacon")
                .sort_by_score()
                .score_field("relevance")
        })
        .filter(|q| q.field("cuisine").eq("breakfast"))
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("name")),
                e.field("relevance").alias("relevance"),
            ]
        })
        .limit(20)
        .into_pipeline();

    let pipeline = translate(
        sql,
        &FirestoreSqlParameters::new().param("query", "waffles -bacon"),
        &FirestoreSqlOptions::new(),
    );
    assert_eq!(pipeline.unwrap(), expected);
}

#[test]
fn a_geo_distance_condition_is_lifted_into_the_source() {
    let distance = "geo_distance(location, geo_point(38.989177, -107.065076))";
    let sql = format!(
        "SELECT name FROM restaurants WHERE {distance} <= 1000 AND cuisine = 'diner' \
         ORDER BY {distance}"
    );
    let center = FirestoreLatLng(FirestoreGeoPoint {
        latitude: 38.989177,
        longitude: -107.065076,
    });

    let expected = fluent()
        .from("restaurants")
        .search(|s| s.geo_within("location", center, 1000.0).sort_by_distance())
        .filter(|q| q.field("cuisine").eq("diner"))
        .fields(|e| [e.field("name")])
        .into_pipeline();

    assert_eq!(translated(&sql), expected);
}

#[test]
fn aggregation_set_operations_and_search_outside_their_forms_are_refused_by_name() {
    let refusals = [
        (
            "SELECT title, COUNT(*) AS n FROM books GROUP BY genre",
            "GROUP BY",
        ),
        ("SELECT * FROM books GROUP BY genre", "*"),
        (
            "SELECT genre, COUNT(*) AS n FROM books GROUP BY 1",
            "GROUP BY",
        ),
        ("SELECT genre FROM books GROUP BY ALL", "GROUP BY ALL"),
        ("SELECT genre FROM books GROUP BY ROLLUP(genre)", "ROLLUP"),
        ("SELECT genre FROM books HAVING genre = 'x'", "HAVING"),
        ("SELECT genre FROM books WHERE COUNT(*) > 1", "count"),
        ("SELECT SUM(SUM(stock)) AS n FROM books", "sum"),
        ("SELECT SUM(stock, pages) AS n FROM books", "sum"),
        ("SELECT COUNT(*) FROM books", "AS"),
        (
            "SELECT ANY_VALUE(title) AS t FROM books",
            "the aggregate function any_value",
        ),
        ("SELECT DISTINCT * FROM books", "*"),
        (
            "SELECT DISTINCT genre, COUNT(*) AS n FROM books GROUP BY genre",
            "DISTINCT with GROUP BY",
        ),
        ("SELECT DISTINCT ON (genre) genre FROM books", "DISTINCT ON"),
        (
            "SELECT title FROM books UNION ALL SELECT name FROM restaurants",
            "UNION",
        ),
        ("SELECT a, b FROM x UNION ALL SELECT b, a FROM y", "UNION"),
        (
            "SELECT * FROM books UNION DISTINCT SELECT * FROM archive",
            "UNION",
        ),
        (
            "SELECT title FROM books UNION ALL BY NAME SELECT title FROM archive",
            "UNION BY NAME",
        ),
        (
            "SELECT * FROM books WHERE document_matches('a') OR stock = 1",
            "document_matches",
        ),
        (
            "SELECT * FROM books WHERE document_matches('a') AND document_matches('b')",
            "document_matches",
        ),
        (
            "SELECT * FROM documents('books/b1') WHERE document_matches('a')",
            "document_matches",
        ),
        (
            "SELECT * FROM books WHERE geo_distance(location, geo_point(1, 2)) >= 5",
            "geo_distance",
        ),
        (
            "SELECT * FROM books WHERE document_matches('a') ORDER BY score()",
            "score",
        ),
        (
            "SELECT * FROM books WHERE document_matches('a') ORDER BY score() DESC, title",
            "score",
        ),
        ("SELECT score() AS s FROM books", "score"),
    ];

    for (sql, construct) in refusals {
        assert_eq!(refused(sql).field, construct, "{sql}");
    }
}

#[test]
fn group_by_and_having_refer_to_select_aliases() {
    let sql = "SELECT LOWER(genre) AS kind, COUNT(*) AS n FROM books \
               GROUP BY kind HAVING n > 1";

    let expected = fluent()
        .from("books")
        .aggregate_grouped(
            |e| [e.field("genre").to_lower().alias("kind")],
            |e| [e.count_all().alias("n")],
        )
        .filter(|q| q.field("n").greater_than(1))
        .fields(|e| [e.field("kind").alias("kind"), e.field("n").alias("n")])
        .into_pipeline();

    assert_eq!(translated(sql), expected);
}

#[derive(Serialize)]
struct Item {
    #[serde(rename = "__name__", skip_serializing_if = "Option::is_none")]
    name: Option<FirestoreReference>,
    sku: &'static str,
    stock: i64,
}

fn item(id: Option<&str>, sku: &'static str, stock: i64) -> Item {
    Item {
        name: id.map(|id| FirestoreReference(format!("{DOCUMENTS}/items/{id}"))),
        sku,
        stock,
    }
}

fn atomic(
    pipeline: FirestorePipeline,
    write: impl Into<FirestorePipelineWrite>,
) -> FirestorePipelineWriteParams {
    FirestorePipelineWriteParams::new(pipeline, write.into())
        .with_transaction(FirestorePipelineWriteTransaction::Atomic)
}

fn items() -> FirestoreWriteCollection {
    FirestoreWriteCollection::new("items".to_string())
}

#[test]
fn an_insert_with_values_translates_to_an_atomic_insert_of_literals() {
    let sql = "INSERT INTO items (__name__, sku, stock) \
               VALUES ('a-1', 'a-1', 3), (?, 'a-2', ?)";
    let parameters = FirestoreSqlParameters::new().params([json!("a-2"), json!(0)]);

    let expected = atomic(
        fluent()
            .literals([item(Some("a-1"), "a-1", 3), item(Some("a-2"), "a-2", 0)])
            .into_pipeline(),
        FirestoreInsertWrite::new().with_target(items()),
    );

    assert_eq!(
        translated_write_with(sql, &parameters, &FirestoreSqlOptions::new()),
        expected
    );
}

#[test]
fn an_insert_select_translates_to_an_atomic_insert_of_the_selected_rows() {
    let sql = "INSERT INTO archive SELECT __name__, title, stock FROM books WHERE stock = 0";

    let expected = atomic(
        fluent()
            .from("books")
            .filter(|q| q.field("stock").eq(0))
            .fields(|e| [e.field("__name__"), e.field("title"), e.field("stock")])
            .into_pipeline(),
        FirestoreInsertWrite::new()
            .with_target(FirestoreWriteCollection::new("archive".to_string())),
    );

    assert_eq!(translated_write(sql), expected);
}

#[test]
fn a_column_list_names_the_fields_of_the_selected_columns() {
    let sql = "INSERT INTO archive (name, copies) SELECT title, stock FROM books";

    let expected = atomic(
        fluent()
            .from("books")
            .fields(|e| [e.field("title"), e.field("stock")])
            .fields(|e| {
                [
                    e.field("title").alias("name"),
                    e.field("stock").alias("copies"),
                ]
            })
            .into_pipeline(),
        FirestoreInsertWrite::new()
            .with_target(FirestoreWriteCollection::new("archive".to_string())),
    );

    assert_eq!(translated_write(sql), expected);
}

#[test]
fn an_insert_or_replace_translates_to_an_atomic_upsert() {
    let sql = "INSERT OR REPLACE INTO items (sku, stock) VALUES ('a-1', 3)";

    let expected = atomic(
        fluent().literals([item(None, "a-1", 3)]).into_pipeline(),
        FirestoreUpsertWrite::new().with_target(items()),
    );

    assert_eq!(translated_write(sql), expected);
}

#[test]
fn on_conflict_over_a_select_needs_the_opt_in() {
    let sql = "INSERT INTO items SELECT __name__, stock FROM other \
               ON CONFLICT (__name__) DO NOTHING";

    assert_eq!(refused(sql).field, "ON CONFLICT");
    translated_write_with(
        sql,
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::new().allow_subqueries(),
    );
}

#[test]
fn on_conflict_over_a_select_needs_a_document_name_the_translation_can_see() {
    let refused_sources = [
        "INSERT INTO items SELECT 'a' AS __name__, stock FROM other \
         ON CONFLICT (__name__) DO NOTHING",
        "INSERT INTO items (__name__, stock) SELECT ref, stock FROM other \
         ON CONFLICT (__name__) DO NOTHING",
        "INSERT INTO items SELECT * FROM (SELECT stock FROM other) AS derived \
         ON CONFLICT (__name__) DO NOTHING",
        "INSERT INTO items SELECT * EXCEPT (__name__) FROM other \
         ON CONFLICT (__name__) DO NOTHING",
    ];
    let options = FirestoreSqlOptions::new().allow_subqueries();
    for sql in refused_sources {
        assert_eq!(
            refused_with(sql, &FirestoreSqlParameters::new(), &options).field,
            "ON CONFLICT",
            "{sql}"
        );
    }

    for sql in [
        "INSERT INTO items SELECT * FROM other ON CONFLICT (__name__) DO NOTHING",
        "INSERT INTO items SELECT * FROM other WHERE stock > 0 ORDER BY stock LIMIT 5 \
         ON CONFLICT (__name__) DO NOTHING",
        "INSERT INTO items (__name__, stock) SELECT __name__, stock FROM other \
         ON CONFLICT (__name__) DO UPDATE SET stock = excluded.stock",
    ] {
        translated_write_with(sql, &FirestoreSqlParameters::new(), &options);
    }
}

#[test]
fn an_update_sets_each_target_and_a_dotted_target_sets_a_nested_field() {
    let sql = "UPDATE items AS i SET status = 'sold_out', price = i.price * 2, dims.w = 2 \
               WHERE i.stock = 0";

    let expected = atomic(
        fluent()
            .from("items")
            .filter(|q| q.field("stock").eq(0))
            .into_pipeline(),
        FirestoreUpdateWrite::new().with_transforms(vec![
            FirestoreExpr::constant("sold_out").alias("status"),
            field("price").multiply(2).alias("price"),
            FirestoreExpr::constant(2).alias("dims.w"),
        ]),
    );

    assert_eq!(translated_write(sql), expected);
}

#[test]
fn a_delete_filters_the_documents_it_deletes() {
    assert_eq!(
        translated_write("DELETE FROM items WHERE stock = 0"),
        atomic(
            fluent()
                .from("items")
                .filter(|q| q.field("stock").eq(0))
                .into_pipeline(),
            FirestorePipelineWrite::Delete
        )
    );
    assert_eq!(
        translated_write("DELETE items WHERE __name__ = 'a-1'"),
        atomic(
            fluent()
                .from("items")
                .filter(|q| {
                    q.field("__name__")
                        .eq(FirestoreReference(format!("{DOCUMENTS}/items/a-1")))
                })
                .into_pipeline(),
            FirestorePipelineWrite::Delete
        )
    );
}

#[test]
fn writes_outside_their_forms_are_refused_by_name() {
    let refusals = [
        (
            "MERGE items AS t USING other AS s ON t.sku = s.sku WHEN MATCHED THEN DELETE",
            "MERGE",
        ),
        (
            "INSERT INTO items (sku) VALUES ('a') ON CONFLICT (sku) DO NOTHING",
            "ON CONFLICT",
        ),
        (
            "INSERT INTO items (__name__) VALUES ('a') ON CONFLICT (__name__, sku) DO NOTHING",
            "ON CONFLICT",
        ),
        (
            "INSERT INTO items (__name__) VALUES ('a') ON CONFLICT ON CONSTRAINT c DO NOTHING",
            "ON CONFLICT",
        ),
        (
            "INSERT INTO items (__name__, stock) VALUES ('a', 1) \
             ON CONFLICT DO UPDATE SET stock = 0",
            "ON CONFLICT",
        ),
        (
            "INSERT INTO items (sku) VALUES ('a') ON CONFLICT (__name__) DO NOTHING",
            "ON CONFLICT",
        ),
        (
            "INSERT INTO items (__name__, _conflict_stored) VALUES ('a', 1) \
             ON CONFLICT (__name__) DO NOTHING",
            "_conflict_stored",
        ),
        (
            "INSERT INTO items (__name__, stock) VALUES ('a', 1) \
             ON CONFLICT (__name__) DO UPDATE SET stock = 0 WHERE __name__ = 'a'",
            "__name__",
        ),
        (
            "INSERT INTO items (__name__, stock) VALUES ('a', 1) \
             ON CONFLICT (__name__) DO UPDATE SET __name__ = 'b'",
            "__name__",
        ),
        (
            "INSERT INTO items (__name__, stock) VALUES ('a', 1) \
             ON CONFLICT (__name__) DO UPDATE SET stock = 0, stock = 1",
            "SET",
        ),
        (
            "INSERT OR REPLACE INTO items (__name__) VALUES ('a') \
             ON CONFLICT (__name__) DO NOTHING",
            "ON CONFLICT",
        ),
        (
            "INSERT INTO items (sku) VALUES ('a') ON DUPLICATE KEY UPDATE sku = 'b'",
            "ON DUPLICATE KEY UPDATE",
        ),
        (
            "INSERT OR IGNORE INTO items (sku) VALUES ('a')",
            "INSERT OR IGNORE",
        ),
        ("INSERT INTO items VALUES ('a')", "INSERT"),
        ("INSERT INTO items (sku, stock) VALUES ('a')", "VALUES"),
        ("INSERT INTO items (sku) VALUES (UPPER('a'))", "VALUES"),
        ("INSERT INTO items (sku) VALUES (stock)", "VALUES"),
        ("INSERT INTO items (sku, sku) VALUES ('a', 'b')", "INSERT"),
        ("INSERT INTO items (dims.w) VALUES (1)", "INSERT"),
        (
            "INSERT INTO items (__name__) VALUES ('other/o1')",
            "__name__",
        ),
        ("INSERT INTO items (__name__) VALUES ('x/y/z')", "document"),
        ("INSERT INTO items (sku) VALUES ('a') LIMIT 1", "LIMIT"),
        ("INSERT INTO items (sku) SELECT * FROM books", "INSERT"),
        ("INSERT INTO items (a, b) SELECT title FROM books", "INSERT"),
        ("INSERT INTO items DEFAULT VALUES", "INSERT"),
        (
            "INSERT INTO items (sku) VALUES ('a') RETURNING sku",
            "RETURNING",
        ),
        ("UPDATE items SET stock = 0", "WHERE"),
        ("DELETE FROM items", "WHERE"),
        (
            "UPDATE items SET __name__ = 'a' WHERE stock = 0",
            "__name__",
        ),
        ("UPDATE items SET stock = 0, stock = 1 WHERE TRUE", "SET"),
        ("UPDATE items SET dims = 0, dims.w = 1 WHERE TRUE", "SET"),
        (
            "UPDATE items SET (a, b) = (1, 2) WHERE TRUE",
            "a tuple assignment",
        ),
        ("UPDATE items SET stock = @stock WHERE TRUE", "@stock"),
        ("UPDATE items SET stock = 0 FROM other WHERE TRUE", "FROM"),
        (
            "UPDATE collection_group('items') SET stock = 0 WHERE TRUE",
            "a write",
        ),
        ("DELETE FROM documents('items/a') WHERE TRUE", "a write"),
        (
            "DELETE FROM items AS i JOIN other AS o ON i.a = o.a WHERE TRUE",
            "JOIN",
        ),
        ("DELETE FROM items WHERE TRUE LIMIT 1", "LIMIT"),
        (
            "DELETE FROM items WHERE document_matches('a')",
            "document_matches",
        ),
    ];

    for (sql, construct) in refusals {
        assert_eq!(refused(sql).field, construct, "{sql}");
    }
}

#[test]
fn the_collection_allowlist_applies_to_the_target_of_a_write() {
    let options =
        FirestoreSqlOptions::new().with_collections([FirestoreCollectionId::from_static("items")]);
    let parameters = FirestoreSqlParameters::new();

    for sql in [
        "INSERT INTO secrets (sku) VALUES ('a')",
        "INSERT INTO items SELECT * FROM secrets",
        "UPDATE secrets SET stock = 0 WHERE TRUE",
        "DELETE FROM secrets WHERE TRUE",
    ] {
        assert_eq!(
            refused_with(sql, &parameters, &options).field,
            "collections",
            "{sql}"
        );
    }
}

fn subqueries_allowed() -> FirestoreSqlOptions {
    FirestoreSqlOptions::new().allow_subqueries()
}

fn translated_allowing_subqueries(sql: &str) -> FirestorePipeline {
    translate(sql, &FirestoreSqlParameters::new(), &subqueries_allowed())
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn refused_with_subqueries(sql: &str) -> FirestoreInvalidParametersPublicDetails {
    refused_with(sql, &FirestoreSqlParameters::new(), &subqueries_allowed())
}

/// The reviews of the restaurant held by the variable `variable`, as an array.
fn reviews_of(variable: &str) -> FirestoreExpr {
    FirestorePipelineExprBuilder.array_subquery(|p| {
        p.from("reviews")
            .all_descendants()
            .filter(|q| q.field("restaurant").eq(q.variable(variable)))
    })
}

const RESTAURANTS_JOINED_TO_REVIEWS: &str = "FROM `pipeline-restaurants` AS rest \
    JOIN collection_group('reviews') AS r ON r.restaurant = rest.__name__";

#[test]
fn a_join_translates_to_an_array_subquery_that_unnest_flattens() {
    let sql = format!(
        "SELECT rest.name AS name, r.rating AS rating {RESTAURANTS_JOINED_TO_REVIEWS} \
         WHERE r.rating >= 4 ORDER BY rest.name, r.rating DESC"
    );

    let expected = fluent()
        .from("pipeline-restaurants")
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .add_fields(|_| [reviews_of("rest___name__").alias("r")])
        .unnest(|e| e.field("r"))
        .filter(|q| q.field("r.rating").greater_than_or_equal(4))
        .sort(|e| [e.field("name").asc(), e.field("r.rating").desc()])
        .fields(|e| {
            [
                e.field("name").alias("name"),
                e.field("r.rating").alias("rating"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated_allowing_subqueries(&sql), expected);
}

#[test]
fn a_subquery_in_a_join_condition_reads_the_joined_table() {
    let sql = format!(
        "SELECT rest.name AS name {RESTAURANTS_JOINED_TO_REVIEWS} \
         AND EXISTS (SELECT c.name FROM cities AS c WHERE c.__name__ = r.city)"
    );

    let reviews = FirestorePipelineExprBuilder.array_subquery(|p| {
        p.from("reviews")
            .all_descendants()
            .define_variables(|e| [e.field("city").alias("r_city")])
            .filter(|q| {
                q.for_all([
                    q.field("restaurant").eq(q.variable("rest___name__")),
                    Some(
                        q.values()
                            .array_subquery(|p| {
                                p.from("cities")
                                    .filter(|q| {
                                        q.field("__name__")
                                            .is_in(q.values().array([q.variable("r_city")]))
                                    })
                                    .limit(1)
                            })
                            .array_length()
                            .greater_than(0),
                    ),
                ])
            })
    });
    let expected = fluent()
        .from("pipeline-restaurants")
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .add_fields(|_| [reviews.alias("r")])
        .unnest(|e| e.field("r"))
        .fields(|e| [e.field("name").alias("name")])
        .into_pipeline();

    assert_eq!(translated_allowing_subqueries(&sql), expected);
}

#[test]
fn a_left_join_keeps_a_row_without_a_match_as_a_null_element() {
    let sql = "SELECT rest.name AS name, r.rating AS rating \
               FROM `pipeline-restaurants` AS rest \
               LEFT OUTER JOIN collection_group('reviews') AS r ON r.restaurant = rest.__name__";

    let expected = fluent()
        .from("pipeline-restaurants")
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .add_fields(|_| [reviews_of("rest___name__").alias("r")])
        .add_fields(|e| {
            [e.conditional(
                e.field("r").array_length().equal(0),
                e.array([FirestoreExpr::constant(None::<bool>)]),
                e.field("r"),
            )
            .alias("r")]
        })
        .unnest(|e| e.field("r"))
        .fields(|e| {
            [
                e.field("name").alias("name"),
                e.field("r.rating").alias("rating"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated_allowing_subqueries(sql), expected);
}

#[test]
fn a_join_on_a_document_name_looks_the_document_up_by_an_array_of_the_key() {
    let sql = "SELECT r.rating, rest.name AS restaurant \
               FROM collection_group('reviews') AS r \
               JOIN `pipeline-restaurants` AS rest ON rest.__name__ = r.restaurant";

    let expected = fluent()
        .from("reviews")
        .all_descendants()
        .define_variables(|e| [e.field("restaurant").alias("r_restaurant")])
        .add_fields(|e| {
            [e.array_subquery(|p| {
                p.from("pipeline-restaurants").filter(|q| {
                    q.field("__name__")
                        .is_in(q.values().array([q.variable("r_restaurant")]))
                })
            })
            .alias("rest")]
        })
        .unnest(|e| e.field("rest"))
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("rating")),
                e.field("rest.name").alias("restaurant"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated_allowing_subqueries(sql), expected);
}

#[test]
fn a_join_into_a_subcollection_reads_the_children_without_a_variable_or_the_opt_in() {
    let expected = fluent()
        .from("pipeline-restaurants")
        .add_fields(|e| [e.array_subquery(|p| p.subcollection("reviews")).alias("r")])
        .unnest(|e| e.field("r"))
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("name")),
                e.field("r.rating").alias("rating"),
            ]
        })
        .into_pipeline();

    for join in [
        "JOIN subcollection('reviews') AS r ON TRUE",
        "CROSS JOIN subcollection('reviews') AS r",
        ", subcollection('reviews') AS r",
    ] {
        let sql = format!(
            "SELECT rest.name, r.rating AS rating FROM `pipeline-restaurants` AS rest {join}"
        );
        assert_eq!(translated(&sql), expected, "{sql}");
    }
}

#[test]
fn select_star_over_a_join_is_refused() {
    for sql in [
        "SELECT * FROM `pipeline-restaurants` AS rest JOIN subcollection('reviews') AS r ON TRUE",
        "SELECT * FROM books AS b CROSS JOIN UNNEST(b.tags) AS tag",
        "SELECT * EXCEPT (title) FROM books AS b CROSS JOIN UNNEST(b.tags) AS tag",
    ] {
        assert_eq!(refused_with_subqueries(sql).field, "*", "{sql}");
    }
}

#[test]
fn a_join_or_a_correlated_subquery_needs_the_opt_in() {
    let per_row = [
        (
            format!("SELECT rest.name AS name {RESTAURANTS_JOINED_TO_REVIEWS}"),
            "JOIN",
        ),
        (
            "SELECT rest.name AS name FROM `pipeline-restaurants` AS rest WHERE EXISTS \
             (SELECT 1 FROM collection_group('reviews') AS r WHERE r.restaurant = rest.__name__)"
                .to_string(),
            "subquery",
        ),
    ];

    for (sql, construct) in per_row {
        assert_eq!(refused(&sql).field, construct, "{sql}");
        translated_allowing_subqueries(&sql);
    }
}

#[test]
fn a_scalar_subquery_is_a_scalar_subquery_of_the_variables_it_reads() {
    let sql = "SELECT rest.name AS name, \
               (SELECT AVG(r.rating) AS average FROM collection_group('reviews') AS r \
                WHERE r.restaurant = rest.__name__) AS average_rating \
               FROM `pipeline-restaurants` AS rest";

    let expected = fluent()
        .from("pipeline-restaurants")
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .fields(|e| {
            [
                e.field("name").alias("name"),
                e.scalar_subquery(|p| {
                    p.from("reviews")
                        .all_descendants()
                        .filter(|q| q.field("restaurant").eq(q.variable("rest___name__")))
                        .aggregate(|e| [e.field("rating").average().alias("average")])
                        .fields(|e| [e.field("average").alias("average")])
                })
                .alias("average_rating"),
            ]
        })
        .into_pipeline();

    assert_eq!(translated_allowing_subqueries(sql), expected);
}

#[test]
fn exists_tests_that_the_subquery_finds_a_row() {
    let subquery = |variable: &str| {
        FirestorePipelineExprBuilder
            .array_subquery(|p| {
                p.from("reviews")
                    .all_descendants()
                    .filter(|q| q.field("restaurant").eq(q.variable(variable)))
                    .limit(1)
            })
            .array_length()
    };
    let exists = |test: FirestoreBoolExpr| {
        fluent()
            .from("pipeline-restaurants")
            .define_variables(|e| [e.field("__name__").alias("rest___name__")])
            .filter(|_| Some(test))
            .fields(|e| [e.field("name").alias("name")])
            .into_pipeline()
    };

    for (negation, test) in [
        ("", subquery("rest___name__").greater_than(0)),
        ("NOT ", subquery("rest___name__").equal(0)),
    ] {
        let sql = format!(
            "SELECT rest.name AS name FROM `pipeline-restaurants` AS rest WHERE {negation}EXISTS \
             (SELECT 1 FROM collection_group('reviews') AS r WHERE r.restaurant = rest.__name__)"
        );
        assert_eq!(translated_allowing_subqueries(&sql), exists(test), "{sql}");
    }
}

#[test]
fn in_and_array_subqueries_translate_to_array_subqueries() {
    let high_ratings = FirestorePipelineExprBuilder.array_subquery(|p| {
        p.from("reviews")
            .all_descendants()
            .filter(|q| q.field("rating").greater_than_or_equal(5))
            .fields(|e| [e.field("restaurant")])
    });

    for (negation, test) in [
        ("", field("__name__").equal_any(high_ratings.clone())),
        ("NOT ", field("__name__").not_equal_any(high_ratings)),
    ] {
        let sql = format!(
            "SELECT rest.name AS name FROM `pipeline-restaurants` AS rest WHERE rest.__name__ \
             {negation}IN (SELECT r.restaurant FROM collection_group('reviews') AS r WHERE r.rating >= 5)"
        );
        assert_eq!(
            translated_allowing_subqueries(&sql),
            fluent()
                .from("pipeline-restaurants")
                .filter(|_| Some(test))
                .fields(|e| [e.field("name").alias("name")])
                .into_pipeline(),
            "{sql}"
        );
    }

    assert_eq!(
        translated(
            "SELECT rest.name AS name, ARRAY(SELECT rating FROM subcollection('reviews')) AS ratings \
             FROM `pipeline-restaurants` AS rest"
        ),
        fluent()
            .from("pipeline-restaurants")
            .fields(|e| {
                [
                    e.field("name").alias("name"),
                    e.array_subquery(|p| p.subcollection("reviews").fields(|e| [e.field("rating")]))
                        .alias("ratings"),
                ]
            })
            .into_pipeline()
    );
}

#[test]
fn a_derived_table_continues_the_pipeline_of_its_query() {
    assert_eq!(
        translated(
            "SELECT t.title AS title FROM (SELECT title, rating FROM books WHERE rating > 4) AS t \
             WHERE t.rating > 4.5 ORDER BY t.title"
        ),
        fluent()
            .from("books")
            .filter(|q| q.field("rating").greater_than(4))
            .fields(|e| [e.field("title"), e.field("rating")])
            .filter(|q| q.field("rating").greater_than(4.5))
            .sort(|e| [e.field("title").asc()])
            .fields(|e| [e.field("title").alias("title")])
            .into_pipeline()
    );
}

#[test]
fn a_cte_runs_again_for_each_reference_to_it() {
    let good_titles = || {
        fluent()
            .from("books")
            .filter(|q| q.field("rating").greater_than(4))
            .fields(|e| [e.field("title")])
            .into_pipeline()
    };

    assert_eq!(
        translated(
            "WITH good AS (SELECT title FROM books WHERE rating > 4) \
             SELECT * FROM good UNION ALL SELECT * FROM good"
        ),
        good_titles().with_stage(FirestorePipelineStage::Union(FirestoreUnionStage::new(
            good_titles()
        )))
    );
}

#[test]
fn a_cte_sees_the_ones_before_it_and_shadows_a_collection_of_the_same_name() {
    assert_eq!(
        translated(
            "WITH a AS (SELECT * FROM books WHERE stock > 0), b AS (SELECT title FROM a) \
             SELECT * FROM b"
        ),
        fluent()
            .from("books")
            .filter(|q| q.field("stock").greater_than(0))
            .fields(|e| [e.field("title")])
            .into_pipeline()
    );
    assert_eq!(
        translated("WITH Books AS (SELECT title FROM archive) SELECT * FROM books"),
        fluent()
            .from("archive")
            .fields(|e| [e.field("title")])
            .into_pipeline()
    );
}

#[test]
fn unnest_flattens_an_array_of_the_row_and_can_number_its_elements() {
    let expected = fluent()
        .from("pipeline-books")
        .unnest_with_index_field(|e| e.field("tags").alias("tag"), "pos")
        .filter(|q| q.field("tag").eq("space"))
        .fields(|e| [e.field("title"), e.field("tag"), e.field("pos")])
        .into_pipeline();

    for join in [
        "CROSS JOIN UNNEST(b.tags) AS tag WITH OFFSET AS pos",
        ", UNNEST(b.tags) AS tag WITH OFFSET AS pos",
        "INNER JOIN UNNEST(b.tags) AS tag WITH OFFSET AS pos ON TRUE",
    ] {
        let sql = format!(
            "SELECT b.title, tag, pos FROM `pipeline-books` AS b {join} WHERE tag = 'space'"
        );
        assert_eq!(translated(&sql), expected, "{sql}");
    }
}

#[test]
fn in_unnest_tests_membership_in_an_array() {
    assert_eq!(
        condition("SELECT * FROM books WHERE 'space' IN UNNEST(tags)"),
        FirestoreExpr::constant("space")
            .equal_any(field("tags"))
            .into()
    );
    assert_eq!(
        condition("SELECT * FROM books WHERE stock NOT IN UNNEST([0, 7])"),
        field("stock").not_equal_any(vec![0, 7]).into()
    );

    let ids = FirestoreSqlParameters::new().param("ids", ["b1", "b2"]);
    let names = translate(
        "SELECT * FROM books WHERE __name__ IN UNNEST(@ids)",
        &ids,
        &FirestoreSqlOptions::new(),
    )
    .unwrap();
    assert_eq!(
        names,
        fluent()
            .from("books")
            .filter(|q| {
                q.field("__name__").is_in(vec![
                    FirestoreReference(format!("{DOCUMENTS}/books/b1")),
                    FirestoreReference(format!("{DOCUMENTS}/books/b2")),
                ])
            })
            .into_pipeline()
    );
}

#[test]
fn a_name_in_a_join_is_a_table_or_alias_or_an_error() {
    let refusals = [
        (
            "SELECT rest.name FROM `pipeline-restaurants` AS rest \
             JOIN subcollection('reviews') AS r ON TRUE WHERE rv.rating > 3",
            "rv",
        ),
        (
            "SELECT rest.name FROM `pipeline-restaurants` AS rest \
             JOIN subcollection('reviews') AS r ON TRUE WHERE awards.hugo",
            "awards",
        ),
        (
            "SELECT * FROM `pipeline-restaurants` AS rest \
             JOIN collection_group('reviews') AS r ON restaurant = rest.__name__",
            "restaurant",
        ),
        (
            "SELECT * FROM `pipeline-restaurants` AS rest \
             JOIN collection_group('reviews') AS rest ON rest.restaurant = rest.__name__",
            "rest",
        ),
        (
            "SELECT * FROM `pipeline-restaurants` AS rest \
             JOIN collection_group('reviews') AS r ON c.restaurant = rest.__name__ \
             JOIN `cities` AS c ON c.__name__ = rest.city",
            "c",
        ),
        (
            "SELECT * FROM `pipeline-restaurants` AS rest WHERE EXISTS \
             (SELECT 1 FROM subcollection('reviews') AS r WHERE rv.rating > 3)",
            "rv",
        ),
    ];

    for (sql, name) in refusals {
        assert_eq!(refused_with_subqueries(sql).field, name, "{sql}");
    }
}

#[test]
fn joins_subqueries_and_ctes_outside_their_forms_are_refused_by_name() {
    let refusals = [
        ("SELECT * FROM a RIGHT JOIN b ON a.k = b.k", "RIGHT JOIN"),
        (
            "SELECT * FROM a FULL OUTER JOIN b ON a.k = b.k",
            "FULL JOIN",
        ),
        ("SELECT * FROM a JOIN b USING (k)", "JOIN USING"),
        ("SELECT * FROM a NATURAL JOIN b", "NATURAL JOIN"),
        ("SELECT * FROM a JOIN b", "JOIN"),
        (
            "SELECT * FROM a LEFT JOIN UNNEST(a.tags) AS tag ON TRUE",
            "LEFT JOIN UNNEST",
        ),
        ("SELECT * FROM a CROSS JOIN UNNEST(a.tags)", "UNNEST"),
        ("SELECT * FROM UNNEST([1, 2]) AS n", "UNNEST"),
        (
            "WITH RECURSIVE t AS (SELECT * FROM a) SELECT * FROM t",
            "WITH RECURSIVE",
        ),
        (
            "WITH t(x) AS (SELECT * FROM a) SELECT * FROM t",
            "an alias with columns",
        ),
        ("SELECT * FROM subcollection('reviews')", "subcollection"),
        ("SELECT * FROM a WHERE x = (SELECT x, y FROM b)", "subquery"),
        ("SELECT * FROM a WHERE x = (SELECT * FROM b)", "subquery"),
        (
            "SELECT * FROM a WHERE x IN (SELECT x FROM b UNION ALL SELECT x FROM c)",
            "subquery",
        ),
        (
            "SELECT * FROM a WHERE x IN (SELECT x, y FROM b)",
            "subquery",
        ),
    ];

    for (sql, construct) in refusals {
        assert_eq!(refused_with_subqueries(sql).field, construct, "{sql}");
    }
}

#[test]
fn a_subquery_the_clause_cannot_define_a_variable_for_is_refused() {
    let correlated_set = "UPDATE items AS i SET stock = (SELECT MAX(o.stock) AS most \
                          FROM other AS o WHERE o.sku = i.sku) WHERE TRUE";

    assert_eq!(refused_with_subqueries(correlated_set).field, "subquery");
}

#[test]
fn a_subquery_in_the_where_of_a_write_reads_the_variables_before_the_write_stage() {
    let sql = "DELETE FROM items WHERE EXISTS \
               (SELECT 1 FROM other AS o WHERE o.sku = items.sku)";

    let found = FirestorePipelineExprBuilder
        .array_subquery(|p| {
            p.from("other")
                .filter(|q| q.field("sku").eq(q.variable("items_sku")))
                .limit(1)
        })
        .array_length();
    let expected = atomic(
        fluent()
            .from("items")
            .define_variables(|e| [e.field("sku").alias("items_sku")])
            .filter(|_| Some(found.greater_than(0)))
            .into_pipeline(),
        FirestorePipelineWrite::Delete,
    );

    assert_eq!(
        translated_write_with(sql, &FirestoreSqlParameters::new(), &subqueries_allowed()),
        expected
    );
}

#[test]
fn a_cte_that_doubles_at_each_step_is_refused_and_a_long_chain_is_not() {
    let doubling = {
        let steps: Vec<String> = (1..=30)
            .map(|step| {
                let previous = step - 1;
                format!(
                    "c{step} AS (SELECT * FROM c{previous} UNION ALL SELECT * FROM c{previous})"
                )
            })
            .collect();
        format!(
            "WITH c0 AS (SELECT * FROM books), {} SELECT * FROM c30",
            steps.join(", ")
        )
    };
    let chain = {
        let steps: Vec<String> = (1..=40)
            .map(|step| format!("c{step} AS (SELECT * FROM c{})", step - 1))
            .collect();
        format!(
            "WITH c0 AS (SELECT * FROM books), {} SELECT * FROM c40",
            steps.join(", ")
        )
    };
    let nested = format!(
        "SELECT * FROM a WHERE x IN (SELECT x FROM b WHERE x IN (SELECT x FROM c WHERE {}))",
        "x IN (SELECT x FROM d)"
    );

    // An overflow aborts the process, so a statement that recurses too deep fails the run.
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            assert_eq!(refused(&doubling).field, "WITH");
            assert_eq!(translated(&chain), fluent().from("books").into_pipeline());
            translated_allowing_subqueries(&nested);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn a_string_compared_with_the_name_of_a_joined_table_must_be_a_path() {
    let sql = |name: &str| {
        format!(
            "SELECT rest.name FROM `pipeline-restaurants` AS rest \
             JOIN subcollection('reviews') AS r ON TRUE WHERE r.__name__ = '{name}'"
        )
    };
    let path = "pipeline-restaurants/rest-01/reviews/review-1";

    let stages = translated(&sql(path)).stages;
    let [_, _, FirestorePipelineStage::Where(filter), _] = stages.as_slice() else {
        panic!("{stages:?}")
    };
    assert_eq!(
        FirestoreExpr::from(filter.condition.clone()),
        field("r.__name__").equal(reference(path)).into()
    );
    assert_eq!(refused(&sql("review-1")).field, "__name__");
}

#[test]
fn a_joined_alias_cannot_hide_a_field_of_the_left_side() {
    let hidden = "SELECT o.customer AS ref FROM orders AS o \
                  JOIN customers AS customer ON customer.__name__ = o.customer";
    assert_eq!(refused_with_subqueries(hidden).field, "customer");

    translated_allowing_subqueries(
        "SELECT o.total AS total, customer.name AS name FROM orders AS o \
         JOIN customers AS customer ON customer.__name__ = o.customer",
    );
}

#[test]
fn a_bare_name_that_is_the_alias_of_a_joined_table_is_not_read_as_a_field() {
    let bare_name = "SELECT customer AS c FROM orders AS o \
                     JOIN customers AS customer ON customer.__name__ = o.customer";
    assert_eq!(refused_with_subqueries(bare_name).field, "customer");

    translated_allowing_subqueries(
        "SELECT o.total AS total FROM orders AS o \
         LEFT JOIN customers AS customer ON customer.__name__ = o.customer \
         WHERE customer IS NULL",
    );
    translated_allowing_subqueries(
        "SELECT b.title AS title FROM books AS b CROSS JOIN UNNEST(b.tags) AS tag WHERE tag = 'x'",
    );
}

#[test]
fn a_position_below_one_is_not_shifted() {
    let positions = [
        ("SUBSTR(title, 0)", field("title").substring(0)),
        ("SUBSTR(title, -1)", field("title").substring(-1)),
    ];

    for (call, expected) in positions {
        let sql = format!("SELECT {call} AS v FROM books");
        assert_eq!(column(&sql), expected, "{call}");
    }
}

/// A position counted from 1 as a position counted from 0, which only a positive one needs: 0
/// and a negative position pass through as they are.
fn positive_to_zero_based(position: FirestoreExpr) -> FirestoreExpr {
    FirestorePipelineExprBuilder.conditional(
        position.clone().greater_than(0),
        position.clone().subtract(1),
        position,
    )
}

#[test]
fn an_unaliased_nested_group_key_is_refused() {
    for sql in [
        "SELECT awards.hugo, COUNT(*) AS n FROM books GROUP BY awards.hugo",
        "SELECT DISTINCT awards.hugo FROM books",
    ] {
        assert_eq!(refused(sql).field, "AS", "{sql}");
    }

    translated("SELECT awards.hugo AS hugo, COUNT(*) AS n FROM books GROUP BY awards.hugo");
}

#[test]
fn exists_refuses_grouping_that_changes_whether_a_row_exists() {
    for select in [
        "SELECT COUNT(*) AS n FROM reviews WHERE FALSE",
        "SELECT 1 FROM reviews GROUP BY stars",
        "SELECT 1 FROM reviews HAVING stars > 1",
    ] {
        let sql = format!("SELECT * FROM books WHERE EXISTS ({select})");
        assert_eq!(refused_with_subqueries(&sql).field, "EXISTS", "{sql}");
    }

    translated_allowing_subqueries(
        "SELECT * FROM books WHERE EXISTS (SELECT DISTINCT stars FROM reviews WHERE stars > 1)",
    );
}

#[test]
fn is_not_null_of_a_computed_value_compares_with_null_alone() {
    assert_eq!(
        condition("SELECT * FROM books WHERE LOWER(title) IS NOT NULL"),
        field("title").to_lower().not_equal(None::<bool>).into()
    );
}

#[test]
fn a_parenthesised_search_condition_is_lifted_into_the_source() {
    let plain = translated("SELECT name FROM restaurants WHERE document_matches('waffles')");

    assert_eq!(
        translated("SELECT name FROM restaurants WHERE (document_matches('waffles'))"),
        plain
    );
}

#[test]
fn a_table_alias_matches_without_regard_to_case() {
    assert_eq!(
        column("SELECT B.title AS v FROM books AS b"),
        field("title")
    );
    assert_eq!(
        refused_with_subqueries(
            "SELECT * FROM `pipeline-restaurants` AS rest \
             JOIN subcollection('reviews') AS REST ON TRUE"
        )
        .field,
        "REST"
    );
}

#[cfg(feature = "admin")]
#[test]
fn show_indexes_is_allowed_by_read_only_options() {
    let statement = translate_statement(
        "SHOW INDEXES FROM orders",
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::read_only(),
    )
    .unwrap_or_else(|error| panic!("{error}"));

    assert_eq!(
        statement,
        FirestoreSqlStatement::ShowIndexes(Some(FirestoreCollectionId::from_static("orders")))
    );
}

#[test]
fn show_collections_in_a_document_resolves_against_the_documents_root() {
    let statement = translate_statement(
        "SHOW COLLECTIONS IN `users/alice`",
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::new(),
    )
    .unwrap_or_else(|error| panic!("{error}"));

    assert_eq!(
        statement,
        FirestoreSqlStatement::ShowCollections(
            FirestoreListCollectionIdsParams::new().with_parent(format!("{DOCUMENTS}/users/alice"))
        )
    );
}

#[test]
fn read_only_options_refuse_index_statements() {
    let refusal = refused_with(
        "DROP INDEX `CICAgPj-z4oK`",
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::read_only(),
    );

    assert_eq!(refusal.field, "read_only");
}

#[cfg(feature = "admin")]
#[test]
fn if_not_exists_and_if_exists_are_read_as_part_of_the_statement() {
    let statements = [
        (
            "CREATE INDEX IF NOT EXISTS ON orders (customer_id)",
            "CREATE INDEX",
            location(1, 28),
        ),
        (
            "DROP INDEX IF EXISTS CICAgJim14AK",
            "DROP INDEX",
            location(1, 22),
        ),
        (
            "DROP UNIQUE INDEX IF EXISTS ON users (email)",
            "DROP INDEX",
            location(1, 29),
        ),
    ];

    for (sql, statement, body) in statements {
        let refusal = refused(sql);
        assert_eq!(
            (refusal.field.as_str(), refusal.location),
            (statement, body),
            "{sql}"
        );
    }
}
