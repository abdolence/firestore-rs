use crate::common::setup_enterprise;
use firestore::errors::{BackoffError, FirestoreError};
use firestore::*;
use futures::{FutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

mod common;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RestaurantRating {
    name: String,
    rating: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RestaurantAverage {
    name: String,
    average_rating: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Relevant {
    name: String,
    relevance: f64,
}

/// The reviews of the seeded restaurants as `(restaurant, rating)`, in the order a join with
/// `ORDER BY name, rating DESC` returns them.
const REVIEWS: [(&str, Option<i64>); 6] = [
    ("Pancake Palace", Some(3)),
    ("Slice of New York", Some(5)),
    ("Slice of New York", Some(4)),
    ("Slice of New York", Some(2)),
    ("Sunrise Waffle House", Some(5)),
    ("Sunrise Waffle House", Some(4)),
];

fn rated(rows: &[RestaurantRating]) -> Vec<(&str, Option<i64>)> {
    rows.iter()
        .map(|row| (row.name.as_str(), row.rating))
        .collect()
}

fn names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["name"].as_str().unwrap_or_default())
        .collect()
}

fn per_row_allowed() -> FirestoreSqlOptions {
    FirestoreSqlOptions::new().allow_subqueries()
}

#[tokio::test]
async fn a_select_returns_the_filtered_sorted_rows() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT title, rating FROM `pipeline-books` \
             WHERE rating >= @min_rating AND genre != @excluded \
             ORDER BY rating DESC, title LIMIT 5",
        )
        .param("min_rating", 4.2)
        .param("excluded", "Fantasy")
        .obj()
        .query()
        .await?;

    assert_eq!(
        rows,
        [
            json!({"title": "Dune", "rating": 4.6}),
            json!({"title": "Pride and Prejudice", "rating": 4.5}),
            json!({"title": "Crime and Punishment", "rating": 4.3}),
            json!({"title": "One Hundred Years of Solitude", "rating": 4.3}),
            json!({"title": "1984", "rating": 4.2}),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_substring_position_counts_from_1_and_a_negative_one_from_the_end() -> TestResult {
    let db = setup_enterprise().await?;

    let pieces: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT SUBSTR(title, 1, 5) AS first, SUBSTR(title, 0) AS whole, \
             SUBSTR(title, -4) AS last FROM `pipeline-books` WHERE __name__ = 'book-02'",
        )
        .obj()
        .query()
        .await?;

    assert_eq!(
        pieces,
        [json!({"first": "Pride", "whole": "Pride and Prejudice", "last": "dice"})]
    );
    Ok(())
}

#[tokio::test]
async fn a_computed_value_is_tested_for_null() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` WHERE LOWER(title) IS NOT NULL ORDER BY title")
        .obj()
        .query()
        .await?;

    assert_eq!(rows.len(), 11);
    assert_eq!(rows[0], json!({"title": "1984"}));
    Ok(())
}

#[tokio::test]
async fn a_document_name_selects_documents_by_list_and_by_parameter() -> TestResult {
    let db = setup_enterprise().await?;

    let mut by_list: Vec<Value> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` WHERE __name__ IN ('book-08', 'book-10')")
        .obj()
        .query()
        .await?;
    by_list.sort_by_key(ToString::to_string);
    let by_parameter: Vec<Value> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` WHERE __name__ = ? AND stock > 0")
        .params(["book-08"])
        .obj()
        .query()
        .await?;

    assert_eq!(
        by_list,
        [json!({"title": "1984"}), json!({"title": "Dune"})]
    );
    assert_eq!(by_parameter, [json!({"title": "1984"})]);
    Ok(())
}

#[tokio::test]
async fn a_group_by_returns_the_groups_that_pass_having() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT genre, COUNT(*) AS books FROM `pipeline-books` \
             GROUP BY genre HAVING AVG(rating) > 4.0 ORDER BY genre",
        )
        .obj()
        .query()
        .await?;

    assert_eq!(
        rows,
        [
            json!({"genre": "Dystopian", "books": 2}),
            json!({"genre": "Fantasy", "books": 1}),
            json!({"genre": "Magical Realism", "books": 1}),
            json!({"genre": "Psychological Thriller", "books": 1}),
            json!({"genre": "Romance", "books": 1}),
            json!({"genre": "Science Fiction", "books": 2}),
            json!({"genre": "Southern Gothic", "books": 1}),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn select_distinct_returns_one_row_per_value() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql("SELECT DISTINCT genre FROM `pipeline-books` ORDER BY genre")
        .obj()
        .query()
        .await?;

    assert_eq!(rows.len(), 9);
    Ok(())
}

#[tokio::test]
async fn a_bernoulli_sample_of_0_percent_keeps_no_row_and_of_100_percent_every_row() -> TestResult {
    let db = setup_enterprise().await?;
    let books: Vec<Value> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` ORDER BY title")
        .obj()
        .query()
        .await?;

    let none: Vec<Value> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` TABLESAMPLE BERNOULLI (0 PERCENT) ORDER BY title")
        .obj()
        .query()
        .await?;
    let all: Vec<Value> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` TABLESAMPLE BERNOULLI (@share PERCENT) ORDER BY title")
        .param("share", 100)
        .obj()
        .query()
        .await?;

    assert!(none.is_empty());
    assert_eq!(all, books);
    Ok(())
}

#[tokio::test]
async fn a_union_appends_the_rows_of_the_other_collection() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT title AS name FROM `pipeline-books` \
             UNION ALL SELECT name FROM `pipeline-restaurants` ORDER BY name",
        )
        .obj()
        .query()
        .await?;

    let names = names(&rows);
    assert_eq!(names.len(), 20);
    assert_eq!(names[..2], ["1984", "Café Crêpe"]);
    assert!(names.contains(&"Waffle and Bacon Co") && names.contains(&"Untitled draft"));
    Ok(())
}

#[tokio::test]
async fn a_text_search_returns_the_hits_by_relevance() -> TestResult {
    let db = setup_enterprise().await?;

    let hits: Vec<Relevant> = db
        .fluent()
        .sql(
            "SELECT name, score() AS relevance FROM `pipeline-restaurants` \
             WHERE document_matches(@query) ORDER BY score() DESC",
        )
        .param("query", "waffles -bacon")
        .obj()
        .query()
        .await?;

    // The first two hits tie, and Firestore does not order a tie.
    let mut best: Vec<&str> = hits[..2].iter().map(|hit| hit.name.as_str()).collect();
    best.sort_unstable();
    assert_eq!(best, ["Gluten-Free Kitchen", "The \"Best\" Diner"]);
    assert_eq!(hits[2].name, "Coffee Corner");
    assert_eq!(hits.len(), 3);
    assert!(hits
        .windows(2)
        .all(|pair| pair[0].relevance >= pair[1].relevance));
    Ok(())
}

#[tokio::test]
async fn a_geo_distance_condition_returns_the_places_nearest_first() -> TestResult {
    let db = setup_enterprise().await?;
    let center = FirestoreLatLng(FirestoreGeoPoint {
        latitude: 38.989177,
        longitude: -107.065076,
    });

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT name FROM `pipeline-restaurants` \
             WHERE geo_distance(location, @center) <= 5000 \
             ORDER BY geo_distance(location, @center)",
        )
        .param("center", &center)
        .obj()
        .query()
        .await?;

    assert_eq!(
        names(&rows),
        [
            "The \"Best\" Diner",
            "Gluten-Free Kitchen",
            "Sunrise Waffle House",
            "Slice of New York",
            "Pancake Palace",
            "Café Crêpe",
            "Waffle and Bacon Co",
        ]
    );
    Ok(())
}

#[tokio::test]
async fn an_inner_join_returns_the_restaurants_that_have_reviews() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<RestaurantRating> = db
        .fluent()
        .sql(
            "SELECT rest.name, r.rating AS rating \
             FROM `pipeline-restaurants` AS rest \
             JOIN collection_group('reviews') AS r ON r.restaurant = rest.__name__ \
             ORDER BY rest.name, r.rating DESC",
        )
        .options(per_row_allowed())
        .obj()
        .query()
        .await?;

    assert_eq!(rated(&rows), REVIEWS);
    Ok(())
}

#[tokio::test]
async fn a_left_join_keeps_the_restaurants_without_reviews() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<RestaurantRating> = db
        .fluent()
        .sql(
            "SELECT rest.name, r.rating AS rating \
             FROM `pipeline-restaurants` AS rest \
             LEFT JOIN collection_group('reviews') AS r ON r.restaurant = rest.__name__ \
             ORDER BY rest.name, r.rating DESC",
        )
        .options(per_row_allowed())
        .obj()
        .query()
        .await?;

    let reviewed: Vec<_> = rated(&rows)
        .into_iter()
        .filter(|(_, rating)| rating.is_some())
        .collect();
    assert_eq!(reviewed, REVIEWS);
    assert_eq!(rows.len(), 12);
    assert_eq!(rated(&rows)[0], ("Café Crêpe", None));
    Ok(())
}

#[tokio::test]
async fn a_join_on_a_document_name_looks_the_document_up() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT r.rating, rest.name AS restaurant \
             FROM collection_group('reviews') AS r \
             JOIN `pipeline-restaurants` AS rest ON rest.__name__ = r.restaurant \
             ORDER BY rest.name, r.rating DESC",
        )
        .options(per_row_allowed())
        .obj()
        .query()
        .await?;

    let expected =
        REVIEWS.map(|(restaurant, rating)| json!({"rating": rating, "restaurant": restaurant}));
    assert_eq!(rows, expected);
    Ok(())
}

#[tokio::test]
async fn a_scalar_subquery_returns_one_value_per_row() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<RestaurantAverage> = db
        .fluent()
        .sql(
            "SELECT rest.name, \
               (SELECT AVG(r.rating) AS average FROM collection_group('reviews') AS r \
                WHERE r.restaurant = rest.__name__) AS average_rating \
             FROM `pipeline-restaurants` AS rest ORDER BY rest.name",
        )
        .options(per_row_allowed())
        .obj()
        .query()
        .await?;

    let averages: Vec<(&str, f64)> = rows
        .iter()
        .filter_map(|row| Some((row.name.as_str(), row.average_rating?)))
        .collect();
    assert_eq!(rows.len(), 9);
    assert_eq!(
        averages,
        [
            ("Pancake Palace", 3.0),
            ("Slice of New York", 11.0 / 3.0),
            ("Sunrise Waffle House", 4.5),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn exists_keeps_the_restaurants_that_have_a_review() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT rest.name FROM `pipeline-restaurants` AS rest \
             WHERE EXISTS (SELECT 1 FROM collection_group('reviews') AS r \
                           WHERE r.restaurant = rest.__name__) \
             ORDER BY rest.name",
        )
        .options(per_row_allowed())
        .obj()
        .query()
        .await?;

    assert_eq!(
        names(&rows),
        [
            "Pancake Palace",
            "Slice of New York",
            "Sunrise Waffle House"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_cte_used_twice_returns_its_rows_twice() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "WITH good AS (SELECT title, rating FROM `pipeline-books` WHERE rating >= 4.5) \
             SELECT title FROM good UNION ALL SELECT title FROM good ORDER BY title",
        )
        .obj()
        .query()
        .await?;

    let titles: Vec<&str> = rows
        .iter()
        .map(|row| row["title"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        titles,
        [
            "Dune",
            "Dune",
            "Pride and Prejudice",
            "Pride and Prejudice",
            "The Lord of the Rings",
            "The Lord of the Rings",
        ]
    );
    Ok(())
}

#[tokio::test]
async fn unnest_with_offset_returns_each_element_with_its_position() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Value> = db
        .fluent()
        .sql(
            "SELECT b.title, tag, pos FROM `pipeline-books` AS b \
             CROSS JOIN UNNEST(b.tags) AS tag WITH OFFSET AS pos \
             WHERE b.genre = 'Science Fiction' ORDER BY b.title, pos",
        )
        .obj()
        .query()
        .await?;

    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows[..3],
        [
            json!({"title": "Dune", "tag": "politics", "pos": 0}),
            json!({"title": "Dune", "tag": "desert", "pos": 1}),
            json!({"title": "Dune", "tag": "ecology", "pos": 2}),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_join_into_a_subcollection_works_without_the_opt_in() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<RestaurantRating> = db
        .fluent()
        .sql(
            "SELECT rest.name, r.rating AS rating \
             FROM `pipeline-restaurants` AS rest \
             JOIN subcollection('reviews') AS r ON TRUE \
             ORDER BY rest.name, r.rating DESC",
        )
        .obj()
        .query()
        .await?;

    assert_eq!(rated(&rows), REVIEWS);
    Ok(())
}

/// Every collection a SQL write may change starts with this.
const WRITE_COLLECTION_PREFIX: &str = "sql-tests-";

#[derive(Serialize)]
struct Stock {
    #[serde(rename = "__name__")]
    name: FirestoreReference,
    sku: String,
    stock: i64,
    price: f64,
    extra: &'static str,
}

fn reference(db: &FirestoreDb, collection: &str, id: &str) -> FirestoreReference {
    FirestoreReference(format!("{}/{collection}/{id}", db.get_documents_path()))
}

fn stock(db: &FirestoreDb, collection: &str, id: &str, stock: i64, price: f64) -> Stock {
    Stock {
        name: reference(db, collection, id),
        sku: id.to_string(),
        stock,
        price,
        extra: "old",
    }
}

/// The fields of a seeded document, as `contents` reads them.
fn stocked(id: &str, stock: i64, price: f64) -> serde_json::Value {
    serde_json::json!({"sku": id, "stock": stock, "price": price, "extra": "old"})
}

fn documents<const N: usize>(documents: [(&str, serde_json::Value); N]) -> Contents {
    documents
        .into_iter()
        .map(|(id, fields)| (id.to_string(), fields))
        .collect()
}

/// The documents of a collection by ID, with their fields.
type Contents = BTreeMap<String, serde_json::Value>;

async fn contents(db: &FirestoreDb, collection: &str) -> TestResult<Contents> {
    let mut contents = Contents::new();
    for document in db.fluent().select().from(collection).query().await? {
        let id = document
            .name
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        let mut fields: BTreeMap<String, serde_json::Value> =
            FirestoreDb::deserialize_doc_to(&document)?;
        fields.retain(|field, _| !field.starts_with("_firestore_"));
        contents.insert(id, serde_json::to_value(fields)?);
    }
    Ok(contents)
}

/// The fields of the documents without their IDs, for a write that leaves Firestore to name them.
fn fields_only(contents: Contents) -> Vec<serde_json::Value> {
    let mut fields: Vec<serde_json::Value> = contents.into_values().collect();
    fields.sort_by_key(ToString::to_string);
    fields
}

/// Empties `collection`, which a failed run may have left documents in.
async fn empty(db: &FirestoreDb, collection: &str) -> TestResult {
    assert!(
        collection.starts_with(WRITE_COLLECTION_PREFIX),
        "{collection} is not a test collection"
    );
    db.fluent()
        .pipeline()
        .delete()
        .from(collection)
        .execute()
        .await?;
    Ok(())
}

/// Replaces the documents of `collection` with the stock `seed` names, as `(id, stock, price)`.
async fn seed(db: &FirestoreDb, collection: &str, seed: &[(&str, i64, f64)]) -> TestResult {
    empty(db, collection).await?;
    if !seed.is_empty() {
        db.fluent()
            .pipeline()
            .insert()
            .literals(
                seed.iter()
                    .map(|(id, stock_, price)| stock(db, collection, id, *stock_, *price))
                    .collect::<Vec<_>>(),
            )
            .execute()
            .await?;
    }
    Ok(())
}

/// A collection that holds the stock `stock_` names.
async fn collection(
    db: &FirestoreDb,
    name: &str,
    stock_: &[(&str, i64, f64)],
) -> TestResult<String> {
    let collection = format!("{WRITE_COLLECTION_PREFIX}{name}");
    seed(db, &collection, stock_).await?;
    Ok(collection)
}

/// Returns `sql` once it is clear that its write changes the test collections alone: it writes
/// to one of them, or it changes the documents of its source, which is one of them.
fn guarded(db: &FirestoreDb, sql: String) -> String {
    guarded_with(db, sql, &FirestoreSqlOptions::new())
}

fn guarded_with(db: &FirestoreDb, sql: String, options: &FirestoreSqlOptions) -> String {
    let translated =
        FirestoreSqlStatement::translate(db, &sql, &FirestoreSqlParameters::new(), options);
    let Ok(FirestoreSqlStatement::Write(write)) = translated else {
        panic!("{sql} does not translate to a write: {translated:?}");
    };
    let in_test_collections = match &write.write {
        FirestorePipelineWrite::Insert(FirestoreInsertWrite { target })
        | FirestorePipelineWrite::Upsert(FirestoreUpsertWrite { target, .. }) => target
            .as_ref()
            .is_some_and(|target| target.collection_path.starts_with(WRITE_COLLECTION_PREFIX)),
        FirestorePipelineWrite::Update(_) | FirestorePipelineWrite::Delete => {
            match &write.pipeline.source {
                FirestorePipelineSource::Collection(source) => {
                    source.parent.is_none()
                        && source.collection_id.starts_with(WRITE_COLLECTION_PREFIX)
                }
                FirestorePipelineSource::Documents(source) => source
                    .paths
                    .iter()
                    .all(|path| path.starts_with(WRITE_COLLECTION_PREFIX)),
                _ => false,
            }
        }
    };
    assert!(
        in_test_collections,
        "the write leaves the test collections: {write:?}"
    );
    sql
}

#[tokio::test]
async fn an_insert_with_values_writes_the_documents() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(&db, "insert-values", &[]).await?;

    let inserted = db
        .fluent()
        .sql(guarded(
            &db,
            format!(
                "INSERT INTO `{collection}` (__name__, sku, stock, price, extra) VALUES \
                 ('a-1', 'a-1', 3, 9.5, 'old'), ('a-2', 'a-2', 0, 4.0, 'old'), \
                 ('a-3', 'a-3', 0, 1.25, 'old')"
            ),
        ))
        .execute()
        .await?;

    assert_eq!(
        contents(&db, &collection).await?,
        documents([
            ("a-1", stocked("a-1", 3, 9.5)),
            ("a-2", stocked("a-2", 0, 4.0)),
            ("a-3", stocked("a-3", 0, 1.25)),
        ])
    );
    assert_eq!(inserted.documents_modified, 3);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_select_writes_the_selected_rows() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(&db, "insert-select", &[]).await?;
    let source = format!("{WRITE_COLLECTION_PREFIX}insert-select-source");
    seed(
        &db,
        &source,
        &[("a-1", 3, 9.5), ("a-2", 0, 4.0), ("a-3", 0, 1.25)],
    )
    .await?;

    let inserted = db
        .fluent()
        .sql(guarded(
            &db,
            format!("INSERT INTO `{collection}` SELECT sku, stock FROM `{source}` WHERE stock = 0"),
        ))
        .execute()
        .await?;

    // The write does not name its documents, so Firestore picks their IDs.
    assert_eq!(
        fields_only(contents(&db, &collection).await?),
        [
            serde_json::json!({"sku": "a-2", "stock": 0}),
            serde_json::json!({"sku": "a-3", "stock": 0}),
        ]
    );
    assert_eq!(inserted.documents_modified, 2);
    for collection in [&collection, &source] {
        empty(&db, collection).await?;
    }
    Ok(())
}

#[tokio::test]
async fn an_insert_or_replace_replaces_the_stored_document() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(
        &db,
        "insert-or-replace",
        &[("a-1", 3, 9.5), ("a-2", 1, 2.0)],
    )
    .await?;

    let upserted = db
        .fluent()
        .sql(guarded(
            &db,
            format!(
                "INSERT OR REPLACE INTO `{collection}` (__name__, sku, stock) VALUES \
                 ('a-1', 'a-1', 9), ('a-3', 'a-3', 5)"
            ),
        ))
        .execute()
        .await?;

    assert_eq!(
        contents(&db, &collection).await?,
        documents([
            ("a-1", serde_json::json!({"sku": "a-1", "stock": 9})),
            ("a-2", stocked("a-2", 1, 2.0)),
            ("a-3", serde_json::json!({"sku": "a-3", "stock": 5})),
        ])
    );
    assert_eq!(upserted.documents_modified, 2);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_do_nothing_writes_only_the_rows_without_a_stored_document() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(
        &db,
        "on-conflict-nothing",
        &[("a-1", 3, 9.5), ("a-2", 0, 4.0)],
    )
    .await?;

    let inserted = db
        .fluent()
        .sql(guarded(
            &db,
            format!(
                "INSERT INTO `{collection}` (__name__, sku, stock) \
                 VALUES ('a-1', 'a-1', 99), ('a-3', 'a-3', 5) \
                 ON CONFLICT (__name__) DO NOTHING"
            ),
        ))
        .execute()
        .await?;

    assert_eq!(
        contents(&db, &collection).await?,
        documents([
            ("a-1", stocked("a-1", 3, 9.5)),
            ("a-2", stocked("a-2", 0, 4.0)),
            ("a-3", serde_json::json!({"sku": "a-3", "stock": 5})),
        ])
    );
    assert_eq!(inserted.documents_modified, 1);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_do_nothing_over_select_star_skips_the_documents_that_exist() -> TestResult {
    let db = setup_enterprise().await?;
    let collection =
        collection(&db, "on-conflict-star", &[("a-1", 3, 9.5), ("a-2", 0, 4.0)]).await?;
    let source = format!("{WRITE_COLLECTION_PREFIX}on-conflict-star-source");
    seed(&db, &source, &[("a-1", 99, 1.0), ("a-3", 5, 2.0)]).await?;
    let options = FirestoreSqlOptions::new().allow_subqueries();

    db.fluent()
        .sql(guarded_with(
            &db,
            format!(
                "INSERT INTO `{collection}` SELECT * FROM `{source}` ON CONFLICT (__name__) DO NOTHING"
            ),
            &options,
        ))
        .options(options)
        .execute()
        .await?;

    assert_eq!(
        contents(&db, &collection).await?,
        documents([
            ("a-1", stocked("a-1", 3, 9.5)),
            ("a-2", stocked("a-2", 0, 4.0)),
            ("a-3", stocked("a-3", 5, 2.0)),
        ])
    );
    empty(&db, &collection).await?;
    empty(&db, &source).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_do_update_sets_the_listed_fields_of_the_stored_document() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(
        &db,
        "on-conflict-update",
        &[("a-1", 3, 9.5), ("a-2", 0, 4.0)],
    )
    .await?;

    let written = db
        .fluent()
        .sql(guarded(
            &db,
            format!(
                "INSERT INTO `{collection}` (__name__, sku, stock) \
                 VALUES ('a-1', 'a-1', 9), ('a-3', 'a-3', 5) \
                 ON CONFLICT (__name__) DO UPDATE \
                 SET stock = stock + excluded.stock, dims.w = excluded.stock"
            ),
        ))
        .execute()
        .await?;

    // `price` and `extra` are named by neither side of the conflict, so the stored values stay.
    assert_eq!(
        contents(&db, &collection).await?,
        documents([
            (
                "a-1",
                serde_json::json!({
                    "sku": "a-1", "stock": 12, "price": 9.5, "extra": "old", "dims": {"w": 9}
                })
            ),
            ("a-2", stocked("a-2", 0, 4.0)),
            ("a-3", serde_json::json!({"sku": "a-3", "stock": 5})),
        ])
    );
    assert_eq!(written.documents_modified, 2);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_do_update_where_leaves_the_stored_documents_that_fail_the_condition(
) -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(
        &db,
        "on-conflict-where",
        &[("a-1", 3, 9.5), ("a-2", 0, 4.0)],
    )
    .await?;

    let written = db
        .fluent()
        .sql(guarded(
            &db,
            format!(
                "INSERT INTO `{collection}` (__name__, sku, stock) \
                 VALUES ('a-1', 'a-1', 7), ('a-2', 'a-2', 7), ('a-4', 'a-4', 7) \
                 ON CONFLICT (__name__) DO UPDATE SET stock = excluded.stock WHERE stock = 0"
            ),
        ))
        .execute()
        .await?;

    let stored = contents(&db, &collection).await?;
    assert_eq!(stored["a-1"]["stock"], 3);
    assert_eq!(stored["a-2"]["stock"], 7);
    assert_eq!(stored["a-4"]["stock"], 7);
    assert_eq!(written.documents_modified, 2);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn an_update_sets_the_fields_of_the_selected_documents() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(
        &db,
        "update",
        &[("a-1", 3, 9.5), ("a-2", 0, 4.0), ("a-3", 0, 1.25)],
    )
    .await?;

    let updated = db
        .fluent()
        .sql(guarded(
            &db,
            format!(
                "UPDATE `{collection}` SET status = 'sold_out', dims.w = 2, dims.h = price \
                 WHERE stock = 0"
            ),
        ))
        .execute()
        .await?;

    let written = contents(&db, &collection).await?;
    assert_eq!(
        written["a-2"]["dims"],
        serde_json::json!({"w": 2, "h": 4.0})
    );
    assert_eq!(
        written["a-3"]["dims"],
        serde_json::json!({"w": 2, "h": 1.25})
    );
    assert_eq!(written["a-3"]["status"], "sold_out");
    assert_eq!(written["a-1"], stocked("a-1", 3, 9.5));
    assert_eq!(updated.documents_modified, 2);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn a_delete_removes_the_selected_documents() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(
        &db,
        "delete",
        &[("a-1", 3, 9.5), ("a-2", 0, 4.0), ("a-3", 0, 1.25)],
    )
    .await?;

    let deleted = db
        .fluent()
        .sql(guarded(
            &db,
            format!("DELETE FROM `{collection}` WHERE stock = 0"),
        ))
        .execute()
        .await?;

    let remaining = contents(&db, &collection).await?;
    assert_eq!(remaining.keys().collect::<Vec<_>>(), ["a-1"]);
    assert_eq!(deleted.documents_modified, 2);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn a_write_in_a_transaction_changes_the_documents_at_the_commit() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = collection(&db, "transaction", &[("a-1", 3, 9.5), ("a-2", 0, 4.0)]).await?;
    let sql = guarded(
        &db,
        format!("UPDATE `{collection}` SET stock = stock + 10 WHERE stock >= 0"),
    );

    let updated = db
        .run_transaction(|db, transaction| {
            let sql = sql.clone();
            async move {
                let updated = db
                    .fluent()
                    .sql(sql)
                    .execute_in_transaction(transaction)
                    .await?;
                Ok::<_, BackoffError<FirestoreError>>(updated)
            }
            .boxed()
        })
        .await?;

    let written = contents(&db, &collection).await?;
    assert_eq!(written["a-1"]["stock"], 13);
    assert_eq!(written["a-2"]["stock"], 10);
    assert_eq!(updated.documents_modified, 2);
    empty(&db, &collection).await?;
    Ok(())
}

#[tokio::test]
async fn a_terminal_of_the_other_kind_fails_before_sending() -> TestResult {
    let db = setup_enterprise().await?;
    let collection = format!("{WRITE_COLLECTION_PREFIX}terminals");
    let delete = guarded(&db, format!("DELETE FROM `{collection}` WHERE TRUE"));
    let field = |result: FirestoreResult<_>| match result {
        Err(FirestoreError::InvalidParametersError(error)) => error.public.field,
        Err(error) => panic!("{error:?}"),
        Ok(_) => panic!("the call succeeded"),
    };

    assert_eq!(
        field(db.fluent().sql(&delete).query().await.map(drop)),
        "sql"
    );
    assert_eq!(
        field(db.fluent().sql(&delete).explain().execute().await.map(drop)),
        "explain"
    );
    assert_eq!(
        field(
            db.fluent()
                .sql(format!("SELECT * FROM `{collection}`"))
                .execute()
                .await
                .map(drop)
        ),
        "sql"
    );
    Ok(())
}

#[tokio::test]
async fn show_collections_lists_the_subcollections_of_a_document() -> TestResult {
    let db = setup_enterprise().await?;

    let FirestoreSqlOutcome::Collections(collections) = db
        .fluent()
        .sql("SHOW COLLECTIONS IN `pipeline-restaurants/rest-01`")
        .run()
        .await?
    else {
        panic!("SHOW COLLECTIONS returns collection IDs");
    };

    assert_eq!(collections, [FirestoreCollectionId::from_static("reviews")]);
    Ok(())
}

#[tokio::test]
async fn explain_text_returns_the_plan() -> TestResult {
    let db = setup_enterprise().await?;

    for (prefix, documents) in [("EXPLAIN", 0), ("EXPLAIN ANALYZE", 2)] {
        let FirestoreSqlOutcome::Rows(rows) = db
            .fluent()
            .sql(format!(
                "{prefix} SELECT title FROM `pipeline-books` WHERE genre = 'Dystopian'"
            ))
            .run()
            .await?
        else {
            panic!("{prefix} returns rows");
        };
        let mut items: Vec<_> = rows.try_collect().await?;

        let closing = items
            .pop()
            .expect("a document-less item must close the stream");
        assert_eq!(items.len(), documents, "{prefix}");
        assert!(closing.document.is_none(), "{prefix}");
        assert!(
            closing
                .metadata
                .explain_stats
                .is_some_and(|stats| !stats.text.is_empty()),
            "{prefix}"
        );
    }
    Ok(())
}
