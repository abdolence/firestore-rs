use crate::common::setup_enterprise;
use firestore::errors::{BackoffError, FirestoreError};
use firestore::*;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod common;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BOOKS: &str = "pipeline-books";

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RatedBook {
    title: String,
    rating: f64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Title {
    title: String,
}

#[tokio::test]
async fn a_select_returns_the_rows_of_the_fluent_pipeline() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<RatedBook> = db
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
    let fluent: Vec<RatedBook> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .filter(|q| {
            q.for_all([
                q.field("rating").greater_than_or_equal(4.2),
                q.field("genre").neq("Fantasy"),
            ])
        })
        .sort(|e| [e.field("rating").desc(), e.field("title").asc()])
        .fields(|e| [e.field("title"), e.field("rating")])
        .limit(5)
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 5);
    assert_eq!(translated, fluent);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Pieces {
    first: String,
    whole: String,
    last: String,
}

#[tokio::test]
async fn a_substring_position_counts_from_1_and_a_negative_one_from_the_end() -> TestResult {
    let db = setup_enterprise().await?;

    let pieces: Vec<Pieces> = db
        .fluent()
        .sql(
            "SELECT SUBSTR(title, 1, 5) AS first, SUBSTR(title, 0) AS whole, \
             SUBSTR(title, -4) AS last FROM `pipeline-books` WHERE __name__ = 'book-02'",
        )
        .obj()
        .query()
        .await?;

    let expected = Pieces {
        first: "Pride".to_string(),
        whole: "Pride and Prejudice".to_string(),
        last: "dice".to_string(),
    };
    assert_eq!(pieces, [expected]);
    Ok(())
}

#[tokio::test]
async fn a_computed_value_is_tested_for_null_by_the_fluent_comparison() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<Title> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` WHERE LOWER(title) IS NOT NULL ORDER BY title")
        .obj()
        .query()
        .await?;
    let fluent: Vec<Title> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .filter(|q| {
            q.condition(
                FirestoreExpr::field("title")
                    .to_lower()
                    .not_equal(None::<bool>),
            )
        })
        .sort(|e| [e.field("title").asc()])
        .fields(|e| [e.field("title")])
        .obj()
        .query()
        .await?;

    assert!(!fluent.is_empty());
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_document_name_selects_the_documents_of_the_fluent_pipeline() -> TestResult {
    let db = setup_enterprise().await?;
    let book = |id: &str| FirestoreReference(format!("{}/{BOOKS}/{id}", db.get_documents_path()));

    // The first statement reads by path, and the second compares `__name__` in a filter.
    let by_path: Vec<Title> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` WHERE __name__ IN ('book-08', 'book-10')")
        .obj()
        .query()
        .await?;
    let by_filter: Vec<Title> = db
        .fluent()
        .sql("SELECT title FROM `pipeline-books` WHERE __name__ = ? AND stock > 0")
        .params(["book-08"])
        .obj()
        .query()
        .await?;
    let fluent: Vec<Title> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .filter(|q| {
            q.for_any([
                q.field("__name__").eq(book("book-08")),
                q.field("__name__").eq(book("book-10")),
            ])
        })
        .sort(|e| [e.field("title").asc()])
        .fields(|e| [e.field("title")])
        .obj()
        .query()
        .await?;

    let mut by_path_titles = by_path;
    by_path_titles.sort_by(|left, right| left.title.cmp(&right.title));
    assert_eq!(fluent.len(), 2);
    assert_eq!(by_path_titles, fluent);
    assert_eq!(by_filter, fluent[..1]);
    Ok(())
}

const RESTAURANTS: &str = "pipeline-restaurants";

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct GenreBooks {
    genre: Option<String>,
    books: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Genre {
    genre: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Name {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Relevant {
    name: String,
    relevance: f64,
}

#[tokio::test]
async fn a_group_by_returns_the_groups_of_the_fluent_aggregate() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<GenreBooks> = db
        .fluent()
        .sql(
            "SELECT genre, COUNT(*) AS books FROM `pipeline-books` \
             GROUP BY genre HAVING AVG(rating) > 4.0 ORDER BY genre",
        )
        .obj()
        .query()
        .await?;
    let fluent: Vec<GenreBooks> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .aggregate_grouped(
            |e| [e.field("genre")],
            |e| {
                [
                    e.count_all().alias("books"),
                    e.field("rating").average().alias("_agg0"),
                ]
            },
        )
        .filter(|q| q.field("_agg0").greater_than(4.0))
        .sort(|e| [e.field("genre").asc()])
        .fields(|e| [e.field("genre"), e.field("books")])
        .obj()
        .query()
        .await?;

    assert!(!fluent.is_empty());
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn select_distinct_returns_the_values_of_the_fluent_distinct() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<Genre> = db
        .fluent()
        .sql("SELECT DISTINCT genre FROM `pipeline-books` ORDER BY genre")
        .obj()
        .query()
        .await?;
    let fluent: Vec<Genre> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .distinct(|e| [e.field("genre")])
        .sort(|e| [e.field("genre").asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 9);
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_union_returns_the_rows_of_the_fluent_union() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<Name> = db
        .fluent()
        .sql(
            "SELECT title AS name FROM `pipeline-books` \
             UNION ALL SELECT name FROM `pipeline-restaurants` ORDER BY name",
        )
        .obj()
        .query()
        .await?;
    let fluent: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .fields(|e| [e.field("title").alias("name")])
        .union(|p| p.from(RESTAURANTS).fields(|e| [e.field("name")]))
        .sort(|e| [e.field("name").asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 20);
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_text_search_returns_the_hits_of_the_fluent_search() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<Relevant> = db
        .fluent()
        .sql(
            "SELECT name, score() AS relevance FROM `pipeline-restaurants` \
             WHERE document_matches(@query) ORDER BY score() DESC",
        )
        .param("query", "waffles -bacon")
        .obj()
        .query()
        .await?;
    let fluent: Vec<Relevant> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| {
            s.text(|_| "waffles -bacon")
                .sort_by_score()
                .score_field("relevance")
        })
        .fields(|e| [e.field("name"), e.field("relevance")])
        .obj()
        .query()
        .await?;

    assert!(!fluent.is_empty());
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_geo_distance_condition_returns_the_places_of_the_fluent_search() -> TestResult {
    let db = setup_enterprise().await?;
    let center = FirestoreLatLng(FirestoreGeoPoint {
        latitude: 38.989177,
        longitude: -107.065076,
    });

    let translated: Vec<Name> = db
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
    let fluent: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| s.geo_within("location", center, 5000.0).sort_by_distance())
        .fields(|e| [e.field("name")])
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 7);
    assert_eq!(translated, fluent);
    Ok(())
}

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
struct ReviewedRestaurant {
    rating: i64,
    restaurant: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct BookTag {
    title: String,
    tag: String,
    pos: i64,
}

fn per_row_allowed() -> FirestoreSqlOptions {
    FirestoreSqlOptions::new().allow_subqueries()
}

#[tokio::test]
async fn an_inner_join_returns_the_rows_of_the_fluent_join() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<RestaurantRating> = db
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
    let fluent: Vec<RestaurantRating> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .add_fields(|e| {
            [e.array_subquery(|p| {
                p.from("reviews")
                    .all_descendants()
                    .filter(|q| q.field("restaurant").eq(q.variable("rest___name__")))
            })
            .alias("r")]
        })
        .unnest(|e| e.field("r"))
        .sort(|e| [e.field("name").asc(), e.field("r.rating").desc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("name")),
                e.field("r.rating").alias("rating"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 6);
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_left_join_returns_the_rows_of_the_fluent_join_and_the_restaurants_without_reviews(
) -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<RestaurantRating> = db
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
    let fluent: Vec<RestaurantRating> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .add_fields(|e| {
            [e.array_subquery(|p| {
                p.from("reviews")
                    .all_descendants()
                    .filter(|q| q.field("restaurant").eq(q.variable("rest___name__")))
            })
            .alias("r")]
        })
        .add_fields(|e| {
            [e.conditional(
                e.field("r").array_length().equal(0),
                e.array([FirestoreExpr::constant(None::<bool>)]),
                e.field("r"),
            )
            .alias("r")]
        })
        .unnest(|e| e.field("r"))
        .sort(|e| [e.field("name").asc(), e.field("r.rating").desc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("name")),
                e.field("r.rating").alias("rating"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 12);
    assert!(fluent.iter().any(|row| row.rating.is_none()));
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_join_on_a_document_name_returns_the_rows_of_the_fluent_lookup() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<ReviewedRestaurant> = db
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
    let fluent: Vec<ReviewedRestaurant> = db
        .fluent()
        .pipeline()
        .select()
        .from("reviews")
        .all_descendants()
        .define_variables(|e| [e.field("restaurant").alias("r_restaurant")])
        .add_fields(|e| {
            [e.array_subquery(|p| {
                p.from(RESTAURANTS).filter(|q| {
                    q.field("__name__")
                        .is_in(q.values().array([q.variable("r_restaurant")]))
                })
            })
            .alias("rest")]
        })
        .unnest(|e| e.field("rest"))
        .sort(|e| [e.field("rest.name").asc(), e.field("rating").desc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("rating")),
                e.field("rest.name").alias("restaurant"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 6);
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_scalar_subquery_returns_the_values_of_the_fluent_scalar_subquery() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<RestaurantAverage> = db
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
    let fluent: Vec<RestaurantAverage> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .sort(|e| [e.field("name").asc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("name")),
                e.scalar_subquery(|p| {
                    p.from("reviews")
                        .all_descendants()
                        .filter(|q| q.field("restaurant").eq(q.variable("rest___name__")))
                        .aggregate(|e| [e.field("rating").average().alias("average")])
                })
                .alias("average_rating"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 9);
    assert!(fluent.iter().any(|row| row.average_rating.is_none()));
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn exists_returns_the_rows_of_the_fluent_array_length_test() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<Name> = db
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
    let fluent: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .define_variables(|e| [e.field("__name__").alias("rest___name__")])
        .filter(|q| {
            q.expr(
                q.values()
                    .array_subquery(|p| {
                        p.from("reviews")
                            .all_descendants()
                            .filter(|q| q.field("restaurant").eq(q.variable("rest___name__")))
                            .limit(1)
                    })
                    .array_length(),
            )
            .greater_than(0)
        })
        .sort(|e| [e.field("name").asc()])
        .fields(|e| [e.field("name")])
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 3);
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_cte_used_twice_runs_again_for_each_reference_like_the_fluent_union() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<Title> = db
        .fluent()
        .sql(
            "WITH good AS (SELECT title, rating FROM `pipeline-books` WHERE rating >= 4.5) \
             SELECT title FROM good UNION ALL SELECT title FROM good ORDER BY title",
        )
        .obj()
        .query()
        .await?;
    let fluent: Vec<Title> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .filter(|q| q.field("rating").greater_than_or_equal(4.5))
        .fields(|e| [e.field("title"), e.field("rating")])
        .fields(|e| [e.field("title")])
        .union(|p| {
            p.from(BOOKS)
                .filter(|q| q.field("rating").greater_than_or_equal(4.5))
                .fields(|e| [e.field("title"), e.field("rating")])
                .fields(|e| [e.field("title")])
        })
        .sort(|e| [e.field("title").asc()])
        .obj()
        .query()
        .await?;

    assert!(!fluent.is_empty());
    assert_eq!(fluent.len() % 2, 0);
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn unnest_with_offset_returns_the_rows_of_the_fluent_unnest() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<BookTag> = db
        .fluent()
        .sql(
            "SELECT b.title, tag, pos FROM `pipeline-books` AS b \
             CROSS JOIN UNNEST(b.tags) AS tag WITH OFFSET AS pos \
             WHERE b.genre = 'Science Fiction' ORDER BY b.title, pos",
        )
        .obj()
        .query()
        .await?;
    let fluent: Vec<BookTag> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .unnest_with_index_field(|e| e.field("tags").alias("tag"), "pos")
        .filter(|q| q.field("genre").eq("Science Fiction"))
        .sort(|e| [e.field("title").asc(), e.field("pos").asc()])
        .fields(|e| [e.field("title"), e.field("tag"), e.field("pos")])
        .obj()
        .query()
        .await?;

    assert!(!fluent.is_empty());
    assert_eq!(translated, fluent);
    Ok(())
}

#[tokio::test]
async fn a_join_into_a_subcollection_works_without_the_opt_in() -> TestResult {
    let db = setup_enterprise().await?;

    let translated: Vec<RestaurantRating> = db
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
    let fluent: Vec<RestaurantRating> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .add_fields(|e| [e.array_subquery(|p| p.subcollection("reviews")).alias("r")])
        .unnest(|e| e.field("r"))
        .sort(|e| [e.field("name").asc(), e.field("r.rating").desc()])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field("name")),
                e.field("r.rating").alias("rating"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(fluent.len(), 6);
    assert_eq!(translated, fluent);
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

#[derive(Serialize)]
struct Replacement {
    #[serde(rename = "__name__")]
    name: FirestoreReference,
    sku: String,
    stock: i64,
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

fn replacement(db: &FirestoreDb, collection: &str, id: &str, stock: i64) -> Replacement {
    Replacement {
        name: reference(db, collection, id),
        sku: id.to_string(),
        stock,
    }
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
fn fields_only(contents: Contents) -> Vec<String> {
    let mut fields: Vec<String> = contents.values().map(ToString::to_string).collect();
    fields.sort();
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

/// Two collections, one for the SQL write and one for the same write in the fluent API, each
/// holding the stock `stock_` names.
async fn collections(
    db: &FirestoreDb,
    name: &str,
    stock_: &[(&str, i64, f64)],
) -> TestResult<(String, String)> {
    let by_sql = format!("{WRITE_COLLECTION_PREFIX}{name}-sql");
    let by_fluent = format!("{WRITE_COLLECTION_PREFIX}{name}-fluent");
    seed(db, &by_sql, stock_).await?;
    seed(db, &by_fluent, stock_).await?;
    Ok((by_sql, by_fluent))
}

/// Returns `sql` once it is clear that its write changes the test collections alone: it writes
/// to one of them, or it changes the documents of its source, which is one of them.
fn guarded(db: &FirestoreDb, sql: String) -> String {
    let translated = FirestoreSqlStatement::translate(
        db,
        &sql,
        &FirestoreSqlParameters::new(),
        &FirestoreSqlOptions::new(),
    );
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
async fn an_insert_with_values_writes_the_documents_of_the_fluent_insert() -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(&db, "insert-values", &[]).await?;

    let inserted = db
        .fluent()
        .sql(guarded(
            &db,
            format!(
                "INSERT INTO `{by_sql}` (__name__, sku, stock, price, extra) VALUES \
                 ('a-1', 'a-1', 3, 9.5, 'old'), ('a-2', 'a-2', 0, 4.0, 'old'), \
                 ('a-3', 'a-3', 0, 1.25, 'old')"
            ),
        ))
        .execute()
        .await?;
    let fluent_inserted = db
        .fluent()
        .pipeline()
        .insert()
        .into(&by_fluent)
        .literals([
            stock(&db, &by_fluent, "a-1", 3, 9.5),
            stock(&db, &by_fluent, "a-2", 0, 4.0),
            stock(&db, &by_fluent, "a-3", 0, 1.25),
        ])
        .atomic()
        .execute()
        .await?;

    let written = contents(&db, &by_sql).await?;
    assert_eq!(written.len(), 3);
    assert_eq!(written, contents(&db, &by_fluent).await?);
    assert_eq!(inserted, fluent_inserted);
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_select_writes_the_documents_of_the_fluent_insert() -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(&db, "insert-select", &[]).await?;
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
            format!("INSERT INTO `{by_sql}` SELECT sku, stock FROM `{source}` WHERE stock = 0"),
        ))
        .execute()
        .await?;
    let fluent_inserted = db
        .fluent()
        .pipeline()
        .insert()
        .into(&by_fluent)
        .from(&source)
        .filter(|q| q.field("stock").eq(0))
        .fields(|e| [e.field("sku"), e.field("stock")])
        .atomic()
        .execute()
        .await?;

    // Neither write names its documents, so Firestore picks their IDs.
    let written = fields_only(contents(&db, &by_sql).await?);
    assert_eq!(written.len(), 2);
    assert_eq!(written, fields_only(contents(&db, &by_fluent).await?));
    assert_eq!(inserted, fluent_inserted);
    for collection in [&by_sql, &by_fluent, &source] {
        empty(&db, collection).await?;
    }
    Ok(())
}

#[tokio::test]
async fn an_insert_or_replace_writes_the_documents_of_the_fluent_upsert() -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(
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
                "INSERT OR REPLACE INTO `{by_sql}` (__name__, sku, stock) VALUES \
                 ('a-1', 'a-1', 9), ('a-3', 'a-3', 5)"
            ),
        ))
        .execute()
        .await?;
    let fluent_upserted = db
        .fluent()
        .pipeline()
        .upsert(|_| Vec::<FirestoreSelection>::new())
        .into(&by_fluent)
        .literals([
            replacement(&db, &by_fluent, "a-1", 9),
            replacement(&db, &by_fluent, "a-3", 5),
        ])
        .atomic()
        .execute()
        .await?;

    let written = contents(&db, &by_sql).await?;
    assert_eq!(
        written["a-1"],
        serde_json::json!({"sku": "a-1", "stock": 9})
    );
    assert_eq!(written.len(), 3);
    assert_eq!(written, contents(&db, &by_fluent).await?);
    assert_eq!(upserted, fluent_upserted);
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_do_nothing_writes_the_rows_the_fluent_insert_of_the_new_documents_writes(
) -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(
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
                "INSERT INTO `{by_sql}` (__name__, sku, stock) \
                 VALUES ('a-1', 'a-1', 99), ('a-3', 'a-3', 5) \
                 ON CONFLICT (__name__) DO NOTHING"
            ),
        ))
        .execute()
        .await?;
    let fluent_inserted = db
        .fluent()
        .pipeline()
        .insert()
        .into(&by_fluent)
        .literals([replacement(&db, &by_fluent, "a-3", 5)])
        .atomic()
        .execute()
        .await?;

    let written = contents(&db, &by_sql).await?;
    assert_eq!(written["a-1"]["stock"], 3);
    assert_eq!(written.len(), 3);
    assert_eq!(written, contents(&db, &by_fluent).await?);
    assert_eq!(inserted, fluent_inserted);
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_do_update_sets_the_fields_the_fluent_update_of_the_stored_documents_sets(
) -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(
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
                "INSERT INTO `{by_sql}` (__name__, sku, stock) \
                 VALUES ('a-1', 'a-1', 9), ('a-3', 'a-3', 5) \
                 ON CONFLICT (__name__) DO UPDATE \
                 SET stock = stock + excluded.stock, dims.w = excluded.stock"
            ),
        ))
        .execute()
        .await?;
    let fluent_updated = db
        .fluent()
        .pipeline()
        .update(|e| {
            [
                e.field("stock").add(9).alias("stock"),
                e.constant(9).alias("dims.w"),
            ]
        })
        .in_col(&by_fluent)
        .filter(|q| q.field("__name__").eq(reference(&db, &by_fluent, "a-1")))
        .atomic()
        .execute()
        .await?;
    let fluent_inserted = db
        .fluent()
        .pipeline()
        .insert()
        .into(&by_fluent)
        .literals([replacement(&db, &by_fluent, "a-3", 5)])
        .atomic()
        .execute()
        .await?;

    let stored = contents(&db, &by_sql).await?;
    // `price` and `extra` are named by neither side of the conflict, so the stored values stay.
    assert_eq!(
        stored["a-1"],
        serde_json::json!({
            "sku": "a-1", "stock": 12, "price": 9.5, "extra": "old", "dims": {"w": 9}
        })
    );
    assert_eq!(stored.len(), 3);
    assert_eq!(stored, contents(&db, &by_fluent).await?);
    assert_eq!(
        written.documents_modified,
        fluent_updated.documents_modified + fluent_inserted.documents_modified
    );
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
    Ok(())
}

#[tokio::test]
async fn an_insert_do_update_where_leaves_the_stored_documents_that_fail_the_condition(
) -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(
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
                "INSERT INTO `{by_sql}` (__name__, sku, stock) \
                 VALUES ('a-1', 'a-1', 7), ('a-2', 'a-2', 7), ('a-4', 'a-4', 7) \
                 ON CONFLICT (__name__) DO UPDATE SET stock = excluded.stock WHERE stock = 0"
            ),
        ))
        .execute()
        .await?;
    let fluent_updated = db
        .fluent()
        .pipeline()
        .update(|e| [e.constant(7).alias("stock")])
        .in_col(&by_fluent)
        .filter(|q| q.field("__name__").eq(reference(&db, &by_fluent, "a-2")))
        .atomic()
        .execute()
        .await?;
    let fluent_inserted = db
        .fluent()
        .pipeline()
        .insert()
        .into(&by_fluent)
        .literals([replacement(&db, &by_fluent, "a-4", 7)])
        .atomic()
        .execute()
        .await?;

    let stored = contents(&db, &by_sql).await?;
    assert_eq!(stored["a-1"]["stock"], 3);
    assert_eq!(stored["a-2"]["stock"], 7);
    assert_eq!(stored, contents(&db, &by_fluent).await?);
    assert_eq!(
        written.documents_modified,
        fluent_updated.documents_modified + fluent_inserted.documents_modified
    );
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
    Ok(())
}

#[tokio::test]
async fn an_update_sets_the_fields_of_the_fluent_update() -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(
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
                "UPDATE `{by_sql}` SET status = 'sold_out', dims.w = 2, dims.h = price \
                 WHERE stock = 0"
            ),
        ))
        .execute()
        .await?;
    let fluent_updated = db
        .fluent()
        .pipeline()
        .update(|e| {
            [
                e.constant("sold_out").alias("status"),
                e.constant(2).alias("dims.w"),
                e.field("price").alias("dims.h"),
            ]
        })
        .in_col(&by_fluent)
        .filter(|q| q.field("stock").eq(0))
        .atomic()
        .execute()
        .await?;

    let written = contents(&db, &by_sql).await?;
    assert_eq!(
        written["a-2"]["dims"],
        serde_json::json!({"w": 2, "h": 4.0})
    );
    assert_eq!(written["a-1"].get("status"), None);
    assert_eq!(written, contents(&db, &by_fluent).await?);
    assert_eq!(updated, fluent_updated);
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
    Ok(())
}

#[tokio::test]
async fn a_delete_removes_the_documents_of_the_fluent_delete() -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) = collections(
        &db,
        "delete",
        &[("a-1", 3, 9.5), ("a-2", 0, 4.0), ("a-3", 0, 1.25)],
    )
    .await?;

    let deleted = db
        .fluent()
        .sql(guarded(
            &db,
            format!("DELETE FROM `{by_sql}` WHERE stock = 0"),
        ))
        .execute()
        .await?;
    let fluent_deleted = db
        .fluent()
        .pipeline()
        .delete()
        .from(&by_fluent)
        .filter(|q| q.field("stock").eq(0))
        .atomic()
        .execute()
        .await?;

    let remaining = contents(&db, &by_sql).await?;
    assert_eq!(remaining.keys().collect::<Vec<_>>(), ["a-1"]);
    assert_eq!(remaining, contents(&db, &by_fluent).await?);
    assert_eq!(deleted, fluent_deleted);
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
    Ok(())
}

#[tokio::test]
async fn a_write_in_a_transaction_changes_what_the_fluent_write_in_one_changes() -> TestResult {
    let db = setup_enterprise().await?;
    let (by_sql, by_fluent) =
        collections(&db, "transaction", &[("a-1", 3, 9.5), ("a-2", 0, 4.0)]).await?;
    let sql = guarded(
        &db,
        format!("UPDATE `{by_sql}` SET stock = stock + 10 WHERE stock >= 0"),
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
    let fluent_updated = db
        .run_transaction(|db, transaction| {
            let by_fluent = by_fluent.clone();
            async move {
                let updated = db
                    .fluent()
                    .pipeline()
                    .update(|e| [e.field("stock").add(10).alias("stock")])
                    .in_col(&by_fluent)
                    .filter(|q| q.field("stock").greater_than_or_equal(0))
                    .execute_in_transaction(transaction)
                    .await?;
                Ok::<_, BackoffError<FirestoreError>>(updated)
            }
            .boxed()
        })
        .await?;

    let written = contents(&db, &by_sql).await?;
    assert_eq!(written["a-1"]["stock"], 13);
    assert_eq!(written, contents(&db, &by_fluent).await?);
    assert_eq!(updated, fluent_updated);
    empty(&db, &by_sql).await?;
    empty(&db, &by_fluent).await?;
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
