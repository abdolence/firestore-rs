use crate::common::setup_enterprise;
use firestore::*;
use serde::Deserialize;

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

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
