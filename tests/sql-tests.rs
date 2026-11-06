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
