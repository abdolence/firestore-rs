use crate::common::{setup, setup_enterprise};
use firestore::errors::FirestoreError;
use firestore::*;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BOOKS: &str = "pipeline-books";
const RESTAURANTS: &str = "pipeline-restaurants";

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Book {
    title: String,
    author: String,
    genre: Option<String>,
    rating: Option<f64>,
    stock: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct SelectedBook {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    title: String,
    author_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LiteralRow {
    number: i64,
    letter: String,
}

fn titles(books: &[Book]) -> Vec<&str> {
    books.iter().map(|book| book.title.as_str()).collect()
}

#[tokio::test]
async fn a_collection_source_reads_the_whole_collection() -> TestResult {
    let db = setup_enterprise().await?;

    let books: Vec<Book> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .obj()
        .query()
        .await?;
    let mut reviews: Vec<String> = db
        .fluent()
        .pipeline()
        .collection("reviews")
        .parent(db.parent_path(RESTAURANTS, "rest-01")?)
        .query()
        .await?
        .into_iter()
        .map(|review| review.name)
        .collect();
    reviews.sort();

    assert_eq!(books.len(), 11);
    let rest_01_reviews = format!("{}/{RESTAURANTS}/rest-01/reviews", db.get_documents_path());
    assert_eq!(
        reviews,
        [
            format!("{rest_01_reviews}/review-1"),
            format!("{rest_01_reviews}/review-2"),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_collection_group_source_reads_every_collection_with_the_id() -> TestResult {
    let db = setup_enterprise().await?;

    let reviews = db
        .fluent()
        .pipeline()
        .collection_group("reviews")
        .query()
        .await?;

    assert_eq!(reviews.len(), 6);
    Ok(())
}

#[tokio::test]
async fn a_documents_source_leaves_out_a_missing_document() -> TestResult {
    let db = setup_enterprise().await?;

    let books: Vec<Book> = db
        .fluent()
        .pipeline()
        .documents([
            format!("{BOOKS}/book-01"),
            format!("{BOOKS}/book-10"),
            format!("{BOOKS}/no-such-book"),
        ])
        .sort(|e| [e.field(path!(Book::title)).asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(
        titles(&books),
        ["Dune", "The Hitchhiker's Guide to the Galaxy"]
    );
    Ok(())
}

#[tokio::test]
async fn a_literals_source_returns_its_rows() -> TestResult {
    let db = setup_enterprise().await?;
    let rows = [
        LiteralRow {
            number: 2,
            letter: "y".to_string(),
        },
        LiteralRow {
            number: 1,
            letter: "x".to_string(),
        },
    ];

    let returned: Vec<LiteralRow> = db
        .fluent()
        .pipeline()
        .literals(&rows)
        .sort(|e| [e.field(path!(LiteralRow::number)).asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(returned, [rows[1].clone(), rows[0].clone()]);
    Ok(())
}

#[tokio::test]
async fn filter_sort_offset_and_limit_apply_in_order() -> TestResult {
    let db = setup_enterprise().await?;

    let books: Vec<Book> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .filter(|e| e.field(path!(Book::rating)).greater_than_or_equal(4.2))
        .sort(|e| {
            [
                e.field(path!(Book::rating)).desc(),
                e.field(path!(Book::title)).asc(),
            ]
        })
        .offset(1)
        .limit(3)
        .obj()
        .query()
        .await?;

    assert_eq!(
        titles(&books),
        ["Dune", "Pride and Prejudice", "Crime and Punishment"]
    );
    Ok(())
}

#[tokio::test]
async fn select_returns_the_selected_fields_without_a_document_id() -> TestResult {
    let db = setup_enterprise().await?;

    let books: Vec<SelectedBook> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .sort(|e| [e.field(path!(Book::title)).asc()])
        .limit(2)
        .select(|e| {
            [
                FirestoreSelection::from(e.field(path!(Book::title))),
                e.field(path!(Book::author)).alias("author_name"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(
        books,
        [
            SelectedBook {
                id: None,
                title: "1984".to_string(),
                author_name: "George Orwell".to_string(),
            },
            SelectedBook {
                id: None,
                title: "Crime and Punishment".to_string(),
                author_name: "Fyodor Dostoevsky".to_string(),
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn explain_returns_the_plan_after_the_documents_it_reads() -> TestResult {
    let db = setup_enterprise().await?;

    for (options, documents) in [
        (FirestoreExplainOptions::new(), 0),
        (FirestoreExplainOptions::new().with_analyze(true), 2),
    ] {
        let mut items: Vec<_> = db
            .fluent()
            .pipeline()
            .collection(BOOKS)
            .filter(|e| e.field(path!(Book::genre)).eq("Dystopian"))
            .explain_with_options(options.clone())
            .stream_query_with_metadata()
            .await?
            .try_collect()
            .await?;

        let closing = items
            .pop()
            .expect("a document-less item must close the stream");
        assert!(
            items.iter().all(|item| item.document.is_some()),
            "{options:?}"
        );
        assert_eq!(items.len(), documents, "{options:?}");
        assert!(closing.document.is_none(), "{options:?}");
        assert!(
            closing
                .metadata
                .explain_stats
                .is_some_and(|stats| !stats.text.is_empty()),
            "{options:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_standard_edition_database_fails_the_pipeline_without_a_retry() -> TestResult {
    let db = setup().await?;

    let result = db
        .fluent()
        .pipeline()
        .collection("pipeline-standard-probe")
        .query()
        .await;

    assert!(
        matches!(
            result,
            Err(FirestoreError::DatabaseError(ref err))
                if err.public.code == "FailedPrecondition" && !err.retry_possible
        ),
        "{result:?}"
    );
    Ok(())
}
