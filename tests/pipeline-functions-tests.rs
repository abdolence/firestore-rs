use crate::common::setup_enterprise;
use firestore::*;
use futures::TryStreamExt;
use serde::Deserialize;

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BOOKS: &str = "pipeline-books";

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Book {
    title: String,
}

#[tokio::test]
async fn an_empty_and_keeps_every_document_and_an_empty_or_none() -> TestResult {
    let db = setup_enterprise().await?;

    let every_book: Vec<Book> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .filter(|e| e.and([]))
        .obj()
        .query()
        .await?;
    let no_book: Vec<Book> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .filter(|e| e.or([]))
        .obj()
        .query()
        .await?;

    assert_eq!(every_book.len(), 11);
    assert!(no_book.is_empty());
    Ok(())
}
