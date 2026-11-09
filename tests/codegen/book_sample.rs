// SELECT title FROM books TABLESAMPLE RESERVOIR (5 ROWS) WHERE rating > 4
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct BooksRow {
    title: Option<serde_json::Value>,
}

async fn query_books(db: &FirestoreDb) -> FirestoreResult<Vec<BooksRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("books")
        .sample(FirestoreSampleSize::Documents(5))
        .filter(|q| q.field("rating").greater_than(4))
        .fields(|e| [e.field("title")])
        .obj::<BooksRow>()
        .query()
        .await
}
