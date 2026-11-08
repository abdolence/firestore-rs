// SELECT rand() AS roll FROM books
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct BooksRow {
    roll: Option<serde_json::Value>,
}

async fn query_books(db: &FirestoreDb) -> FirestoreResult<Vec<BooksRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("books")
        .fields(|e| [e.raw("rand", Vec::<FirestoreExpr>::new()).alias("roll")])
        .obj::<BooksRow>()
        .query()
        .await
}
