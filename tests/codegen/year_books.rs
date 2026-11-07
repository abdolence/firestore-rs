// SELECT gen, title FROM `2024-books` WHERE genre = @q AND rating > @e
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct Collection2024BooksRow {
    r#gen: Option<serde_json::Value>,
    title: Option<serde_json::Value>,
}

async fn query_2024_books(
    db: &FirestoreDb,
    e_2: i64,
    q_2: &str,
) -> FirestoreResult<Vec<Collection2024BooksRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("2024-books")
        .filter(|q| {
            q.for_all([q.field("genre").equal(q_2), q.field("rating").greater_than(e_2)])
        })
        .fields(|e| [e.field("gen"), e.field("title")])
        .obj::<Collection2024BooksRow>()
        .query()
        .await
}
