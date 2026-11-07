// SELECT title, author, rating, publishedAt, tags, edition FROM books
// WHERE genre = @genre AND rating >= @min_rating ORDER BY rating DESC LIMIT 20
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct BooksRow {
    title: String,
    author: Option<String>,
    rating: f64,
    #[serde(rename = "publishedAt")]
    published_at: FirestoreTimestamp,
    tags: serde_json::Value,
    /// Observed as integer and string in the sample.
    edition: serde_json::Value,
}

async fn query_books(
    db: &FirestoreDb,
    genre: &str,
    min_rating: f64,
) -> FirestoreResult<Vec<BooksRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("books")
        .filter(|q| {
            q
                .for_all([
                    q.field("genre").equal(genre),
                    q.field("rating").greater_than_or_equal(min_rating),
                ])
        })
        .sort(|e| [e.field("rating").desc()])
        .fields(|e| [
            e.field("title"),
            e.field("author"),
            e.field("rating"),
            e.field("publishedAt"),
            e.field("tags"),
            e.field("edition"),
        ])
        .limit(20)
        .obj::<BooksRow>()
        .query()
        .await
}
