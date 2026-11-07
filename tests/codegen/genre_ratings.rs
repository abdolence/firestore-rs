// SELECT genre, COUNT(*) AS books, AVG(rating) AS mean_rating,
// MIN(rating) AS lowest, MAX(rating) AS highest, SUM(pages) AS pages, COUNT(DISTINCT author) AS authors
// FROM books WHERE stock > 0 GROUP BY genre ORDER BY books DESC
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct BooksRow {
    genre: String,
    books: i64,
    mean_rating: f64,
    lowest: f64,
    highest: f64,
    pages: i64,
    authors: i64,
}

async fn query_books(db: &FirestoreDb) -> FirestoreResult<Vec<BooksRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("books")
        .filter(|q| q.field("stock").greater_than(0))
        .aggregate_grouped(
            |e| [e.field("genre")],
            |e| [
                e.count_all().alias("books"),
                e.field("rating").average().alias("mean_rating"),
                e.field("rating").minimum().alias("lowest"),
                e.field("rating").maximum().alias("highest"),
                e.field("pages").sum().alias("pages"),
                e.field("author").count_distinct().alias("authors"),
            ],
        )
        .sort(|e| [e.field("books").desc()])
        .fields(|e| [
            FirestoreSelection::from(e.field("genre")),
            e.field("books").alias("books"),
            e.field("mean_rating").alias("mean_rating"),
            e.field("lowest").alias("lowest"),
            e.field("highest").alias("highest"),
            e.field("pages").alias("pages"),
            e.field("authors").alias("authors"),
        ])
        .obj::<BooksRow>()
        .query()
        .await
}
