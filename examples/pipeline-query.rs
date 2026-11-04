use firestore::*;
use serde::Deserialize;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// Pipelines need an Enterprise edition database. `pipeline-books` is read only here.
const BOOKS: &str = "pipeline-books";

#[derive(Debug, Clone, Deserialize)]
struct Book {
    title: String,
    author: String,
    rating: Option<f64>,
    stock: i64,
}

// A `fields` stage returns documents without a name, so the ID is optional.
#[derive(Debug, Clone, Deserialize)]
struct BookSummary {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    title: String,
    author_name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct GenreStats {
    genre: Option<String>,
    books: i64,
    average_rating: Option<f64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = FirestoreDb::with_options(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?)
            .with_database_id(config_env_var("DATABASE_ID")?),
    )
    .await?;

    // Filter, sort, page.
    let top_rated: Vec<Book> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .filter(|q| {
            q.for_all([
                q.field(path!(Book::rating)).greater_than_or_equal(4.2),
                q.field(path!(Book::stock)).greater_than(0),
            ])
        })
        .sort(|e| {
            [
                e.field(path!(Book::rating)).desc(),
                e.field(path!(Book::title)).asc(),
            ]
        })
        .limit(5)
        .obj()
        .query()
        .await?;
    println!("In stock and rated 4.2 or more:");
    for book in &top_rated {
        println!("  {} by {}, {:?}", book.title, book.author, book.rating);
    }

    // Projection with a renamed field.
    let summaries: Vec<BookSummary> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .sort(|e| [e.field(path!(BookSummary::title)).asc()])
        .limit(3)
        .fields(|e| {
            [
                FirestoreSelection::from(e.field(path!(BookSummary::title))),
                e.field("author").alias(path!(BookSummary::author_name)),
            ]
        })
        .obj()
        .query()
        .await?;
    println!("Summaries:");
    for summary in &summaries {
        println!(
            "  {} by {} (document ID {:?})",
            summary.title, summary.author_name, summary.id
        );
    }

    // Aggregation per group. A null genre is a group of its own.
    let stats: Vec<GenreStats> = db
        .fluent()
        .pipeline()
        .select()
        .from(BOOKS)
        .aggregate_grouped(
            |e| [e.field(path!(GenreStats::genre))],
            |e| {
                [
                    e.count_all().alias(path!(GenreStats::books)),
                    e.field("rating")
                        .average()
                        .alias(path!(GenreStats::average_rating)),
                ]
            },
        )
        .sort(|e| {
            [
                e.field(path!(GenreStats::books)).desc(),
                e.field(path!(GenreStats::genre)).asc(),
            ]
        })
        .limit(3)
        .obj()
        .query()
        .await?;
    println!("Largest genres:");
    for genre in &stats {
        println!(
            "  {:?}: {} books, average rating {:?}",
            genre.genre, genre.books, genre.average_rating
        );
    }

    Ok(())
}
