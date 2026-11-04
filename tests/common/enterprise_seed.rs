//! The documents and indexes the live pipeline tests read, written into the Enterprise edition
//! database `GCP_ENTERPRISE_DATABASE_ID` by [`ensure_seeded`].
//!
//! The tests assert on exact rows, counts and scores of these documents, so every document is
//! upserted by its fixed ID and a drifted one is restored. Documents the seed does not own are
//! never touched.

use super::populate_collection;
use firestore::index_builder::FirestoreIndexesBuilder;
use firestore::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::{sleep, Instant};
use tracing::info;

type SeedResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BOOKS: &str = "pipeline-books";
const RESTAURANTS: &str = "pipeline-restaurants";
const REVIEWS: &str = "reviews";
const BULK: &str = "pipeline-bulk";

/// Enough documents for a response stream of more than one message.
const BULK_ITEMS: usize = 1_500;

/// Building a missing index takes minutes.
const INDEX_BUILD_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const INDEX_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The outcome is kept as text so that a failed seeding fails every test of the binary at once,
/// instead of each of them waiting for the same index again.
static SEEDED: OnceCell<Result<(), String>> = OnceCell::const_new();

/// Seeds the database once per test binary.
///
/// Several binaries may seed concurrently: the upserts commit the same contents, and an index
/// create that loses the race to another binary is reported by the sync as unchanged once the
/// listing shows the winner's index, or else fails with `ALREADY_EXISTS`. [`ensure_indexes_ready`]
/// plans again after either, and waits for the winner's build.
pub async fn ensure_seeded(db: &FirestoreDb) -> SeedResult {
    SEEDED
        .get_or_init(|| async { seed(db).await.map_err(|err| err.to_string()) })
        .await
        .clone()
        .map_err(Into::into)
}

async fn seed(db: &FirestoreDb) -> SeedResult {
    upsert_fixed_documents(db).await?;
    seed_bulk_when_missing(db).await?;
    ensure_indexes_ready(book_indexes(db)).await?;
    ensure_indexes_ready(restaurant_indexes(db)).await?;
    Ok(())
}

#[derive(Serialize)]
struct Awards {
    hugo: bool,
    nebula: bool,
}

/// Fields that are `None` are absent from the document, except `genre`, which is an explicit
/// null: pipeline expressions tell a null from an absent field.
#[derive(Serialize)]
struct Book {
    title: &'static str,
    author: &'static str,
    #[serde(with = "firestore::serialize_as_null")]
    genre: Option<&'static str>,
    published: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    rating: Option<f64>,
    pages: i64,
    stock: i64,
    tags: &'static [&'static str],
    #[serde(skip_serializing_if = "Option::is_none")]
    awards: Option<Awards>,
    #[serde(skip_serializing_if = "Option::is_none")]
    embedding: Option<FirestoreVector>,
}

#[derive(Serialize)]
struct Restaurant {
    name: &'static str,
    cuisine: &'static str,
    description: &'static str,
    menu: &'static str,
    location: FirestoreLatLng,
    rating: f64,
    price_level: i64,
}

/// A review is joined to its restaurant by its parent path, for `subcollection(..)`, and by the
/// `restaurant` reference, for joins over the `reviews` collection group.
#[derive(Serialize)]
struct Review {
    rating: i64,
    reviewer: &'static str,
    restaurant: FirestoreReference,
}

#[derive(Serialize, Deserialize)]
struct BulkItem {
    n: i64,
    bucket: i64,
    label: String,
}

#[derive(Deserialize)]
struct Count {
    count: i64,
}

fn books() -> Vec<(&'static str, Book)> {
    let awards = |hugo, nebula| Some(Awards { hugo, nebula });
    let embedding = |components: [f64; 3]| Some(FirestoreVector::new(components.to_vec()));
    vec![
        (
            "book-01",
            Book {
                title: "The Hitchhiker's Guide to the Galaxy",
                author: "Douglas Adams",
                genre: Some("Science Fiction"),
                published: 1979,
                rating: Some(4.2),
                pages: 224,
                stock: 3,
                tags: &["comedy", "space", "adventure"],
                awards: awards(true, false),
                embedding: embedding([0.9, 0.1, 0.2]),
            },
        ),
        (
            "book-02",
            Book {
                title: "Pride and Prejudice",
                author: "Jane Austen",
                genre: Some("Romance"),
                published: 1813,
                rating: Some(4.5),
                pages: 432,
                stock: 0,
                tags: &["classic", "social commentary", "love"],
                awards: awards(false, false),
                embedding: embedding([0.1, 0.9, 0.1]),
            },
        ),
        (
            "book-03",
            Book {
                title: "One Hundred Years of Solitude",
                author: "Gabriel García Márquez",
                genre: Some("Magical Realism"),
                published: 1967,
                rating: Some(4.3),
                pages: 417,
                stock: 5,
                tags: &["family", "history", "fantasy"],
                awards: awards(false, false),
                embedding: embedding([0.3, 0.6, 0.6]),
            },
        ),
        (
            "book-04",
            Book {
                title: "The Lord of the Rings",
                author: "J.R.R. Tolkien",
                genre: Some("Fantasy"),
                published: 1954,
                rating: Some(4.7),
                pages: 1178,
                stock: 2,
                tags: &["adventure", "magic", "epic"],
                awards: awards(false, false),
                embedding: embedding([0.4, 0.2, 0.9]),
            },
        ),
        (
            "book-05",
            Book {
                title: "The Handmaid's Tale",
                author: "Margaret Atwood",
                genre: Some("Dystopian"),
                published: 1985,
                rating: Some(4.1),
                pages: 311,
                stock: 0,
                tags: &["feminism", "totalitarianism", "resistance"],
                awards: awards(false, false),
                embedding: embedding([0.6, 0.5, 0.1]),
            },
        ),
        (
            "book-06",
            Book {
                title: "Crime and Punishment",
                author: "Fyodor Dostoevsky",
                genre: Some("Psychological Thriller"),
                published: 1866,
                rating: Some(4.3),
                pages: 671,
                stock: 1,
                tags: &["philosophy", "crime", "redemption"],
                awards: awards(false, false),
                embedding: embedding([0.2, 0.7, 0.4]),
            },
        ),
        (
            "book-07",
            Book {
                title: "To Kill a Mockingbird",
                author: "Harper Lee",
                genre: Some("Southern Gothic"),
                published: 1960,
                rating: Some(4.2),
                pages: 281,
                stock: 4,
                tags: &["racism", "injustice", "coming-of-age"],
                awards: awards(false, false),
                embedding: embedding([0.2, 0.8, 0.2]),
            },
        ),
        (
            "book-08",
            Book {
                title: "1984",
                author: "George Orwell",
                genre: Some("Dystopian"),
                published: 1949,
                rating: Some(4.2),
                pages: 328,
                stock: 7,
                tags: &["surveillance", "totalitarianism", "propaganda"],
                awards: awards(false, false),
                embedding: embedding([0.7, 0.4, 0.1]),
            },
        ),
        (
            "book-09",
            Book {
                title: "The Great Gatsby",
                author: "F. Scott Fitzgerald",
                genre: Some("Modernist"),
                published: 1925,
                rating: Some(4.0),
                pages: 180,
                stock: 0,
                tags: &["wealth", "american dream", "love"],
                awards: awards(false, false),
                embedding: embedding([0.1, 0.7, 0.3]),
            },
        ),
        (
            "book-10",
            Book {
                title: "Dune",
                author: "Frank Herbert",
                genre: Some("Science Fiction"),
                published: 1965,
                rating: Some(4.6),
                pages: 412,
                stock: 6,
                tags: &["politics", "desert", "ecology"],
                awards: awards(true, true),
                embedding: embedding([0.95, 0.15, 0.35]),
            },
        ),
        (
            "book-11",
            Book {
                title: "Untitled draft",
                author: "Anonymous",
                genre: None,
                published: 2024,
                rating: None,
                pages: 12,
                stock: 0,
                tags: &[],
                awards: None,
                embedding: None,
            },
        ),
    ]
}

/// Distances are measured from the point (38.989177, -107.065076), the one the Firebase geo
/// documentation uses; 0.001 degrees of latitude is about 111 m.
fn restaurants() -> Vec<(&'static str, Restaurant)> {
    let location = |latitude, longitude| {
        FirestoreLatLng(FirestoreGeoPoint {
            latitude,
            longitude,
        })
    };
    vec![
        (
            "rest-01",
            Restaurant {
                name: "Sunrise Waffle House",
                cuisine: "breakfast",
                description: "Belgian waffles and pancakes with maple syrup, served all day.",
                menu: "belgian waffles, maple syrup, buttermilk pancakes, bacon",
                location: location(38.991877, -107.065076),
                rating: 4.6,
                price_level: 2,
            },
        ),
        (
            "rest-02",
            Restaurant {
                name: "Pancake Palace",
                cuisine: "breakfast",
                description: "Stacks of pancakes and French crepes.",
                menu: "buttermilk pancakes, crepes, scrambled eggs",
                location: location(38.996377, -107.065076),
                rating: 4.2,
                price_level: 1,
            },
        ),
        (
            "rest-03",
            Restaurant {
                name: "Waffle and Bacon Co",
                cuisine: "breakfast",
                description: "Waffles with bacon and strong coffee.",
                menu: "waffles, bacon, coffee",
                location: location(39.007177, -107.065076),
                rating: 3.9,
                price_level: 2,
            },
        ),
        (
            "rest-04",
            Restaurant {
                name: "Slice of New York",
                cuisine: "pizza",
                description: "New York style pizza by the slice.",
                menu: "pizza, garlic knots, soda",
                location: location(38.993677, -107.065076),
                rating: 4.4,
                price_level: 1,
            },
        ),
        (
            "rest-05",
            Restaurant {
                name: "Windy City Deep Dish",
                cuisine: "pizza",
                description: "Chicago deep dish pizza, not New York style.",
                menu: "deep dish pizza, salad",
                location: location(39.034177, -107.065076),
                rating: 4.0,
                price_level: 2,
            },
        ),
        (
            "rest-06",
            Restaurant {
                name: "Café Crêpe",
                cuisine: "french",
                description: "Sweet crêpes, croissants and café au lait.",
                menu: "sweet crêpes, croissants, café au lait",
                location: location(38.997277, -107.065076),
                rating: 4.8,
                price_level: 3,
            },
        ),
        (
            "rest-07",
            Restaurant {
                name: "Coffee Corner",
                cuisine: "cafe",
                description: "Espresso bar with muffins; no waffles here.",
                menu: "espresso, coffee, blueberry muffins",
                location: location(39.439177, -107.065076),
                rating: 4.1,
                price_level: 1,
            },
        ),
        (
            "rest-08",
            Restaurant {
                name: "Gluten-Free Kitchen",
                cuisine: "health",
                description: "Gluten-free waffles and e-mail order specials.",
                menu: "gluten-free waffles, quinoa bowls, c++ cookies",
                location: location(38.990077, -107.065076),
                rating: 4.3,
                price_level: 3,
            },
        ),
        (
            "rest-09",
            Restaurant {
                name: "The \"Best\" Diner",
                cuisine: "american",
                description: "Serves the \"best\" waffles in town: field:menu is not a field here.",
                menu: "waffles, burgers, milkshakes",
                location: location(38.989177, -107.064000),
                rating: 3.5,
                price_level: 1,
            },
        ),
    ]
}

/// Each review with the ID of its restaurant and its own.
fn reviews(db: &FirestoreDb) -> Vec<(&'static str, &'static str, Review)> {
    let review = |restaurant_id: &str, rating, reviewer| Review {
        rating,
        reviewer,
        restaurant: FirestoreReference::new(format!(
            "{}/{RESTAURANTS}/{restaurant_id}",
            db.get_documents_path()
        )),
    };
    vec![
        ("rest-01", "review-1", review("rest-01", 5, "alice")),
        ("rest-01", "review-2", review("rest-01", 4, "bob")),
        ("rest-02", "review-1", review("rest-02", 3, "carol")),
        ("rest-04", "review-1", review("rest-04", 5, "alice")),
        ("rest-04", "review-2", review("rest-04", 4, "dave")),
        ("rest-04", "review-3", review("rest-04", 2, "erin")),
    ]
}

/// Writes the books, restaurants and reviews in one transaction, so the database never holds
/// half of them.
async fn upsert_fixed_documents(db: &FirestoreDb) -> SeedResult {
    let mut transaction = db.begin_transaction().await?;
    for (id, book) in books() {
        transaction.update_object(BOOKS, id, &book, None, None, Vec::new())?;
    }
    for (id, restaurant) in restaurants() {
        transaction.update_object(RESTAURANTS, id, &restaurant, None, None, Vec::new())?;
    }
    for (restaurant_id, id, review) in reviews(db) {
        let parent = db.parent_path(RESTAURANTS, restaurant_id)?;
        transaction.update_object_at(
            parent.as_ref(),
            REVIEWS,
            id,
            &review,
            None,
            None,
            Vec::new(),
        )?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn bulk_count(db: &FirestoreDb) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let counts: Vec<Count> = db
        .fluent()
        .select()
        .from(BULK)
        .aggregate(|a| a.fields([a.field("count").count()]))
        .obj()
        .query()
        .await?;
    Ok(counts.first().map_or(0, |count| count.count))
}

/// Writes the bulk collection only when it holds fewer than [`BULK_ITEMS`] documents, since
/// counting is much cheaper than rewriting them.
async fn seed_bulk_when_missing(db: &FirestoreDb) -> SeedResult {
    if bulk_count(db).await? >= BULK_ITEMS as i64 {
        return Ok(());
    }
    info!(collection = BULK, "Writing the bulk collection.");
    populate_collection(
        db,
        BULK,
        BULK_ITEMS,
        |n| BulkItem {
            n: n as i64,
            bucket: (n % 10) as i64,
            label: format!("item {n}"),
        },
        |item| format!("item-{:04}", item.n),
    )
    .await?;
    let written = bulk_count(db).await?;
    if written < BULK_ITEMS as i64 {
        return Err(
            format!("{BULK} holds {written} of {BULK_ITEMS} documents after seeding").into(),
        );
    }
    Ok(())
}

fn book_indexes(db: &FirestoreDb) -> FirestoreIndexesBuilder<'_, FirestoreDb> {
    db.fluent()
        .indexes()
        .collection_group(BOOKS)
        .composite(|i| {
            i.indexes([
                i.index([i.field("genre").asc(), i.field("rating").desc()]),
                i.index([i.field("embedding").vector(3)]),
            ])
        })
}

fn restaurant_indexes(db: &FirestoreDb) -> FirestoreIndexesBuilder<'_, FirestoreDb> {
    db.fluent()
        .indexes()
        .collection_group(RESTAURANTS)
        .composite(|i| {
            i.indexes([
                i.index([i.field("menu").text(), i.field("description").text()])
                    .search_options(
                        FirestoreSearchIndexOptions::new().with_text_language("en".to_string()),
                    ),
                i.index([i.field("location").geo()]),
            ])
        })
}

/// Returns once every declared index is `READY`. When the indexes already exist this costs one
/// listing; a missing one is created, and one that another process is building is waited for.
async fn ensure_indexes_ready(declared: FirestoreIndexesBuilder<'_, FirestoreDb>) -> SeedResult {
    let deadline = Instant::now() + INDEX_BUILD_TIMEOUT;
    loop {
        let plan = declared.clone().plan().await?;
        if plan.create_indexes.is_empty() && plan.pending.is_empty() {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "indexes not ready after {INDEX_BUILD_TIMEOUT:?}: {} to create, {} building",
                plan.create_indexes.len(),
                plan.pending.len()
            )
            .into());
        }
        if plan.create_indexes.is_empty() {
            sleep(INDEX_POLL_INTERVAL.min(remaining)).await;
        } else {
            match declared.clone().wait_until_ready(remaining).sync().await {
                Ok(_) => {}
                // The index exists, but the listing does not show it yet.
                Err(firestore::errors::FirestoreError::DataConflictError(_)) => {
                    sleep(INDEX_POLL_INTERVAL.min(remaining)).await
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}
