use firestore::errors::{FirestoreError, FirestoreInvalidParametersPublicDetails};
use firestore::*;
use serde::Serialize;
use serde_json::json;

// Each snapshot is a module as well, so that a snippet which stops compiling fails the build.
// prettyplease, not rustfmt, formats them.
#[path = "codegen/books.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod books;

#[path = "codegen/genre_ratings.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod genre_ratings;

#[path = "codegen/review_restaurants.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod review_restaurants;

#[path = "codegen/restaurant_search.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod restaurant_search;

#[path = "codegen/restaurant_distance.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod restaurant_distance;

#[path = "codegen/year_books.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod year_books;

#[path = "codegen/restaurant_media.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod restaurant_media;

#[path = "codegen/review_documents.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod review_documents;

#[path = "codegen/book_roll.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod book_roll;

#[path = "codegen/book_sample.rs"]
#[allow(dead_code)] // compiled to prove the snippet builds, never called
#[rustfmt::skip]
mod book_sample;

const BOOKS: &str = "SELECT title, author, rating, publishedAt, tags, edition FROM books
WHERE genre = @genre AND rating >= @min_rating ORDER BY rating DESC LIMIT 20";

#[derive(Serialize)]
struct SampledBook {
    title: &'static str,
    author: Option<&'static str>,
    rating: serde_json::Value,
    #[serde(rename = "publishedAt")]
    published_at: FirestoreTimestamp,
    tags: Vec<&'static str>,
    edition: serde_json::Value,
}

fn refusal(
    sql: &str,
    parameters: &FirestoreSqlParameters,
) -> FirestoreInvalidParametersPublicDetails {
    match FirestoreSqlStatement::fluent_rust(sql, parameters, &[]) {
        Err(FirestoreError::InvalidParametersError(error)) => error.public,
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn a_select_with_parameters_is_typed_from_its_sample() -> FirestoreResult<()> {
    let published_at = FirestoreTimestamp::now();
    let sample = [
        SampledBook {
            title: "1984",
            author: Some("George Orwell"),
            rating: json!(4.5),
            published_at,
            tags: vec!["classic"],
            edition: json!(2),
        },
        SampledBook {
            title: "We",
            author: None,
            rating: json!(4),
            published_at,
            tags: vec![],
            edition: json!("first"),
        },
    ]
    .iter()
    .map(|book| firestore_document_from_serializable("", book))
    .collect::<FirestoreResult<Vec<_>>>()?;
    let parameters = FirestoreSqlParameters::new()
        .param("genre", "Dystopian")
        .param("min_rating", 4.0);

    let snippet = FirestoreSqlStatement::fluent_rust(BOOKS, &parameters, &sample)?;

    assert_eq!(snippet, include_str!("codegen/books.rs"));
    Ok(())
}

const GENRE_RATINGS: &str = "SELECT genre, COUNT(*) AS books, AVG(rating) AS mean_rating,
MIN(rating) AS lowest, MAX(rating) AS highest, SUM(pages) AS pages, COUNT(DISTINCT author) AS authors
FROM books WHERE stock > 0 GROUP BY genre ORDER BY books DESC";

const REVIEW_RESTAURANTS: &str =
    "SELECT DISTINCT restaurant FROM collection_group('reviews') WHERE rating >= 4";

const RESTAURANT_SEARCH: &str = "SELECT name, cuisine, score() AS relevance FROM restaurants
WHERE document_matches('waffles -bacon') ORDER BY score() DESC LIMIT 10";

const RESTAURANT_DISTANCE: &str = "SELECT name FROM restaurants
WHERE geo_distance(location, geo_point(38.989177, -107.065076)) <= 1000
ORDER BY geo_distance(location, geo_point(38.989177, -107.065076))";

fn typed_sample(rows: &[serde_json::Value]) -> FirestoreResult<Vec<FirestoreDocument>> {
    rows.iter()
        .map(|row| firestore_document_from_serializable("", row))
        .collect()
}

#[test]
fn a_group_by_shows_its_accumulators() -> FirestoreResult<()> {
    let sample = typed_sample(&[json!({
        "genre": "Dystopian",
        "books": 3,
        "mean_rating": 4.5,
        "lowest": 4.0,
        "highest": 5.0,
        "pages": 900,
        "authors": 2,
    })])?;

    let snippet =
        FirestoreSqlStatement::fluent_rust(GENRE_RATINGS, &FirestoreSqlParameters::new(), &sample)?;

    assert_eq!(snippet, include_str!("codegen/genre_ratings.rs"));
    Ok(())
}

#[test]
fn a_distinct_select_of_a_collection_group_reads_every_collection_of_the_name(
) -> FirestoreResult<()> {
    let sample = typed_sample(&[json!({ "restaurant": "restaurants/r1" })])?;

    let snippet = FirestoreSqlStatement::fluent_rust(
        REVIEW_RESTAURANTS,
        &FirestoreSqlParameters::new(),
        &sample,
    )?;

    assert_eq!(snippet, include_str!("codegen/review_restaurants.rs"));
    Ok(())
}

#[test]
fn a_text_search_is_part_of_the_source() -> FirestoreResult<()> {
    let sample = typed_sample(&[
        json!({ "name": "Waffle House", "cuisine": "diner", "relevance": 1.5 }),
        json!({ "name": "Stack", "relevance": 0.5 }),
    ])?;

    let snippet = FirestoreSqlStatement::fluent_rust(
        RESTAURANT_SEARCH,
        &FirestoreSqlParameters::new(),
        &sample,
    )?;

    assert_eq!(snippet, include_str!("codegen/restaurant_search.rs"));
    Ok(())
}

#[test]
fn a_geo_search_is_part_of_the_source() -> FirestoreResult<()> {
    let sample = typed_sample(&[json!({ "name": "Waffle House" })])?;

    let snippet = FirestoreSqlStatement::fluent_rust(
        RESTAURANT_DISTANCE,
        &FirestoreSqlParameters::new(),
        &sample,
    )?;

    assert_eq!(snippet, include_str!("codegen/restaurant_distance.rs"));
    Ok(())
}

const YEAR_BOOKS: &str = "SELECT gen, title FROM `2024-books` WHERE genre = @q AND rating > @e";

#[test]
fn names_that_rust_cannot_use_are_made_valid() -> FirestoreResult<()> {
    let parameters = FirestoreSqlParameters::new()
        .param("q", "Dystopian")
        .param("e", 3);

    let snippet = FirestoreSqlStatement::fluent_rust(YEAR_BOOKS, &parameters, &[])?;

    assert_eq!(snippet, include_str!("codegen/year_books.rs"));
    Ok(())
}

#[test]
fn a_function_without_arguments_is_written_raw_with_a_typed_list() -> FirestoreResult<()> {
    let snippet = FirestoreSqlStatement::fluent_rust(
        "SELECT rand() AS roll FROM books",
        &FirestoreSqlParameters::new(),
        &[],
    )?;

    assert_eq!(snippet, include_str!("codegen/book_roll.rs"));
    Ok(())
}

#[test]
fn a_tablesample_is_written_as_a_sample_stage() -> FirestoreResult<()> {
    let reservoir = FirestoreSqlStatement::fluent_rust(
        "SELECT title FROM books TABLESAMPLE RESERVOIR (5 ROWS) WHERE rating > 4",
        &FirestoreSqlParameters::new(),
        &[],
    )?;
    let bernoulli = FirestoreSqlStatement::fluent_rust(
        "SELECT title FROM books TABLESAMPLE BERNOULLI (25 PERCENT)",
        &FirestoreSqlParameters::new(),
        &[],
    )?;

    assert_eq!(reservoir, include_str!("codegen/book_sample.rs"));
    assert!(bernoulli.contains(".sample(FirestoreSampleSize::Fraction(0.25))"));
    Ok(())
}

const RESTAURANT_MEDIA: &str = "SELECT name, owner, location, photo, thumbnail FROM restaurants
WHERE owner = @owner AND location = @spot AND token = @token";

#[derive(Serialize)]
struct SampledRestaurant {
    name: &'static str,
    owner: FirestoreReference,
    location: FirestoreLatLng,
    photo: serde_bytes::ByteBuf,
    thumbnail: Option<serde_bytes::ByteBuf>,
}

#[test]
fn values_that_json_cannot_carry_keep_their_types() -> FirestoreResult<()> {
    let restaurant = |name, thumbnail| SampledRestaurant {
        name,
        owner: FirestoreReference("projects/p/databases/(default)/documents/owners/o1".into()),
        location: FirestoreLatLng(FirestoreGeoPoint {
            latitude: 38.98,
            longitude: -107.06,
        }),
        photo: serde_bytes::ByteBuf::from(vec![1, 2, 3]),
        thumbnail,
    };
    let sample = [
        restaurant("Waffle House", None),
        restaurant("Stack", Some(serde_bytes::ByteBuf::from(vec![4, 5]))),
    ]
    .iter()
    .map(|restaurant| firestore_document_from_serializable("", restaurant))
    .collect::<FirestoreResult<Vec<_>>>()?;
    let parameters = FirestoreSqlParameters::new()
        .param(
            "owner",
            FirestoreReference("projects/p/databases/(default)/documents/owners/o1".into()),
        )
        .param(
            "spot",
            FirestoreLatLng(FirestoreGeoPoint {
                latitude: 38.98,
                longitude: -107.06,
            }),
        )
        .param("token", serde_bytes::ByteBuf::from(vec![7, 8]));

    let snippet = FirestoreSqlStatement::fluent_rust(RESTAURANT_MEDIA, &parameters, &sample)?;

    assert_eq!(snippet, include_str!("codegen/restaurant_media.rs"));
    Ok(())
}

const REVIEW_DOCUMENTS: &str =
    "SELECT * FROM `restaurants/r1/reviews` WHERE __name__ = 'v1' OR rating IS NULL";

#[test]
fn a_nested_collection_is_read_through_its_parent_document() -> FirestoreResult<()> {
    let snippet =
        FirestoreSqlStatement::fluent_rust(REVIEW_DOCUMENTS, &FirestoreSqlParameters::new(), &[])?;

    assert_eq!(snippet, include_str!("codegen/review_documents.rs"));
    Ok(())
}

#[test]
fn a_union_has_no_code_view() {
    let sql = "SELECT title AS name FROM books UNION ALL SELECT name FROM restaurants";

    assert_eq!(
        refusal(sql, &FirestoreSqlParameters::new()).field,
        "codegen"
    );
}

#[test]
fn a_statement_that_is_not_a_select_is_refused() {
    let statements = [
        (
            "INSERT INTO books (title) VALUES (@title)",
            FirestoreSqlParameters::new().param("title", "We"),
        ),
        (
            "DELETE FROM books WHERE genre = @genre",
            FirestoreSqlParameters::new().param("genre", "Dystopian"),
        ),
        (
            "EXPLAIN SELECT title FROM books",
            FirestoreSqlParameters::new(),
        ),
        ("SHOW COLLECTIONS", FirestoreSqlParameters::new()),
    ];

    for (sql, parameters) in statements {
        assert_eq!(refusal(sql, &parameters).field, "statement", "{sql}");
    }
}

#[test]
fn a_parameter_whose_value_the_translation_reads_is_refused() {
    let refusals = [
        (
            "SELECT title FROM books LIMIT @n",
            FirestoreSqlParameters::new().param("n", 10),
            "@n",
        ),
        (
            "SELECT title FROM books LIMIT ?",
            FirestoreSqlParameters::new().params([10]),
            "?",
        ),
        (
            "SELECT title FROM books TABLESAMPLE RESERVOIR (@n ROWS)",
            FirestoreSqlParameters::new().param("n", 5),
            "@n",
        ),
        (
            "SELECT title FROM books WHERE __name__ = @id",
            FirestoreSqlParameters::new().param("id", "b1"),
            "@id",
        ),
        (
            "SELECT name FROM restaurants WHERE document_matches(@q)",
            FirestoreSqlParameters::new().param("q", "waffles"),
            "@q",
        ),
    ];

    for (sql, parameters, parameter) in refusals {
        assert_eq!(refusal(sql, &parameters).field, parameter, "{sql}");
    }
}
