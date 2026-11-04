use crate::common::setup_enterprise;
use firestore::errors::FirestoreError;
use firestore::*;
use serde::{Deserialize, Serialize};

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BOOKS: &str = "pipeline-books";
const RESTAURANTS: &str = "pipeline-restaurants";

#[derive(Debug, PartialEq, Deserialize)]
struct Totals {
    total: i64,
    in_stock: i64,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Accumulated {
    rated: i64,
    genres: i64,
    stock: i64,
    shortest: i64,
    longest: i64,
    first_title: String,
    last_title: String,
    titles: Vec<String>,
    genre_set: Vec<Option<String>>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct GenreStats {
    genre: Option<String>,
    books: i64,
    max_pages: i64,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Genre {
    genre: Option<String>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Nearest {
    title: String,
    distance: f64,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Name {
    name: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Tag {
    tag: String,
    tag_index: i64,
}

#[derive(Debug, PartialEq, Deserialize)]
struct InPlaceTag {
    tags: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Awards {
    hugo: bool,
    nebula: bool,
}

#[derive(Debug, PartialEq, Serialize)]
struct Overwrite {
    title: &'static str,
    extra: i64,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Merged {
    title: String,
    extra: i64,
    author: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Threshold {
    a: i64,
    over: bool,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Review {
    rating: i64,
    reviewer: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct WithReviews {
    name: String,
    reviews: Vec<Review>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct WithAverage {
    name: String,
    avg_rating: Option<f64>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct WithRatings {
    name: String,
    reviews: Vec<i64>,
    review_count: i64,
}

#[tokio::test]
async fn aggregate_outputs_one_document_of_accumulators() -> TestResult {
    let db = setup_enterprise().await?;

    let totals: Vec<Totals> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .aggregate(|e| {
            [
                e.count_all().alias("total"),
                e.field("stock")
                    .greater_than(0)
                    .count_if()
                    .alias("in_stock"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(
        totals,
        [Totals {
            total: 11,
            in_stock: 7
        }]
    );
    Ok(())
}

#[tokio::test]
async fn every_accumulator_is_accepted_under_its_wire_name() -> TestResult {
    let db = setup_enterprise().await?;

    let accumulated: Vec<Accumulated> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .sort(|e| [e.field("title").asc()])
        .aggregate(|e| {
            [
                e.field("rating").count().alias("rated"),
                e.field("genre").count_distinct().alias("genres"),
                e.field("stock").sum().alias("stock"),
                e.field("pages").minimum().alias("shortest"),
                e.field("pages").maximum().alias("longest"),
                e.field("title").first().alias("first_title"),
                e.field("title").last().alias("last_title"),
                e.field("title").array_agg().alias("titles"),
                e.field("genre").array_agg_distinct().alias("genre_set"),
            ]
        })
        .obj()
        .query()
        .await?;

    let [accumulated] = <[Accumulated; 1]>::try_from(accumulated).expect("one document");
    assert_eq!(
        (
            accumulated.rated,
            accumulated.genres,
            accumulated.stock,
            accumulated.shortest,
            accumulated.longest,
            accumulated.first_title.as_str(),
            accumulated.last_title.as_str(),
        ),
        (10, 8, 28, 12, 1178, "1984", "Untitled draft")
    );
    assert_eq!(accumulated.titles.len(), 11);
    assert!(
        accumulated.titles.windows(2).all(|pair| pair[0] <= pair[1]),
        "array_agg must keep the order of the sort before it: {:?}",
        accumulated.titles
    );
    assert_eq!(accumulated.genre_set.len(), 9);
    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_outputs_one_document_per_group() -> TestResult {
    let db = setup_enterprise().await?;

    let stats: Vec<GenreStats> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .aggregate_grouped(
            |e| [e.field(path!(GenreStats::genre))],
            |e| {
                [
                    e.count_all().alias("books"),
                    e.field("pages").maximum().alias("max_pages"),
                ]
            },
        )
        .sort(|e| [e.field(path!(GenreStats::genre)).asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(stats.len(), 9);
    assert_eq!(
        stats[0],
        GenreStats {
            genre: None,
            books: 1,
            max_pages: 12
        }
    );
    assert_eq!(
        stats[1],
        GenreStats {
            genre: Some("Dystopian".to_string()),
            books: 2,
            max_pages: 328
        }
    );
    Ok(())
}

#[tokio::test]
async fn distinct_outputs_each_value_once_with_null_as_a_group() -> TestResult {
    let db = setup_enterprise().await?;

    let mut genres: Vec<Genre> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .distinct(|e| [e.field(path!(Genre::genre))])
        .obj()
        .query()
        .await?;
    genres.sort_by(|left, right| left.genre.cmp(&right.genre));

    let genres: Vec<Option<&str>> = genres.iter().map(|g| g.genre.as_deref()).collect();
    assert_eq!(
        genres,
        [
            None,
            Some("Dystopian"),
            Some("Fantasy"),
            Some("Magical Realism"),
            Some("Modernist"),
            Some("Psychological Thriller"),
            Some("Romance"),
            Some("Science Fiction"),
            Some("Southern Gothic"),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn find_nearest_keeps_the_closest_documents_and_outputs_the_distance() -> TestResult {
    let db = setup_enterprise().await?;

    let nearest: Vec<Nearest> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .find_nearest_with_options(
            FirestoreFindNearestStage::new(
                "embedding".to_string(),
                FirestoreVector::new(vec![0.9, 0.1, 0.2]),
                FirestoreFindNearestDistanceMeasure::Euclidean,
                3,
            )
            .with_distance_result_field("distance".to_string()),
        )
        .select(|e| {
            [
                FirestoreSelection::from(e.field("title")),
                e.field("distance").into(),
            ]
        })
        .obj()
        .query()
        .await?;
    let plain: Vec<FirestoreDocument> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .find_nearest(
            "embedding",
            FirestoreVector::new(vec![0.9, 0.1, 0.2]),
            FirestoreFindNearestDistanceMeasure::Euclidean,
            3,
        )
        .query()
        .await?;

    let titles: Vec<&str> = nearest.iter().map(|book| book.title.as_str()).collect();
    assert_eq!(
        titles,
        ["The Hitchhiker's Guide to the Galaxy", "Dune", "1984"]
    );
    assert!(
        nearest
            .windows(2)
            .all(|pair| pair[0].distance <= pair[1].distance),
        "{nearest:?}"
    );
    assert_eq!(plain.len(), 3);
    Ok(())
}

#[tokio::test]
async fn sample_keeps_a_count_or_a_fraction_of_the_documents() -> TestResult {
    let db = setup_enterprise().await?;
    let mut sizes = Vec::new();

    for size in [
        FirestoreSampleSize::Documents(3),
        FirestoreSampleSize::Fraction(0.0),
        FirestoreSampleSize::Fraction(1.0),
    ] {
        let sampled = db
            .fluent()
            .pipeline()
            .collection(BOOKS)
            .sample(size)
            .query()
            .await?;
        sizes.push(sampled.len());
    }

    assert_eq!(sizes, [3, 0, 11]);
    Ok(())
}

#[tokio::test]
async fn union_appends_the_documents_of_another_pipeline() -> TestResult {
    let db = setup_enterprise().await?;

    let names: Vec<Name> = db
        .fluent()
        .pipeline()
        .collection(BOOKS)
        .select(|e| [e.field("title").alias("name")])
        .union(|p| {
            p.collection(RESTAURANTS)
                .select(|e| [e.field(path!(Name::name))])
        })
        .sort(|e| [e.field(path!(Name::name)).asc()])
        .obj()
        .query()
        .await?;

    let names: Vec<&str> = names.iter().map(|name| name.name.as_str()).collect();
    assert_eq!(names.len(), 20);
    assert!(names.contains(&"Dune") && names.contains(&"Pancake Palace"));
    Ok(())
}

#[tokio::test]
async fn unnest_outputs_each_array_element_with_its_index() -> TestResult {
    let db = setup_enterprise().await?;

    let tags: Vec<Tag> = db
        .fluent()
        .pipeline()
        .documents([format!("{BOOKS}/book-01")])
        .unnest_with_index_field(|e| e.field("tags").alias("tag"), "tag_index")
        .select(|e| [e.field(path!(Tag::tag)), e.field(path!(Tag::tag_index))])
        .sort(|e| [e.field(path!(Tag::tag_index)).asc()])
        .obj()
        .query()
        .await?;

    let in_place: Vec<InPlaceTag> = db
        .fluent()
        .pipeline()
        .documents([format!("{BOOKS}/book-01")])
        .unnest(|e| e.field(path!(InPlaceTag::tags)))
        .select(|e| [e.field(path!(InPlaceTag::tags))])
        .obj()
        .query()
        .await?;

    let tags: Vec<(&str, i64)> = tags.iter().map(|t| (t.tag.as_str(), t.tag_index)).collect();
    assert_eq!(tags, [("comedy", 0), ("space", 1), ("adventure", 2)]);
    let mut in_place: Vec<String> = in_place.into_iter().map(|tag| tag.tags).collect();
    in_place.sort();
    assert_eq!(in_place, ["adventure", "comedy", "space"]);
    Ok(())
}

#[tokio::test]
async fn replace_with_replaces_or_merges_the_document() -> TestResult {
    let db = setup_enterprise().await?;
    let book = format!("{BOOKS}/book-10");

    let replaced: Vec<Awards> = db
        .fluent()
        .pipeline()
        .documents([&book])
        .replace_with(|e| e.field("awards"), FirestoreReplaceWithMode::FullReplace)
        .obj()
        .query()
        .await?;
    let mut merged = Vec::new();
    for mode in [
        FirestoreReplaceWithMode::MergeOverwriteExisting,
        FirestoreReplaceWithMode::MergeKeepExisting,
    ] {
        let documents: Vec<Merged> = db
            .fluent()
            .pipeline()
            .documents([&book])
            .replace_with(
                |e| {
                    e.constant(Overwrite {
                        title: "Overwritten",
                        extra: 1,
                    })
                },
                mode,
            )
            .select(|e| {
                [
                    e.field(path!(Merged::title)),
                    e.field(path!(Merged::extra)),
                    e.field(path!(Merged::author)),
                ]
            })
            .obj()
            .query()
            .await?;
        merged.extend(documents);
    }

    assert_eq!(
        replaced,
        [Awards {
            hugo: true,
            nebula: true
        }]
    );
    assert_eq!(
        merged,
        [
            Merged {
                title: "Overwritten".to_string(),
                extra: 1,
                author: "Frank Herbert".to_string()
            },
            Merged {
                title: "Dune".to_string(),
                extra: 1,
                author: "Frank Herbert".to_string()
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn define_makes_a_variable_for_the_stages_after_it() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Threshold> = db
        .fluent()
        .pipeline()
        .literals([[("a", 1)], [("a", 20)]].map(std::collections::HashMap::from))
        .define(|e| [e.constant(10).alias("threshold")])
        .add_fields(|e| {
            [e.field(path!(Threshold::a))
                .greater_than(e.variable("threshold"))
                .alias("over")]
        })
        .sort(|e| [e.field(path!(Threshold::a)).asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(
        rows,
        [
            Threshold { a: 1, over: false },
            Threshold { a: 20, over: true }
        ]
    );
    Ok(())
}

#[tokio::test]
async fn an_array_subquery_joins_the_matching_documents_as_an_array() -> TestResult {
    let db = setup_enterprise().await?;

    let restaurants: Vec<WithReviews> = db
        .fluent()
        .pipeline()
        .collection(RESTAURANTS)
        .filter(|q| q.field("cuisine").eq("breakfast"))
        .define(|e| [e.field("__name__").alias("restaurant")])
        .select(|e| {
            [
                FirestoreSelection::from(e.field(path!(WithReviews::name))),
                e.array_subquery(|p| {
                    p.collection_group("reviews")
                        .filter(|q| q.field("restaurant").eq(q.variable("restaurant")))
                        .sort(|e| [e.field("rating").desc()])
                        .select(|e| [e.field("rating"), e.field("reviewer")])
                })
                .alias("reviews"),
            ]
        })
        .sort(|e| [e.field(path!(WithReviews::name)).asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(
        restaurants,
        [
            WithReviews {
                name: "Pancake Palace".to_string(),
                reviews: vec![Review {
                    rating: 3,
                    reviewer: "carol".to_string()
                }]
            },
            WithReviews {
                name: "Sunrise Waffle House".to_string(),
                reviews: vec![
                    Review {
                        rating: 5,
                        reviewer: "alice".to_string()
                    },
                    Review {
                        rating: 4,
                        reviewer: "bob".to_string()
                    }
                ]
            },
            WithReviews {
                name: "Waffle and Bacon Co".to_string(),
                reviews: vec![]
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_scalar_subquery_joins_one_value_and_null_for_no_match() -> TestResult {
    let db = setup_enterprise().await?;

    let restaurants: Vec<WithAverage> = db
        .fluent()
        .pipeline()
        .collection(RESTAURANTS)
        .filter(|q| q.field("cuisine").eq("breakfast"))
        .define(|e| [e.field("__name__").alias("restaurant")])
        .select(|e| {
            [
                FirestoreSelection::from(e.field(path!(WithAverage::name))),
                e.scalar_subquery(|p| {
                    p.collection_group("reviews")
                        .filter(|q| q.field("restaurant").eq(q.variable("restaurant")))
                        .aggregate(|e| [e.field("rating").average().alias("avg")])
                })
                .alias("avg_rating"),
            ]
        })
        .sort(|e| [e.field(path!(WithAverage::name)).asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(
        restaurants,
        [
            WithAverage {
                name: "Pancake Palace".to_string(),
                avg_rating: Some(3.0)
            },
            WithAverage {
                name: "Sunrise Waffle House".to_string(),
                avg_rating: Some(4.5)
            },
            WithAverage {
                name: "Waffle and Bacon Co".to_string(),
                avg_rating: None
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_subcollection_subquery_joins_the_documents_under_each_document() -> TestResult {
    let db = setup_enterprise().await?;

    let restaurants: Vec<WithRatings> = db
        .fluent()
        .pipeline()
        .collection(RESTAURANTS)
        .filter(|q| q.field("cuisine").eq("pizza"))
        .select(|e| {
            [
                FirestoreSelection::from(e.field(path!(WithRatings::name))),
                e.array_subquery(|p| p.subcollection("reviews").select(|e| [e.field("rating")]))
                    .alias("reviews"),
                e.raw(
                    "array_length",
                    [e.array_subquery(|p| p.subcollection("reviews"))],
                )
                .alias("review_count"),
            ]
        })
        .sort(|e| [e.field(path!(WithRatings::name)).asc()])
        .obj()
        .query()
        .await?;

    assert_eq!(
        restaurants,
        [
            WithRatings {
                name: "Slice of New York".to_string(),
                reviews: vec![5, 4, 2],
                review_count: 3
            },
            WithRatings {
                name: "Windy City Deep Dish".to_string(),
                reviews: vec![],
                review_count: 0
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_scalar_subquery_of_several_results_fails_the_request() -> TestResult {
    let db = setup_enterprise().await?;

    let result = db
        .fluent()
        .pipeline()
        .documents([format!("{RESTAURANTS}/rest-04")])
        .define(|e| [e.field("__name__").alias("restaurant")])
        .select(|e| {
            [
                FirestoreSelection::from(e.field("name")),
                e.scalar_subquery(|p| {
                    p.collection_group("reviews")
                        .filter(|q| q.field("restaurant").eq(q.variable("restaurant")))
                        .select(|e| [e.field("rating")])
                })
                .alias("rating"),
            ]
        })
        .query()
        .await;

    assert!(
        matches!(
            result,
            Err(FirestoreError::DatabaseError(ref err))
                if err.public.code == "InvalidArgument" && !err.retry_possible
        ),
        "{result:?}"
    );
    Ok(())
}
