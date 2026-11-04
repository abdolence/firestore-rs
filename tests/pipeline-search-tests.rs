use crate::common::setup_enterprise;
use firestore::errors::FirestoreError;
use firestore::pipeline_search_builder::{FirestoreTextQueryBuilder, FirestoreTextQueryInput};
use firestore::*;
use serde::Deserialize;

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const RESTAURANTS: &str = "pipeline-restaurants";

#[derive(Debug, PartialEq, Deserialize)]
struct Name {
    name: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Scored {
    name: String,
    score: f64,
}

fn sorted(mut names: Vec<Name>) -> Vec<String> {
    names.sort_by(|a, b| a.name.cmp(&b.name));
    names.into_iter().map(|n| n.name).collect()
}

async fn matching(
    db: &FirestoreDb,
    query: impl FirestoreTextQueryInput,
) -> FirestoreResult<Vec<String>> {
    let query = query.build_query();
    let names: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| s.text(|_| query))
        .fields(|e| [e.field("name")])
        .obj()
        .query()
        .await?;
    Ok(sorted(names))
}

#[tokio::test]
async fn a_text_search_finds_terms_phrases_and_leaves_out_exclusions() -> TestResult {
    let db = setup_enterprise().await?;
    let t = FirestoreTextQueryBuilder;

    assert_eq!(
        matching(&db, t.term("waffles")).await?,
        [
            "Coffee Corner",
            "Gluten-Free Kitchen",
            "Sunrise Waffle House",
            "The \"Best\" Diner",
            "Waffle and Bacon Co"
        ]
    );
    assert_eq!(
        matching(&db, t.phrase("maple syrup")).await?,
        ["Sunrise Waffle House"]
    );
    assert_eq!(
        matching(
            &db,
            t.for_all([t.term("waffles"), t.exclude(t.term("bacon"))])
        )
        .await?,
        ["Coffee Corner", "Gluten-Free Kitchen", "The \"Best\" Diner"]
    );
    Ok(())
}

#[tokio::test]
async fn a_term_with_syntax_in_it_is_matched_as_one_quoted_word() -> TestResult {
    let db = setup_enterprise().await?;
    let t = FirestoreTextQueryBuilder;

    let bacon = matching(&db, t.term("bacon")).await?;
    assert_eq!(bacon.len(), 2);
    assert_eq!(matching(&db, t.term("-bacon")).await?, bacon);
    assert!(matching(&db, t.term("menu:waffles")).await?.is_empty());
    assert_eq!(
        matching(&db, t.phrase("\u{201c}maple syrup\u{201d}")).await?,
        ["Sunrise Waffle House"]
    );
    Ok(())
}

#[tokio::test]
async fn a_text_search_sorted_by_score_outputs_the_score() -> TestResult {
    let db = setup_enterprise().await?;

    let scored: Vec<Scored> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| {
            s.text(|t| t.term("waffles"))
                .sort_by_score()
                .score_field("score")
        })
        .fields(|e| [e.field("name"), e.field("score")])
        .obj()
        .query()
        .await?;

    assert_eq!(scored.len(), 5);
    assert!(scored.iter().all(|hit| hit.score > 0.0), "{scored:?}");
    assert!(
        scored.windows(2).all(|pair| pair[0].score >= pair[1].score),
        "{scored:?}"
    );
    Ok(())
}

#[tokio::test]
async fn limit_and_offset_page_a_search() -> TestResult {
    let db = setup_enterprise().await?;

    let page: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| {
            s.text(|t| t.term("waffles"))
                .sort_by_score()
                .limit(2)
                .offset(1)
        })
        .fields(|e| [e.field("name")])
        .obj()
        .query()
        .await?;

    assert_eq!(page.len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_geo_search_keeps_the_documents_within_the_distance_nearest_first() -> TestResult {
    let db = setup_enterprise().await?;
    let center = FirestoreLatLng(FirestoreGeoPoint {
        latitude: 38.989177,
        longitude: -107.065076,
    });

    let names: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| s.geo_within("location", center, 5000.0).sort_by_distance())
        .fields(|e| [e.field("name")])
        .obj()
        .query()
        .await?;

    assert_eq!(
        names.into_iter().map(|n| n.name).collect::<Vec<_>>(),
        [
            "The \"Best\" Diner",
            "Gluten-Free Kitchen",
            "Sunrise Waffle House",
            "Slice of New York",
            "Pancake Palace",
            "Caf\u{e9} Cr\u{ea}pe",
            "Waffle and Bacon Co"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_filter_after_a_search_narrows_the_matches() -> TestResult {
    let db = setup_enterprise().await?;

    let names: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| s.text(|t| t.term("waffles")))
        .filter(|q| q.field("cuisine").eq("breakfast"))
        .fields(|e| [e.field("name")])
        .obj()
        .query()
        .await?;

    assert_eq!(
        sorted(names),
        ["Sunrise Waffle House", "Waffle and Bacon Co"]
    );
    Ok(())
}

#[tokio::test]
async fn a_collection_group_search_without_a_collection_group_index_fails_without_a_retry(
) -> TestResult {
    let db = setup_enterprise().await?;

    let result = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .all_descendants()
        .search(|s| s.text(|t| t.term("waffles")))
        .query()
        .await;

    assert!(
        matches!(
            result,
            Err(FirestoreError::DatabaseError(ref err))
                if err.public.code == "FailedPrecondition" && !err.retry_possible
        ),
        "{result:?}"
    );
    Ok(())
}
