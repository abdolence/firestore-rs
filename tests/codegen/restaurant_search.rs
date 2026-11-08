// SELECT name, cuisine, score() AS relevance FROM restaurants
// WHERE document_matches('waffles -bacon') ORDER BY score() DESC LIMIT 10
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct RestaurantsRow {
    name: String,
    cuisine: Option<String>,
    relevance: f64,
}

async fn query_restaurants(db: &FirestoreDb) -> FirestoreResult<Vec<RestaurantsRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("restaurants")
        .search(|s| {
            s.text(|_| "waffles -bacon").sort_by_score().score_field("relevance")
        })
        .fields(|e| [
            FirestoreSelection::from(e.field("name")),
            FirestoreSelection::from(e.field("cuisine")),
            e.field("relevance").alias("relevance"),
        ])
        .limit(10)
        .obj::<RestaurantsRow>()
        .query()
        .await
}
