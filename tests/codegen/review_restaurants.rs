// SELECT DISTINCT restaurant FROM collection_group('reviews') WHERE rating >= 4
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct ReviewsRow {
    restaurant: String,
}

async fn query_reviews(db: &FirestoreDb) -> FirestoreResult<Vec<ReviewsRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("reviews")
        .all_descendants()
        .filter(|q| q.field("rating").greater_than_or_equal(4))
        .distinct(|e| [e.field("restaurant")])
        .fields(|e| [e.field("restaurant")])
        .obj::<ReviewsRow>()
        .query()
        .await
}
