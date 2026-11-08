// SELECT * FROM `restaurants/r1/reviews` WHERE __name__ = 'v1' OR rating IS NULL
use firestore::*;

async fn query_reviews(db: &FirestoreDb) -> FirestoreResult<Vec<FirestoreDocument>> {
    db.fluent()
        .pipeline()
        .select()
        .from("reviews")
        .parent(db.parent_path("restaurants", "r1")?)
        .filter(|q| {
            q
                .for_any([
                    q
                        .field("__name__")
                        .equal(
                            FirestoreReference::new(
                                format!(
                                    "{}/restaurants/r1/reviews/v1", db.get_documents_path()
                                ),
                            ),
                        ),
                    q.field("rating").is_null(),
                ])
        })
        .query()
        .await
}
