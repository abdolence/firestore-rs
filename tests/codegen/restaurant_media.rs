// SELECT name, owner, location, photo, thumbnail FROM restaurants
// WHERE owner = @owner AND location = @spot AND token = @token
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct RestaurantsRow {
    name: String,
    owner: FirestoreReference,
    location: FirestoreLatLng,
    #[serde(with = "serde_bytes")]
    photo: Vec<u8>,
    #[serde(default, with = "serde_bytes")]
    thumbnail: Option<Vec<u8>>,
}

async fn query_restaurants(
    db: &FirestoreDb,
    owner: &FirestoreReference,
    spot: &FirestoreLatLng,
    token: &serde_bytes::Bytes,
) -> FirestoreResult<Vec<RestaurantsRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("restaurants")
        .filter(|q| {
            q
                .for_all([
                    q.field("owner").equal(owner),
                    q.field("location").equal(spot),
                    q.field("token").equal(token),
                ])
        })
        .fields(|e| [
            e.field("name"),
            e.field("owner"),
            e.field("location"),
            e.field("photo"),
            e.field("thumbnail"),
        ])
        .obj::<RestaurantsRow>()
        .query()
        .await
}
