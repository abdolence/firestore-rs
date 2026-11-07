// SELECT name FROM restaurants
// WHERE geo_distance(location, geo_point(38.989177, -107.065076)) <= 1000
// ORDER BY geo_distance(location, geo_point(38.989177, -107.065076))
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct RestaurantsRow {
    name: String,
}

async fn query_restaurants(db: &FirestoreDb) -> FirestoreResult<Vec<RestaurantsRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("restaurants")
        .search(|s| {
            s
                .geo_within(
                    "location",
                    FirestoreLatLng(FirestoreGeoPoint {
                        latitude: 38.989177,
                        longitude: -107.065076,
                    }),
                    1000.0,
                )
                .sort_by_distance()
        })
        .fields(|e| [e.field("name")])
        .obj::<RestaurantsRow>()
        .query()
        .await
}
