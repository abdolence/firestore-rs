use firestore::*;
use serde::Deserialize;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// `pipeline-restaurants` is read only here. Each restaurant has a `reviews` subcollection, and
// each review also names its restaurant in a `restaurant` reference field.
const RESTAURANTS: &str = "pipeline-restaurants";

#[derive(Debug, Clone, Deserialize)]
struct Review {
    rating: i64,
    reviewer: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RestaurantWithReviews {
    name: String,
    reviews: Vec<Review>,
}

#[derive(Debug, Clone, Deserialize)]
struct RestaurantWithAverage {
    name: String,
    average_rating: Option<f64>,
}

// A subquery that selects one field returns the bare values.
#[derive(Debug, Clone, Deserialize)]
struct RestaurantWithRatings {
    name: String,
    ratings: Vec<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = FirestoreDb::with_options(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?)
            .with_database_id(config_env_var("DATABASE_ID")?),
    )
    .await?;

    // Joins are in Preview. The variable carries each restaurant's reference into the subquery.
    let with_reviews: Vec<RestaurantWithReviews> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .filter(|q| q.field("cuisine").eq("breakfast"))
        .define_variables(|e| [e.field("__name__").alias("restaurant")])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field(path!(RestaurantWithReviews::name))),
                e.array_subquery(|p| {
                    p.from("reviews")
                        .all_descendants()
                        .filter(|q| q.field("restaurant").eq(q.variable("restaurant")))
                        .sort(|e| [e.field(path!(Review::rating)).desc()])
                        .fields(|e| {
                            [
                                e.field(path!(Review::rating)),
                                e.field(path!(Review::reviewer)),
                            ]
                        })
                })
                .alias(path!(RestaurantWithReviews::reviews)),
            ]
        })
        .sort(|e| [e.field(path!(RestaurantWithReviews::name)).asc()])
        .obj()
        .query()
        .await?;
    println!("Breakfast restaurants with their reviews:");
    for restaurant in &with_reviews {
        println!("  {}: {:?}", restaurant.name, restaurant.reviews);
    }

    // A scalar subquery returns one value per restaurant, null when nothing matches.
    let with_average: Vec<RestaurantWithAverage> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .filter(|q| q.field("cuisine").eq("breakfast"))
        .define_variables(|e| [e.field("__name__").alias("restaurant")])
        .fields(|e| {
            [
                FirestoreSelection::from(e.field(path!(RestaurantWithAverage::name))),
                e.scalar_subquery(|p| {
                    p.from("reviews")
                        .all_descendants()
                        .filter(|q| q.field("restaurant").eq(q.variable("restaurant")))
                        .aggregate(|e| [e.field("rating").average().alias("average")])
                })
                .alias(path!(RestaurantWithAverage::average_rating)),
            ]
        })
        .sort(|e| [e.field(path!(RestaurantWithAverage::name)).asc()])
        .obj()
        .query()
        .await?;
    println!("Average ratings:");
    for restaurant in &with_average {
        println!("  {}: {:?}", restaurant.name, restaurant.average_rating);
    }

    // `subcollection` reads the reviews under each restaurant, with no variable needed.
    let with_ratings: Vec<RestaurantWithRatings> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .filter(|q| q.field("cuisine").eq("pizza"))
        .fields(|e| {
            [
                FirestoreSelection::from(e.field(path!(RestaurantWithRatings::name))),
                e.array_subquery(|p| {
                    p.subcollection("reviews")
                        .fields(|e| [e.field(path!(Review::rating))])
                })
                .alias(path!(RestaurantWithRatings::ratings)),
            ]
        })
        .sort(|e| [e.field(path!(RestaurantWithRatings::name)).asc()])
        .obj()
        .query()
        .await?;
    println!("Pizza restaurants with their ratings:");
    for restaurant in &with_ratings {
        println!("  {}: {:?}", restaurant.name, restaurant.ratings);
    }

    Ok(())
}
