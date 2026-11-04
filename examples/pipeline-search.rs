use firestore::*;
use serde::Deserialize;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// `pipeline-restaurants` is read only here. It needs a text search index on `menu` and
// `description`, and a geo search index on `location`.
const RESTAURANTS: &str = "pipeline-restaurants";

#[derive(Debug, Clone, Deserialize)]
struct Hit {
    name: String,
    score: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct Name {
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = FirestoreDb::with_options(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?)
            .with_database_id(config_env_var("DATABASE_ID")?),
    )
    .await?;

    // Text search: a document must contain "waffles" and must not contain "bacon", most relevant
    // first, with the relevance score output as a field.
    let hits: Vec<Hit> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| {
            s.text(|t| t.for_all([t.term("waffles"), t.exclude(t.term("bacon"))]))
                .sort_by_score()
                .score_field(path!(Hit::score))
                .limit(5)
        })
        .fields(|e| {
            [
                FirestoreSelection::from(e.field(path!(Hit::name))),
                e.field(path!(Hit::score)).into(),
            ]
        })
        .obj()
        .query()
        .await?;
    println!("Waffles without bacon:");
    for hit in &hits {
        println!("  {} (score {:.3})", hit.name, hit.score);
    }

    // Geo search: restaurants within 1,000 metres of a point, nearest first.
    let center = FirestoreLatLng(FirestoreGeoPoint {
        latitude: 38.989177,
        longitude: -107.065076,
    });
    let nearby: Vec<Name> = db
        .fluent()
        .pipeline()
        .select()
        .from(RESTAURANTS)
        .search(|s| s.geo_within("location", center, 1000.0).sort_by_distance())
        .fields(|e| [e.field(path!(Name::name))])
        .obj()
        .query()
        .await?;
    println!("Within 1,000 metres:");
    for restaurant in &nearby {
        println!("  {}", restaurant.name);
    }

    Ok(())
}
