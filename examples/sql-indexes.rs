use firestore::*;
use std::time::Duration;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// A dedicated group, so this example never touches the indexes of a collection real data lives in.
const ORDERS_INDEX_GROUP: &str = "firestore-rs-example-sql-orders";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    let FirestoreSqlOutcome::Indexes(listing) = db.fluent().sql("SHOW INDEXES").run().await? else {
        unreachable!("SHOW INDEXES returns a listing");
    };
    println!(
        "The database holds {} composite indexes",
        listing.indexes.len()
    );

    // A Standard edition database needs two fields in an index.
    let FirestoreSqlOutcome::IndexCreated(created) = db
        .fluent()
        .sql(format!(
            "CREATE INDEX IF NOT EXISTS ON `{ORDERS_INDEX_GROUP}` (customer_id, placed_at DESC)"
        ))
        .wait_until_ready(Duration::from_secs(900))
        .run()
        .await?
    else {
        unreachable!("CREATE INDEX returns the index");
    };
    println!("Created {created}");

    let FirestoreSqlOutcome::IndexDropped(dropped) = db
        .fluent()
        .sql(format!("DROP INDEX IF EXISTS `{}`", created.id))
        .run()
        .await?
    else {
        unreachable!("DROP INDEX returns the index it deleted");
    };
    println!("Dropped {dropped:?}");

    Ok(())
}
