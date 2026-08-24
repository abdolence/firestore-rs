use firestore::*;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
}

const FIRST_COLLECTION: &str = "test-caching-dynamic-first";
const SECOND_COLLECTION: &str = "test-caching-dynamic-second";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    for collection in [FIRST_COLLECTION, SECOND_COLLECTION] {
        populate(&db, collection).await?;
    }

    // Build the cache knowing about one collection only.
    let cache = FirestoreCache::memory(&db)
        .name("example-dynamic-cache")
        .preloaded_collection(FIRST_COLLECTION)
        .build()
        .await?;

    // Derived before the second collection exists in the cache. It shares the cache's backend
    // rather than a copy, so it will see the addition without being recreated.
    let cached_db = db.read_cached_only(&cache);

    println!("Cached collections: {:?}", cache.cached_collections());

    // Not cached yet, so a cache-only listing refuses rather than returning an empty result that
    // would look like a complete one.
    match list(&cached_db, SECOND_COLLECTION).await {
        Ok(docs) => println!("Unexpectedly listed {} documents", docs.len()),
        Err(err) => println!("Not cached yet, as expected: {err}"),
    }

    // Add it at runtime: it is downloaded, published once complete, and the listener extended.
    cache
        .add_collection(FirestoreCacheCollection::new(SECOND_COLLECTION).preload_all())
        .await?;

    println!("Cached collections: {:?}", cache.cached_collections());
    println!(
        "Listing {SECOND_COLLECTION} from cache: {} documents",
        list(&cached_db, SECOND_COLLECTION).await?.len()
    );

    // Stop caching it again: the documents are dropped and the listener stops watching it.
    cache.remove_collection(SECOND_COLLECTION).await?;

    println!("Cached collections: {:?}", cache.cached_collections());
    match list(&cached_db, SECOND_COLLECTION).await {
        Ok(docs) => println!("Unexpectedly listed {} documents", docs.len()),
        Err(err) => println!("No longer cached, as expected: {err}"),
    }

    // The other collection is untouched throughout.
    println!(
        "Listing {FIRST_COLLECTION} from cache: {} documents",
        list(&cached_db, FIRST_COLLECTION).await?.len()
    );

    cache.shutdown().await?;
    Ok(())
}

async fn list(db: &FirestoreDb, collection: &str) -> FirestoreResult<Vec<MyTestStructure>> {
    db.fluent()
        .list()
        .from(collection)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?
        .try_collect()
        .await
}

async fn populate(
    db: &FirestoreDb,
    collection: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if db
        .fluent()
        .select()
        .by_id_in(collection)
        .one("test-0")
        .await?
        .is_some()
    {
        return Ok(());
    }

    println!("Populating {collection}");
    let batch_writer = db.create_simple_batch_writer().await?;
    let mut current_batch = batch_writer.new_batch();

    for i in 0..5 {
        let my_struct = MyTestStructure {
            some_id: format!("test-{i}"),
            some_string: format!("Test value {i}"),
        };

        db.fluent()
            .update()
            .in_col(collection)
            .document_id(&my_struct.some_id)
            .object(&my_struct)
            .add_to_batch(&mut current_batch)?;
    }
    current_batch.write().await?;

    Ok(())
}
