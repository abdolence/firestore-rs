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

    // ---------------------------------------------------------------------------------------
    // Tracking individual documents, and changing which ones are tracked
    // ---------------------------------------------------------------------------------------

    // Caching a known set of documents - configuration, feature flags, reference data - is worth
    // saying explicitly: the listener then watches exactly these documents, so unrelated changes
    // in the collection are never streamed here.
    let mut tracked = vec!["test-0".to_string(), "test-1".to_string()];
    track_documents(&cache, &tracked).await?;
    report_tracked(&cached_db, &["test-0", "test-1", "test-2"]).await?;

    // Start tracking one more document and stop tracking another. A Firestore documents target
    // carries a fixed list, so changing it means replacing the target: keep the set on your side
    // and re-apply it, rather than trying to edit it in place.
    tracked.push("test-2".to_string());
    tracked.retain(|id| id != "test-0");
    track_documents(&cache, &tracked).await?;
    report_tracked(&cached_db, &["test-0", "test-1", "test-2"]).await?;

    cache.shutdown().await?;
    Ok(())
}

/// Caches exactly `document_ids` of the tracked collection, replacing whatever was tracked before.
async fn track_documents(
    cache: &FirestoreMemoryCache,
    document_ids: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\nTracking documents: {document_ids:?}");

    // Removing first because a collection can only be cached once, and the watched set is part of
    // its listener target rather than something that can be edited afterwards.
    cache.remove_collection(SECOND_COLLECTION).await?;

    cache
        .add_collection(
            FirestoreCacheCollection::new(SECOND_COLLECTION)
                .documents(document_ids)
                .preload_all(),
        )
        .await?;

    Ok(())
}

/// Prints which of `document_ids` the cache holds, without falling back to Firestore.
async fn report_tracked(
    cached_db: &FirestoreDb,
    document_ids: &[&str],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for document_id in document_ids {
        let found: Option<MyTestStructure> = cached_db
            .fluent()
            .select()
            .by_id_in(SECOND_COLLECTION)
            .obj()
            .one(*document_id)
            .await?;

        println!(
            "  {document_id}: {}",
            if found.is_some() {
                "cached"
            } else {
                "not cached"
            }
        );
    }
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
