use crate::common::{eventually_async, populate_collection, setup};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

mod common;
use firestore::errors::FirestoreError;
use firestore::*;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
}

const FIRST_COLLECTION: &str = "integration-test-caching-dynamic-first";
const SECOND_COLLECTION: &str = "integration-test-caching-dynamic-second";

/// Collections can be added to and removed from a live cache, so applications whose set of cached
/// collections changes do not have to tear the cache down and preload everything again.
#[tokio::test]
async fn collections_can_be_added_and_removed_on_a_live_cache(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    for collection in [FIRST_COLLECTION, SECOND_COLLECTION] {
        populate_collection(
            &db,
            collection,
            5,
            |i| MyTestStructure {
                some_id: format!("test-{i}"),
                some_string: format!("Test value {i}"),
            },
            |ms| ms.some_id.clone(),
        )
        .await?;
    }

    let cache = FirestoreCache::memory(&db)
        .name("dynamic-collections-cache")
        .preloaded_collection(FIRST_COLLECTION)
        .build()
        .await?;

    // Derived *before* the second collection is added. It shares the cache's backend rather than a
    // copy, so it must see the addition without being recreated.
    let cached_only_db = db.read_cached_only(&cache);

    assert_eq!(cache.cached_collections().len(), 1);

    // Not cached yet, so a cache-only listing has to refuse rather than answer emptily.
    let before = list_cached(&cached_only_db, SECOND_COLLECTION).await;
    assert!(
        matches!(before, Err(FirestoreError::CacheError(_))),
        "an uncached collection should not be listable, got {before:?}"
    );

    cache
        .add_collection(FirestoreCacheCollection::new(SECOND_COLLECTION).preload_all())
        .await?;

    assert_eq!(cache.cached_collections().len(), 2);

    let listed = list_cached(&cached_only_db, SECOND_COLLECTION).await?;
    assert_eq!(listed.len(), 5);

    // The listener covers the new collection too, so a write reaches the cache.
    db.fluent()
        .update()
        .fields(paths!(MyTestStructure::some_string))
        .in_col(SECOND_COLLECTION)
        .document_id("test-2")
        .object(&MyTestStructure {
            some_id: "test-2".to_string(),
            some_string: "updated".to_string(),
        })
        .execute::<()>()
        .await?;

    let propagated = eventually_async(10, Duration::from_millis(500), {
        let cached_only_db = cached_only_db.clone();
        move || {
            let cached_only_db = cached_only_db.clone();
            async move {
                let found: Option<MyTestStructure> = cached_only_db
                    .fluent()
                    .select()
                    .by_id_in(SECOND_COLLECTION)
                    .obj()
                    .one("test-2")
                    .await?;
                Ok(matches!(found, Some(doc) if doc.some_string == "updated"))
            }
        }
    })
    .await?;
    assert!(
        propagated,
        "the listener did not pick up the collection added at runtime"
    );

    cache.remove_collection(SECOND_COLLECTION).await?;

    assert_eq!(cache.cached_collections().len(), 1);
    let after = list_cached(&cached_only_db, SECOND_COLLECTION).await;
    assert!(
        matches!(after, Err(FirestoreError::CacheError(_))),
        "a removed collection should no longer be listable, got {after:?}"
    );

    // Removing one collection must not disturb the other.
    assert_eq!(
        list_cached(&cached_only_db, FIRST_COLLECTION).await?.len(),
        5
    );

    assert!(!cache.remove_collection(SECOND_COLLECTION).await?);

    cache.shutdown().await?;
    Ok(())
}

async fn list_cached(db: &FirestoreDb, collection: &str) -> FirestoreResult<Vec<MyTestStructure>> {
    db.fluent()
        .list()
        .from(collection)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?
        .try_collect()
        .await
}
