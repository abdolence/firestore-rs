use crate::common::{eventually_async, populate_collection, setup};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

mod common;
use firestore::errors::FirestoreError;
use firestore::*;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyTestStructure {
    some_id: String,
    some_num: u64,
    some_string: String,
}

#[tokio::test]
async fn precondition_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    const TEST_COLLECTION_NAME_NO_PRELOAD: &str = "integration-test-caching-persistent-no-preload";
    const TEST_COLLECTION_NAME_PRELOAD: &str = "integration-test-caching-persistent-preload";

    populate_collection(
        &db,
        TEST_COLLECTION_NAME_NO_PRELOAD,
        10,
        |i| MyTestStructure {
            some_id: format!("test-{}", i),
            some_num: i as u64,
            some_string: format!("Test value {}", i),
        },
        |ms| ms.some_id.clone(),
    )
    .await?;

    populate_collection(
        &db,
        TEST_COLLECTION_NAME_PRELOAD,
        10,
        |i| MyTestStructure {
            some_id: format!("test-{}", i),
            some_num: i as u64,
            some_string: format!("Test value {}", i),
        },
        |ms| ms.some_id.clone(),
    )
    .await?;
    sleep(Duration::from_secs(1)).await;

    let temp_db_dir = tempfile::tempdir()?;

    let cache = FirestorePersistentCache::builder(&db)
        .name("example-persistent-cache")
        .data_dir(temp_db_dir.keep())
        .collection_with(TEST_COLLECTION_NAME_NO_PRELOAD, |c| {
            c.preload_none().listener_target(1000)
        })
        .collection_with(TEST_COLLECTION_NAME_PRELOAD, |c| {
            c.preload_all().listener_target(1001)
        })
        .build()
        .await?;

    let my_struct: Option<MyTestStructure> = db
        .read_cached_only(&cache)
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME_NO_PRELOAD)
        .obj()
        .one("test-0")
        .await?;

    assert!(my_struct.is_none());

    let my_struct: Option<MyTestStructure> = db
        .read_cached_only(&cache)
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME_PRELOAD)
        .obj()
        .one("test-0")
        .await?;

    assert!(my_struct.is_some());

    db.read_through_cache(&cache)
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME_NO_PRELOAD)
        .obj::<MyTestStructure>()
        .one("test-1")
        .await?;

    let my_struct: Option<MyTestStructure> = db
        .read_cached_only(&cache)
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME_NO_PRELOAD)
        .obj()
        .one("test-1")
        .await?;

    assert!(my_struct.is_some());

    let cached_db = db.read_cached_only(&cache);

    // The lazily populated collection holds only the documents that happened to be read through
    // it, so listing it from the cache alone must fail rather than return a partial result.
    let listing_result = cached_db
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME_NO_PRELOAD)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await;

    assert!(matches!(
        listing_result.err(),
        Some(FirestoreError::CacheError(_))
    ));

    // Reading through the cache falls back to Firestore for the same collection.
    let all_items = db
        .read_through_cache(&cache)
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME_NO_PRELOAD)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(all_items.len(), 10);

    // The preloaded collection is complete, so it is served from the cache.
    let all_items = cached_db
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME_PRELOAD)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(all_items.len(), 10);

    db.fluent()
        .update()
        .fields(paths!(MyTestStructure::some_string))
        .in_col(TEST_COLLECTION_NAME_PRELOAD)
        .document_id("test-2")
        .object(&MyTestStructure {
            some_id: "test-2".to_string(),
            some_num: 2,
            some_string: "updated".to_string(),
        })
        .execute::<()>()
        .await?;

    let cached_db = db.read_cached_only(&cache);
    assert!(
        eventually_async(10, Duration::from_millis(500), move || {
            let cached_db = cached_db.clone();
            async move {
                let my_struct: Option<MyTestStructure> = cached_db
                    .fluent()
                    .select()
                    .by_id_in(TEST_COLLECTION_NAME_PRELOAD)
                    .obj()
                    .one("test-2")
                    .await?;

                if let Some(my_struct) = my_struct {
                    return Ok(my_struct.some_string.as_str() == "updated");
                }
                Ok(false)
            }
        })
        .await?
    );

    let cached_db = db.read_cached_only(&cache);
    let queried = cached_db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME_PRELOAD)
        .filter(|q| {
            q.for_all([q
                .field(path!(MyTestStructure::some_num))
                .greater_than_or_equal(5)])
        })
        .order_by([(
            path!(MyTestStructure::some_num),
            FirestoreQueryDirection::Descending,
        )])
        .obj::<MyTestStructure>()
        .stream_query_with_errors()
        .await?;

    let queried_items = queried.try_collect::<Vec<_>>().await?;
    assert_eq!(queried_items.len(), 5);
    assert_eq!(queried_items.first().map(|d| d.some_num), Some(9));

    // Deleting a document must be propagated to the persistent cache. This guards against the
    // delete transaction being opened but never committed, which used to leave deleted documents
    // in the cache forever.
    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME_PRELOAD)
        .document_id("test-3")
        .execute()
        .await?;

    let cached_db = db.read_cached_only(&cache);
    assert!(
        eventually_async(10, Duration::from_millis(500), move || {
            let cached_db = cached_db.clone();
            async move {
                let all_items = cached_db
                    .fluent()
                    .list()
                    .from(TEST_COLLECTION_NAME_PRELOAD)
                    .obj::<MyTestStructure>()
                    .stream_all_with_errors()
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;

                Ok(!all_items.iter().any(|i| i.some_id == "test-3"))
            }
        })
        .await?
    );

    cache.shutdown().await?;

    Ok(())
}
