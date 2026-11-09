use crate::common::{eventually_async, populate_collection, setup, unique_name};
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

/// Removes the documents `populate_collection` wrote to a collection that only this run uses.
async fn delete_populated_documents(
    db: &FirestoreDb,
    collection: &str,
    count: usize,
) -> Result<(), FirestoreError> {
    for i in 0..count {
        db.fluent()
            .delete()
            .from(collection)
            .document_id(format!("test-{i}"))
            .execute()
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn precondition_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    let no_preload_name = unique_name("integration-test-caching-persistent-no-preload");
    let no_preload_collection = no_preload_name.as_str();
    let preload_name = unique_name("integration-test-caching-persistent-preload");
    let preload_collection = preload_name.as_str();

    populate_collection(
        &db,
        no_preload_collection,
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
        preload_collection,
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
        .collection_with(no_preload_collection, |c| {
            c.preload_none().listener_target(1000)
        })
        .collection_with(preload_collection, |c| {
            c.preload_all().listener_target(1001)
        })
        .build()
        .await?;

    let my_struct: Option<MyTestStructure> = db
        .read_cached_only(&cache)
        .fluent()
        .select()
        .by_id_in(no_preload_collection)
        .obj()
        .one("test-0")
        .await?;

    assert!(my_struct.is_none());

    let my_struct: Option<MyTestStructure> = db
        .read_cached_only(&cache)
        .fluent()
        .select()
        .by_id_in(preload_collection)
        .obj()
        .one("test-0")
        .await?;

    assert!(my_struct.is_some());

    db.read_through_cache(&cache)
        .fluent()
        .select()
        .by_id_in(no_preload_collection)
        .obj::<MyTestStructure>()
        .one("test-1")
        .await?;

    let my_struct: Option<MyTestStructure> = db
        .read_cached_only(&cache)
        .fluent()
        .select()
        .by_id_in(no_preload_collection)
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
        .from(no_preload_collection)
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
        .from(no_preload_collection)
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
        .from(preload_collection)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(all_items.len(), 10);

    db.fluent()
        .update()
        .fields(paths!(MyTestStructure::some_string))
        .in_col(preload_collection)
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
                    .by_id_in(preload_collection)
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
        .from(preload_collection)
        .filter(|q| {
            q.for_all([q
                .field(path!(MyTestStructure::some_num))
                .greater_than_or_equal(5)])
        })
        .order(|o| o.fields([o.field(path!(MyTestStructure::some_num)).desc()]))
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
        .from(preload_collection)
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
                    .from(preload_collection)
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

    delete_populated_documents(&db, no_preload_collection, 10).await?;
    delete_populated_documents(&db, preload_collection, 10).await?;

    Ok(())
}
