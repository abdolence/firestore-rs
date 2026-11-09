use crate::common::{delete_documents, eventually_async, populate_collection, setup, unique_name};
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

const DOCUMENT_COUNT: usize = 10;

fn test_document(i: usize) -> MyTestStructure {
    MyTestStructure {
        some_id: format!("test-{i}"),
        some_string: format!("Test value {i}"),
    }
}

#[tokio::test]
async fn precondition_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    let no_preload_name = unique_name("integration-test-caching-mem-no-preload");
    let no_preload_collection = no_preload_name.as_str();
    let preload_name = unique_name("integration-test-caching-mem-preload");
    let preload_collection = preload_name.as_str();

    populate_collection(
        &db,
        no_preload_collection,
        DOCUMENT_COUNT,
        test_document,
        |ms| ms.some_id.clone(),
    )
    .await?;

    populate_collection(
        &db,
        preload_collection,
        DOCUMENT_COUNT,
        test_document,
        |ms| ms.some_id.clone(),
    )
    .await?;

    let cache = FirestoreCache::memory(&db)
        .name("example-mem-cache")
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

    assert_eq!(all_items.len(), DOCUMENT_COUNT);

    // The preloaded collection is complete, so read-through serves it from the cache.
    let all_items = db
        .read_through_cache(&cache)
        .fluent()
        .list()
        .from(preload_collection)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(all_items.len(), DOCUMENT_COUNT);

    let all_items_stream = cached_db
        .fluent()
        .list()
        .from(preload_collection)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?;

    let all_items = all_items_stream.try_collect::<Vec<_>>().await?;

    assert_eq!(all_items.len(), DOCUMENT_COUNT);

    db.fluent()
        .update()
        .fields(paths!(MyTestStructure::some_string))
        .in_col(preload_collection)
        .document_id("test-2")
        .object(&MyTestStructure {
            some_id: "test-2".to_string(),
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

    cache.shutdown().await?;

    let document_ids: Vec<String> = (0..DOCUMENT_COUNT)
        .map(|i| test_document(i).some_id)
        .collect();
    delete_documents(&db, no_preload_collection, &document_ids).await?;
    delete_documents(&db, preload_collection, &document_ids).await?;

    Ok(())
}
