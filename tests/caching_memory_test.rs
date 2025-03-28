use crate::common::{eventually_async, populate_collection, setup};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

mod common;
use firestore::*;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
}

#[tokio::test]
async fn precondition_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    const TEST_COLLECTION_NAME_NO_PRELOAD: &'static str = "integration-test-caching-mem-no-preload";
    const TEST_COLLECTION_NAME_PRELOAD: &'static str = "integration-test-caching-mem-preload";

    populate_collection(
        &db,
        TEST_COLLECTION_NAME_NO_PRELOAD,
        10,
        |i| MyTestStructure {
            some_id: format!("test-{}", i),
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
            some_string: format!("Test value {}", i),
        },
        |ms| ms.some_id.clone(),
    )
    .await?;

    let mut cache = FirestoreCache::new(
        "example-mem-cache".into(),
        &db,
        FirestoreMemoryCacheBackend::new(
            FirestoreCacheConfiguration::new()
                .add_collection_config(
                    &db,
                    FirestoreCacheCollectionConfiguration::new(
                        TEST_COLLECTION_NAME_NO_PRELOAD,
                        FirestoreListenerTarget::new(1000),
                        FirestoreCacheCollectionLoadMode::PreloadNone,
                    ),
                )
                .add_collection_config(
                    &db,
                    FirestoreCacheCollectionConfiguration::new(
                        TEST_COLLECTION_NAME_PRELOAD,
                        FirestoreListenerTarget::new(1001),
                        FirestoreCacheCollectionLoadMode::PreloadAllDocs,
                    ),
                ),
        )?,
        FirestoreMemListenStateStorage::new(),
    )
    .await?;

    cache.load().await?;

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
    let all_items_stream = cached_db
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME_NO_PRELOAD)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?;

    let all_items = all_items_stream.try_collect::<Vec<_>>().await?;

    assert_eq!(all_items.len(), 1);

    let all_items_stream = cached_db
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME_PRELOAD)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?;

    let all_items = all_items_stream.try_collect::<Vec<_>>().await?;

    assert_eq!(all_items.len(), 10);

    db.fluent()
        .update()
        .fields(paths!(MyTestStructure::some_string))
        .in_col(TEST_COLLECTION_NAME_PRELOAD)
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

    cache.shutdown().await?;

    Ok(())
}
