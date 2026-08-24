use crate::common::{eventually_async, populate_collection, setup};
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

const TEST_COLLECTION_NAME: &str = "integration-test-caching-watched-docs";
const WATCHED: [&str; 2] = ["test-0", "test-1"];
const UNWATCHED: &str = "test-4";

/// A cache limited to named documents subscribes to exactly those documents, rather than to the
/// whole collection. This is the shape to use for configuration and reference data, where the set
/// of interesting documents is known up front.
#[tokio::test]
async fn a_cache_limited_to_named_documents_ignores_the_rest_of_the_collection(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    populate_collection(
        &db,
        TEST_COLLECTION_NAME,
        5,
        |i| MyTestStructure {
            some_id: format!("test-{i}"),
            some_string: format!("Test value {i}"),
        },
        |ms| ms.some_id.clone(),
    )
    .await?;

    let cache = FirestoreCache::memory(&db)
        .name("watched-documents-cache")
        .collection_with(TEST_COLLECTION_NAME, |c| c.documents(WATCHED).preload_all())
        .build()
        .await?;

    let cached_db = db.read_cached_only(&cache);

    // Only the watched documents were downloaded.
    for document_id in WATCHED {
        let found: Option<MyTestStructure> = cached_db
            .fluent()
            .select()
            .by_id_in(TEST_COLLECTION_NAME)
            .obj()
            .one(document_id)
            .await?;
        assert!(found.is_some(), "{document_id} should have been preloaded");
    }

    let unwatched: Option<MyTestStructure> = db
        .read_through_cache(&cache)
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one(UNWATCHED)
        .await?;
    assert!(
        unwatched.is_some(),
        "read-through should still fetch an unwatched document from Firestore"
    );

    // Holding a chosen subset, it must refuse to list the collection rather than answer partially.
    let listing = cached_db
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await
        .map(|_| ());
    assert!(
        matches!(listing, Err(FirestoreError::CacheError(_))),
        "a documents watch must not be listable, got {listing:?}"
    );

    // A change to a watched document reaches the cache...
    update(&db, WATCHED[0], "updated").await?;
    assert!(
        eventually_async(10, Duration::from_millis(500), {
            let cached_db = cached_db.clone();
            move || {
                let cached_db = cached_db.clone();
                async move {
                    Ok(cached_string(&cached_db, WATCHED[0]).await? == Some("updated".to_string()))
                }
            }
        })
        .await?,
        "a watched document should be kept up to date"
    );

    // ...while a change to an unwatched one never does, because the target does not cover it.
    update(&db, UNWATCHED, "should-not-be-cached").await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        cached_string(&cached_db, UNWATCHED).await?,
        None,
        "an unwatched document must never enter the cache"
    );

    cache.shutdown().await?;

    // Each backend builds its listener targets on its own path, so assert the shape of the target
    // itself. Checking cached contents instead would prove nothing here: writes are filtered to
    // the watched set anyway, so a target covering the whole collection would look identical from
    // the cache while quietly streaming every unrelated change in it.
    for (name, targets) in [
        ("memory", memory_backend_targets(&db).await?),
        ("persistent", persistent_backend_targets(&db).await?),
    ] {
        assert_eq!(targets.len(), 1, "{name}: expected one target");
        match &targets[0].target_type {
            FirestoreTargetType::Documents(documents) => {
                assert_eq!(documents.collection, TEST_COLLECTION_NAME, "{name}");
                assert_eq!(documents.documents, WATCHED.to_vec(), "{name}");
            }
            other => panic!(
                "{name}: a watched-documents collection must listen on a documents target, got \
                 {other:?}"
            ),
        }
    }

    Ok(())
}

fn watched_configuration(db: &FirestoreDb) -> FirestoreCacheConfiguration {
    FirestoreCacheConfiguration::new().add_collection_config(
        db,
        FirestoreCacheCollectionConfiguration::new(
            TEST_COLLECTION_NAME,
            FirestoreListenerTarget::new(1600),
            FirestoreCacheCollectionLoadMode::PreloadAllDocs,
        )
        .with_documents(WATCHED),
    )
}

async fn memory_backend_targets(
    db: &FirestoreDb,
) -> Result<Vec<FirestoreListenerTargetParams>, Box<dyn std::error::Error + Send + Sync>> {
    let backend = FirestoreMemoryCacheBackend::new(watched_configuration(db))?;
    Ok(backend
        .load(&FirestoreCacheOptions::new("watched-memory".into()), db)
        .await?)
}

async fn persistent_backend_targets(
    db: &FirestoreDb,
) -> Result<Vec<FirestoreListenerTargetParams>, Box<dyn std::error::Error + Send + Sync>> {
    let data_dir = tempfile::tempdir()?;
    let backend = FirestorePersistentCacheBackend::with_options(
        watched_configuration(db),
        data_dir.path().join("redb"),
    )?;
    let targets = backend
        .load(&FirestoreCacheOptions::new("watched-persistent".into()), db)
        .await?;
    backend.shutdown().await?;
    Ok(targets)
}

async fn cached_string(db: &FirestoreDb, document_id: &str) -> FirestoreResult<Option<String>> {
    let found: Option<MyTestStructure> = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one(document_id)
        .await?;
    Ok(found.map(|doc| doc.some_string))
}

async fn update(
    db: &FirestoreDb,
    document_id: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .update()
        .fields(paths!(MyTestStructure::some_string))
        .in_col(TEST_COLLECTION_NAME)
        .document_id(document_id)
        .object(&MyTestStructure {
            some_id: document_id.to_string(),
            some_string: value.to_string(),
        })
        .execute::<()>()
        .await?;
    Ok(())
}
