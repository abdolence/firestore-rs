use crate::common::{eventually_async, populate_collection, setup};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

mod common;
use firestore::*;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
}

const TEST_COLLECTION_NAME: &str = "integration-test-caching-resume-token";
const LISTENER_TARGET: u32 = 1500;
const DELETED_DOC_ID: &str = "test-3";

/// A persistent cache resumes from a stored token, so a token that is no longer valid is the way
/// its listener goes silently stale: the stream stops delivering and the cache serves whatever it
/// happened to hold, including documents deleted in the meantime.
///
/// This drives that path end to end - corrupt the stored token, delete a document while the cache
/// is down, and require the rebuilt cache to converge anyway.
#[tokio::test]
async fn recovers_when_the_stored_resume_token_is_no_longer_valid(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    let data_dir = tempfile::tempdir()?.keep();

    // Start from a known state: the document we delete later has to exist first.
    populate_collection(
        &db,
        TEST_COLLECTION_NAME,
        10,
        |i| MyTestStructure {
            some_id: format!("test-{i}"),
            some_string: format!("Test value {i}"),
        },
        |ms| ms.some_id.clone(),
    )
    .await?;
    sleep(Duration::from_secs(1)).await;

    // First lifetime: preload the collection and let the listener store a resume token.
    let cache = build_cache(&db, &data_dir).await?;

    let token_file = data_dir.join(format!("firestore-listen-token.{LISTENER_TARGET}.tmp"));
    assert!(
        eventually_async(10, Duration::from_millis(500), || {
            let token_file = token_file.clone();
            async move { Ok(token_file.exists()) }
        })
        .await?,
        "the listener never stored a resume token, so there is nothing to invalidate"
    );

    // `shutdown` closes the database, so the second cache below can open the same directory
    // without the first one having been dropped.
    cache.shutdown().await?;

    // Not a token Firestore ever issued.
    std::fs::write(&token_file, "deadbeefdeadbeefdeadbeef")?;

    // Delete while the cache is down. Only a resync can notice this: a target re-added from a
    // point in time is told about documents that exist, never about ones that no longer do.
    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(DELETED_DOC_ID)
        .execute()
        .await?;

    // Second lifetime over the same directory. The collection is already populated, so the cache
    // skips preloading and resumes from the token we just invalidated.
    let cache = build_cache(&db, &data_dir).await?;

    // Firestore reports the invalid token as a target removal carrying `INVALID_ARGUMENT: bad
    // resume token`, so the cache drops the collection and stops serving listings from it.
    let cached_db = db.read_through_cache(&cache);
    let stopped_serving_the_deleted_doc = eventually_async(12, Duration::from_millis(500), {
        let cached_db = cached_db.clone();
        move || {
            let cached_db = cached_db.clone();
            async move {
                let deleted: Option<MyTestStructure> = cached_db
                    .fluent()
                    .select()
                    .by_id_in(TEST_COLLECTION_NAME)
                    .obj()
                    .one(DELETED_DOC_ID)
                    .await?;
                Ok(deleted.is_none())
            }
        }
    })
    .await?;

    // ...and then the listener re-adds the target without the bad token, Firestore replays the
    // collection, and the cache serves it on its own again - now without the deleted document.
    // This is the half that proves the recovery finished rather than merely failing open.
    let cached_only_db = db.read_cached_only(&cache);
    let repopulated = eventually_async(8, Duration::from_secs(1), move || {
        let cached_only_db = cached_only_db.clone();
        async move {
            // While the collection is being replayed the cache refuses the listing rather than
            // answering it partially, so an error here means "not recovered yet", not a failure.
            let listed: FirestoreResult<Vec<MyTestStructure>> = async {
                cached_only_db
                    .fluent()
                    .list()
                    .from(TEST_COLLECTION_NAME)
                    .obj::<MyTestStructure>()
                    .stream_all_with_errors()
                    .await?
                    .try_collect()
                    .await
            }
            .await;

            Ok(matches!(listed, Ok(docs) if docs.len() == 9
                && !docs.iter().any(|d| d.some_id == DELETED_DOC_ID)))
        }
    })
    .await?;

    cache.shutdown().await?;
    drop(cache);
    std::fs::remove_dir_all(&data_dir).ok();

    assert!(
        stopped_serving_the_deleted_doc,
        "the cache kept serving a document deleted while its resume token was invalid"
    );
    assert!(
        repopulated,
        "the cache never recovered: the target was not re-added and replayed after the bad token"
    );

    Ok(())
}

async fn build_cache(
    db: &FirestoreDb,
    data_dir: &std::path::Path,
) -> Result<FirestorePersistentCache, Box<dyn std::error::Error + Send + Sync>> {
    Ok(FirestorePersistentCache::builder(db)
        .name("resume-token-cache")
        .data_dir(data_dir)
        .collection_with(TEST_COLLECTION_NAME, |c| {
            c.preload_all_if_empty().listener_target(LISTENER_TARGET)
        })
        .build()
        .await?)
}
