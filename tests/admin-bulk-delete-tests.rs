//! Runs bulk delete of a collection group against a real Firestore project (`GCP_PROJECT`, local
//! ADC). Uses a dedicated collection group plus a sibling group that must survive, so it never
//! touches anything a caller's own data depends on.
//!
//! Ignored by default: a real `BulkDeleteDocuments` operation, even on a handful of documents,
//! is a background job and can take longer than a fast test suite should wait on.
//! `GCP_PROJECT=<project> cargo test --test admin-bulk-delete-tests --features admin -- --ignored --nocapture`.

use firestore::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

mod common;
use common::{eventually_async, populate_collection, setup};

const TEST_GROUP: &str = "firestore-rs-bulk-delete-test";
const SIBLING_GROUP: &str = "firestore-rs-bulk-delete-sibling";
const SIBLING_DOC_ID: &str = "sibling-must-survive";

const WAIT_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct BulkDeleteTestDoc {
    some_id: String,
    some_string: String,
}

async fn collection_is_empty(
    db: &FirestoreDb,
    collection_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let docs = db.fluent().select().from(collection_id).query().await?;
    Ok(docs.is_empty())
}

#[tokio::test]
#[ignore = "writes and bulk-deletes real documents in GCP_PROJECT; run with --ignored"]
async fn bulk_delete_removes_only_the_named_group(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    // Step 1: the test group must be empty before this test writes anything into it, or a
    // leftover document from an earlier failed run would make step 4's assertion meaningless.
    if !collection_is_empty(&db, TEST_GROUP).await? {
        return Err(format!(
            "{TEST_GROUP:?} is not empty; aborting rather than bulk-deleting an unknown state"
        )
        .into());
    }

    // Step 2: a handful of documents in the test group, plus one in a sibling group that a bulk
    // delete scoped to the test group must never touch.
    populate_collection(
        &db,
        TEST_GROUP,
        5,
        |i| BulkDeleteTestDoc {
            some_id: format!("doc-{i}"),
            some_string: format!("bulk delete fixture {i}"),
        },
        |doc| doc.some_id.clone(),
    )
    .await?;

    db.fluent()
        .insert()
        .into(SIBLING_GROUP)
        .document_id(SIBLING_DOC_ID)
        .object(&BulkDeleteTestDoc {
            some_id: SIBLING_DOC_ID.to_string(),
            some_string: "must survive the bulk delete".to_string(),
        })
        .execute::<BulkDeleteTestDoc>()
        .await?;

    // Step 3: bulk-delete only the test group, waiting for it to finish.
    let result = db
        .fluent()
        .delete()
        .bulk()
        .collection_groups([TEST_GROUP])
        .wait_until_done(WAIT_TIMEOUT)
        .execute()
        .await?;
    println!("[bulk delete] result:\n{result}");
    assert_eq!(
        result.collection_groups,
        vec![FirestoreCollectionId::new(TEST_GROUP)?]
    );

    // Step 4: the test group is empty, and the sibling group's document survived. Bulk delete
    // completing does not guarantee the very next read observes it, so this tolerates a short
    // settling delay rather than asserting on the first read.
    let test_group_emptied = eventually_async(10, Duration::from_secs(2), || {
        let db = db.clone();
        async move { collection_is_empty(&db, TEST_GROUP).await }
    })
    .await?;
    assert!(
        test_group_emptied,
        "the test group must be empty after the bulk delete"
    );

    let sibling = db
        .fluent()
        .select()
        .by_id_in(SIBLING_GROUP)
        .obj::<BulkDeleteTestDoc>()
        .one(SIBLING_DOC_ID)
        .await?;
    assert!(
        sibling.is_some(),
        "the sibling group's document must survive a bulk delete scoped to another group"
    );

    // Step 5: clean up the sibling document this test itself created.
    db.fluent()
        .delete()
        .from(SIBLING_GROUP)
        .document_id(SIBLING_DOC_ID)
        .execute()
        .await?;

    Ok(())
}
