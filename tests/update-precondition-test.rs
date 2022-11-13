use crate::common::setup;
use serde::{Deserialize, Serialize};

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

    const TEST_COLLECTION_NAME: &'static str = "integration-test-precondition";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "Test".to_string(),
    };

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .execute()
        .await?;

    let should_fail: FirestoreResult<()> = db
        .fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .precondition(FirestoreWritePrecondition::Exists(true))
        .document_id(&my_struct.some_id)
        .object(&MyTestStructure {
            some_string: "created-value".to_string(),
            ..my_struct.clone()
        })
        .execute()
        .await;

    assert!(should_fail.is_err());

    let object_created: MyTestStructure = db
        .fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .precondition(FirestoreWritePrecondition::Exists(false))
        .document_id(&my_struct.some_id)
        .object(&my_struct.clone())
        .execute()
        .await?;

    assert_eq!(object_created, my_struct);

    let object_updated: MyTestStructure = db
        .fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .precondition(FirestoreWritePrecondition::Exists(true))
        .document_id(&my_struct.some_id)
        .object(&my_struct.clone())
        .execute()
        .await?;

    assert_eq!(object_updated, my_struct);

    Ok(())
}
