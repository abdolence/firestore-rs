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
async fn transaction_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    const TEST_COLLECTION_NAME: &'static str = "integration-test-transactions";

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

    {
        let transaction = db.begin_transaction().await?;
        let db = db.clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(
            transaction.transaction_id.clone(),
        ));
        db.fluent()
            .select()
            .by_id_in(TEST_COLLECTION_NAME)
            .obj::<MyTestStructure>()
            .one(&my_struct.some_id)
            .await?;
        transaction.commit().await?;
    }

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
