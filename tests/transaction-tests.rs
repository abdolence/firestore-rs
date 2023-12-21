use crate::common::setup;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

    {
        let transaction = db.begin_transaction().await?;
        let db = db.clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(
            transaction.transaction_id.clone(),
        ));
        let object_updated: MyTestStructure = db
            .fluent()
            .update()
            .in_col(TEST_COLLECTION_NAME)
            .precondition(FirestoreWritePrecondition::Exists(true))
            .document_id(&my_struct.some_id)
            .object(&my_struct.clone())
            .execute()
            .await?;
        transaction.commit().await?;
        assert_eq!(object_updated, my_struct);
    }

    // Handling permanent errors
    {
        let res: FirestoreResult<()> = db
            .run_transaction(|_db, _tx| {
                Box::pin(async move {
                    //Test returning an error
                    Err(backoff::Error::Permanent(common::CustomUserError::new(
                        "test error",
                    )))
                })
            })
            .await;
        assert!(res.is_err());
    }

    // Handling transient errors
    {
        let counter = Arc::new(AtomicUsize::new(1));
        let res: FirestoreResult<()> = db
            .run_transaction(|_db, _tx| {
                let counter = counter.fetch_add(1, Ordering::Relaxed);
                Box::pin(async move {
                    if counter > 2 {
                        return Ok(());
                    }
                    //Test returning an error
                    Err(backoff::Error::Transient {
                        err: common::CustomUserError::new("test error"),
                        retry_after: None,
                    })
                })
            })
            .await;
        assert!(res.is_ok());
    }

    Ok(())
}
