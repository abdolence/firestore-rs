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

    const TEST_COLLECTION_NAME: &str = "integration-test-transactions";

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
            transaction.transaction_id().clone(),
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
            transaction.transaction_id().clone(),
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

/// `FirestoreTransactionOps` is public on purpose: it exists so that transaction operations are
/// available on both `FirestoreTransaction` and `FirestoreTransactionData`, which lets callers
/// write transaction-agnostic abstractions over the trait.
/// See https://github.com/abdolence/firestore-rs/issues/206.
///
/// This test only needs to compile; it guards against the trait being made crate private again.
#[allow(dead_code)]
fn transaction_ops_is_usable_as_a_generic_bound<T>(
    ops: &mut T,
    collection_id: &str,
    document_id: &str,
    obj: &MyTestStructure,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: FirestoreTransactionOps,
{
    ops.update_object(collection_id, document_id, obj, None, None, vec![])?;
    ops.delete_by_id(collection_id, document_id, None)?;
    Ok(())
}

#[allow(dead_code)]
fn transaction_ops_works_for_both_implementors(
    transaction: &mut FirestoreTransaction,
    data: &mut FirestoreTransactionData,
    obj: &MyTestStructure,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    transaction_ops_is_usable_as_a_generic_bound(transaction, "c", "id", obj)?;
    transaction_ops_is_usable_as_a_generic_bound(data, "c", "id", obj)?;
    Ok(())
}
