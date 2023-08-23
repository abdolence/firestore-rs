use crate::common::setup;
use firestore::*;

mod common;
use tracing::*;

#[tokio::test]
async fn crud_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const TEST_COLLECTION_NAME: &'static str = "integration-test-transform";

    let db = setup().await?;

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id("test-t0")
        .execute()
        .await?;

    db.fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id("test-t0")
        .document(FirestoreDb::serialize_map_to_doc(
            "",
            [(
                "bar",
                FirestoreValue::from_map([("123", ["inner-value"].into())]),
            )],
        )?)
        .execute()
        .await?;

    let mut transaction = db.begin_transaction().await?;

    db.fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .document_id("test-t0")
        .transforms(|t| t.fields([t.field("bar.`123`").append_missing_elements(["987654321"])]))
        .only_transform()
        .add_to_transaction(&mut transaction)?;

    transaction.commit().await?;

    Ok(())
}
