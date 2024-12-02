use crate::common::setup;
use serde::{Deserialize, Serialize};

mod common;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyParentStructure {
    some_id: String,
    some_string: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyChildStructure {
    some_id: String,
    another_string: String,
}

#[tokio::test]
async fn crud_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const TEST_PARENT_COLLECTION_NAME: &'static str = "integration-nested-test";
    const TEST_CHILD_COLLECTION_NAME: &'static str = "integration-test-childs";

    let db = setup().await?;

    let parent_struct = MyParentStructure {
        some_id: "test-parent".to_string(),
        some_string: "Test".to_string(),
    };

    // Remove if it already exist
    db.fluent()
        .delete()
        .from(TEST_PARENT_COLLECTION_NAME)
        .document_id(&parent_struct.some_id)
        .execute()
        .await?;

    // Creating a parent doc
    db.fluent()
        .insert()
        .into(TEST_PARENT_COLLECTION_NAME)
        .document_id(&parent_struct.some_id)
        .object(&parent_struct)
        .execute::<()>()
        .await?;

    // Creating a child doc
    let child_struct = MyChildStructure {
        some_id: "test-child".to_string(),
        another_string: "TestChild".to_string(),
    };

    // The doc path where we store our childs
    let parent_path = db.parent_path(TEST_PARENT_COLLECTION_NAME, &parent_struct.some_id)?;

    // Remove child doc if exists
    db.fluent()
        .delete()
        .from(TEST_CHILD_COLLECTION_NAME)
        .parent(&parent_path)
        .document_id(&child_struct.some_id)
        .execute()
        .await?;

    // Create a child doc
    db.fluent()
        .insert()
        .into(TEST_CHILD_COLLECTION_NAME)
        .document_id(&child_struct.some_id)
        .parent(&parent_path)
        .object(&child_struct)
        .execute::<()>()
        .await?;

    let find_parent: Option<MyParentStructure> = db
        .fluent()
        .select()
        .by_id_in(TEST_PARENT_COLLECTION_NAME)
        .obj()
        .one(&parent_struct.some_id)
        .await?;

    assert_eq!(find_parent, Some(parent_struct));

    let find_child: Option<MyChildStructure> = db
        .fluent()
        .select()
        .by_id_in(TEST_CHILD_COLLECTION_NAME)
        .parent(&parent_path)
        .obj()
        .one(&child_struct.some_id)
        .await?;

    assert_eq!(find_child, Some(child_struct));

    Ok(())
}
