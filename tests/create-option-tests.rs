use crate::common::setup;
use serde::{Deserialize, Serialize};

mod common;
use firestore::*;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyTestStructure {
    some_id: String,
    some_string: Option<String>,
}

#[tokio::test]
async fn crud_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const TEST_COLLECTION_NAME: &str = "integration-test-options";

    let db = setup().await?;

    let my_struct1 = MyTestStructure {
        some_id: "test-0".to_string(),
        some_string: Some("some_string".to_string()),
    };

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct1.some_id)
        .execute()
        .await?;

    let object_returned: MyTestStructure = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct1.some_id)
        .object(&my_struct1)
        .execute()
        .await?;

    assert_eq!(object_returned, my_struct1);

    let object_updated: MyTestStructure = db
        .fluent()
        .update()
        .fields(paths!(MyTestStructure::{some_string}))
        .in_col(TEST_COLLECTION_NAME)
        .document_id(&my_struct1.some_id)
        .object(&MyTestStructure {
            some_string: None,
            ..my_struct1.clone()
        })
        .execute()
        .await?;

    assert_eq!(
        object_updated,
        MyTestStructure {
            some_string: None,
            ..my_struct1.clone()
        }
    );

    let find_it_again: Option<MyTestStructure> = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one(&my_struct1.some_id)
        .await?;

    assert_eq!(Some(object_updated), find_it_again);

    Ok(())
}
