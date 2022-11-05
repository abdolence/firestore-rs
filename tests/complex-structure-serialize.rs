use chrono::{DateTime, Utc};
use firestore::*;
use serde::{Deserialize, Serialize};

mod common;
use crate::common::setup;

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct Test1(pub u8);

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct Test1i(pub Test1);

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct Test2 {
    some_id: String,
    some_bool: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum TestEnum {
    TestChoice,
    TestWithParam(String),
    TestWithMultipleParams(String, String),
    TestWithStruct(Test2),
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
    some_num: u64,
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: DateTime<Utc>,
    test1: Test1,
    test1i: Test1i,
    test11: Option<Test1>,
    test2: Option<Test2>,
    test3: Vec<Test2>,
    test4: TestEnum,
    test5: (TestEnum, TestEnum),
    test6: TestEnum,
    test7: TestEnum,
}

#[tokio::test]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    const TEST_COLLECTION_NAME: &'static str = "integration-test-complex";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "Test".to_string(),
        some_num: 41,
        created_at: Utc::now(),
        test1: Test1(1),
        test1i: Test1i(Test1(1)),
        test11: Some(Test1(1)),
        test2: Some(Test2 {
            some_id: "test-1".to_string(),
            some_bool: Some(true),
        }),
        test3: vec![
            Test2 {
                some_id: "test-2".to_string(),
                some_bool: Some(false),
            },
            Test2 {
                some_id: "test-2".to_string(),
                some_bool: Some(true),
            },
        ],
        test4: TestEnum::TestChoice,
        test5: (TestEnum::TestChoice, TestEnum::TestChoice),
        test6: TestEnum::TestWithMultipleParams("ss".to_string(), "ss".to_string()),
        test7: TestEnum::TestWithStruct(Test2 {
            some_id: "test-2".to_string(),
            some_bool: Some(true),
        }),
    };

    // Remove if it already exist
    db.delete_by_id(TEST_COLLECTION_NAME, &my_struct.some_id)
        .await?;

    // Let's insert some data
    db.create_obj(TEST_COLLECTION_NAME, &my_struct.some_id, &my_struct)
        .await?;

    let to_update = MyTestStructure {
        some_num: my_struct.some_num + 1,
        some_string: "updated-value".to_string(),
        ..my_struct.clone()
    };

    // Update some field in it
    let updated_obj: MyTestStructure = db
        .update_obj(
            TEST_COLLECTION_NAME,
            &my_struct.some_id,
            &to_update,
            Some(paths!(MyTestStructure::{
                some_num,
                some_string
            })),
        )
        .await?;

    // Get object by id
    let find_it_again: MyTestStructure =
        db.get_obj(TEST_COLLECTION_NAME, &my_struct.some_id).await?;

    assert_eq!(updated_obj.some_num, to_update.some_num);
    assert_eq!(updated_obj.some_string, to_update.some_string);
    assert_eq!(updated_obj.test1, to_update.test1);

    assert_eq!(updated_obj.some_num, find_it_again.some_num);
    assert_eq!(updated_obj.some_string, find_it_again.some_string);
    assert_eq!(updated_obj.test1, find_it_again.test1);

    Ok(())
}
