use approx::relative_eq;
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
    #[serde(default)]
    #[serde(with = "firestore::serialize_as_optional_timestamp")]
    updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(with = "firestore::serialize_as_null_timestamp")]
    updated_at_as_null: Option<DateTime<Utc>>,
    test1: Test1,
    test1i: Test1i,
    test11: Option<Test1>,
    test2: Option<Test2>,
    test3: Vec<Test2>,
    test4: TestEnum,
    test5: (TestEnum, TestEnum),
    test6: TestEnum,
    test7: TestEnum,
    #[serde(default)]
    #[serde(with = "firestore::serialize_as_null")]
    test_null1: Option<String>,
    #[serde(default)]
    #[serde(with = "firestore::serialize_as_null")]
    test_null2: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyFloatStructure {
    some_f32: f32,
    some_f64: f64,
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
        updated_at: None,
        updated_at_as_null: None,
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
        test_null1: None,
        test_null2: Some("Test".to_string()),
    };

    // Remove if it already exist
    db.delete_by_id(TEST_COLLECTION_NAME, &my_struct.some_id, None)
        .await?;

    // Let's insert some data
    db.create_obj(
        TEST_COLLECTION_NAME,
        Some(&my_struct.some_id),
        &my_struct,
        None,
    )
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
            None,
            None,
        )
        .await?;

    // Get object by id
    let find_it_again: MyTestStructure =
        db.get_obj(TEST_COLLECTION_NAME, &my_struct.some_id).await?;

    assert_eq!(updated_obj.some_num, to_update.some_num);
    println!("updated_obj.some_num: {:?}", to_update.some_num);

    assert_eq!(updated_obj.some_string, to_update.some_string);
    assert_eq!(updated_obj.test1, to_update.test1);

    assert_eq!(updated_obj.some_num, find_it_again.some_num);
    assert_eq!(updated_obj.some_string, find_it_again.some_string);
    assert_eq!(updated_obj.test1, find_it_again.test1);

    let my_float_structure = MyFloatStructure {
        some_f32: 42.0,
        some_f64: 42.0,
    };
    let my_float_structure_returned: MyFloatStructure = db
        .fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .document_id("test-floats")
        .object(&my_float_structure)
        .execute()
        .await?;

    assert!(relative_eq!(
        my_float_structure_returned.some_f32,
        my_float_structure.some_f32
    ));
    assert!(relative_eq!(
        my_float_structure_returned.some_f64,
        my_float_structure.some_f64
    ));

    Ok(())
}
