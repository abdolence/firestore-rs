use crate::common::setup;
use chrono::prelude::*;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

mod common;
use firestore::*;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
    one_more_string: String,
    some_num: u64,
    created_at: DateTime<Utc>,
}

#[tokio::test]
async fn crud_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const TEST_COLLECTION_NAME: &'static str = "integration-test-query";

    let db = setup().await?;

    let my_struct1 = MyTestStructure {
        some_id: format!("test-0"),
        some_string: "some_string".to_string(),
        one_more_string: "one_more_string".to_string(),
        some_num: 42,
        created_at: Utc::now(),
    };

    let my_struct2 = MyTestStructure {
        some_id: format!("test-1"),
        some_string: "some_string-1".to_string(),
        one_more_string: "one_more_string-1".to_string(),
        some_num: 17,
        created_at: Utc::now(),
    };

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct1.some_id)
        .execute()
        .await?;

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct2.some_id)
        .execute()
        .await?;

    db.fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct1.some_id)
        .object(&my_struct1)
        .execute::<()>()
        .await?;

    db.fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct2.some_id)
        .object(&my_struct2)
        .execute::<()>()
        .await?;

    let object_stream: BoxStream<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .filter(|q| {
            q.for_all([
                q.field(path!(MyTestStructure::some_num)).is_not_null(),
                q.field(path!(MyTestStructure::some_string))
                    .eq("some_string"),
            ])
        })
        .order_by([(
            path!(MyTestStructure::some_num),
            FirestoreQueryDirection::Descending,
        )])
        .obj()
        .stream_query()
        .await?;

    let objects_as_vec1: Vec<MyTestStructure> = object_stream.collect().await;
    assert_eq!(objects_as_vec1, vec![my_struct1.clone()]);

    let object_stream: BoxStream<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .filter(|q| {
            q.for_any([
                q.field(path!(MyTestStructure::some_string))
                    .eq("some_string"),
                q.field(path!(MyTestStructure::some_string))
                    .eq("some_string-1"),
            ])
        })
        .order_by([(
            path!(MyTestStructure::some_num),
            FirestoreQueryDirection::Descending,
        )])
        .obj()
        .stream_query()
        .await?;

    let objects_as_vec2: Vec<MyTestStructure> = object_stream.collect().await;
    assert_eq!(objects_as_vec2, vec![my_struct1, my_struct2]);

    Ok(())
}
