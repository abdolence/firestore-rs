use crate::common::setup;
use chrono::{DateTime, Utc};
use firestore::{path, paths, FirestoreQueryDirection};
use futures::StreamExt;
use futures_util::stream::BoxStream;
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
    const TEST_COLLECTION_NAME: &'static str = "integration-test-crud";

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

    let object_returned: MyTestStructure = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct1.some_id)
        .object(&my_struct1)
        .execute()
        .await?;

    db.fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct2.some_id)
        .object(&my_struct2)
        .execute()
        .await?;

    assert_eq!(object_returned, my_struct1);

    let object_updated: MyTestStructure = db
        .fluent()
        .update()
        .fields(paths!(MyTestStructure::{some_num, one_more_string}))
        .in_col(TEST_COLLECTION_NAME)
        .document_id(&my_struct1.some_id)
        .object(&MyTestStructure {
            some_num: my_struct1.some_num + 1,
            some_string: "should-not-change".to_string(),
            one_more_string: "updated-value".to_string(),
            ..my_struct1.clone()
        })
        .execute()
        .await?;

    assert_eq!(
        object_updated,
        MyTestStructure {
            some_num: my_struct1.some_num + 1,
            one_more_string: "updated-value".to_string(),
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

    assert_eq!(Some(object_updated.clone()), find_it_again);

    let get_both_stream: BoxStream<Option<MyTestStructure>> = Box::pin(
        db.batch_stream_get_objects(
            TEST_COLLECTION_NAME,
            [&my_struct1.some_id, &my_struct2.some_id],
            None,
        )
        .await?
        .map(|(_, obj)| obj),
    );

    let get_both_stream_vec: Vec<Option<MyTestStructure>> = get_both_stream.collect().await;

    assert_eq!(vec![find_it_again, Some(my_struct2)], get_both_stream_vec);

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

    let objects_as_vec: Vec<MyTestStructure> = object_stream.collect().await;

    assert_eq!(objects_as_vec, vec![object_updated]);

    Ok(())
}
