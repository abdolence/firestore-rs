use firestore::*;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_num: i32,
    some_string: String,
    some_array: Vec<i32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    const TEST_COLLECTION_NAME: &'static str = "test-transforms";

    println!("Populating a test collection");
    for i in 0..10 {
        let my_struct = MyTestStructure {
            some_id: format!("test-{}", i),
            some_num: i,
            some_string: "Test".to_string(),
            some_array: vec![1, 2, 3],
        };

        // Remove if it already exist
        db.fluent()
            .delete()
            .from(TEST_COLLECTION_NAME)
            .document_id(&my_struct.some_id)
            .execute()
            .await?;

        // Let's insert some data
        db.fluent()
            .insert()
            .into(TEST_COLLECTION_NAME)
            .document_id(&my_struct.some_id)
            .object(&my_struct)
            .execute()
            .await?;
    }

    println!("Transaction with transformations");

    let mut transaction = db.begin_transaction().await?;

    // Only transforms
    db.fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .document_id("test-4")
        .transforms(|t| {
            t.fields([
                t.field(path!(MyTestStructure::some_num)).increment(10),
                t.field(path!(MyTestStructure::some_array))
                    .append_missing_elements([4, 5]),
                t.field(path!(MyTestStructure::some_array))
                    .remove_all_from_array([3]),
            ])
        })
        .only_transform()
        .add_to_transaction(&mut transaction)?;

    // Transforms with update
    db.fluent()
        .update()
        .fields(paths!(MyTestStructure::{
            some_string
        }))
        .in_col(TEST_COLLECTION_NAME)
        .document_id("test-5")
        .object(&MyTestStructure {
            some_id: format!("test-5"),
            some_num: 0,
            some_string: "UpdatedTest".to_string(),
            some_array: vec![1, 2, 3],
        })
        .transforms(|t| {
            t.fields([
                t.field(path!(MyTestStructure::some_num)).increment(10),
                t.field(path!(MyTestStructure::some_array))
                    .append_missing_elements([4, 5]),
                t.field(path!(MyTestStructure::some_array))
                    .remove_all_from_array([3]),
            ])
        })
        .add_to_transaction(&mut transaction)?;

    transaction.commit().await?;

    println!("Listing objects");
    // Query as a stream our data
    let mut objs_stream: BoxStream<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .order_by([(
            path!(MyTestStructure::some_id),
            FirestoreQueryDirection::Descending,
        )])
        .obj()
        .stream_query()
        .await?;

    while let Some(object) = objs_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
