use firestore::*;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyParentStructure {
    some_id: String,
    some_string: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyChildStructure {
    some_id: String,
    another_string: String,
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

    const TEST_PARENT_COLLECTION_NAME: &str = "nested-test";
    const TEST_CHILD_COLLECTION_NAME: &str = "test-childs";

    println!("Populating parent doc/collection");

    for parent_idx in 0..5 {
        let parent_struct = MyParentStructure {
            some_id: format!("test-parent-{}", parent_idx),
            some_string: "Test".to_string(),
        };

        // Remove if it already exist
        db.fluent()
            .delete()
            .from(TEST_PARENT_COLLECTION_NAME)
            .document_id(&parent_struct.some_id)
            .execute()
            .await?;

        db.fluent()
            .insert()
            .into(TEST_PARENT_COLLECTION_NAME)
            .document_id(&parent_struct.some_id)
            .object(&parent_struct)
            .execute::<()>()
            .await?;

        for child_idx in 0..3 {
            // Creating a child doc
            let child_struct = MyChildStructure {
                some_id: format!("test-parent{}-child-{}", parent_idx, child_idx),
                another_string: "TestChild".to_string(),
            };

            // The doc path where we store our childs
            let parent_path =
                db.parent_path(TEST_PARENT_COLLECTION_NAME, &parent_struct.some_id)?;

            // Remove child doc if exists
            db.fluent()
                .delete()
                .from(TEST_CHILD_COLLECTION_NAME)
                .parent(&parent_path)
                .document_id(&child_struct.some_id)
                .execute()
                .await?;

            db.fluent()
                .insert()
                .into(TEST_CHILD_COLLECTION_NAME)
                .document_id(&child_struct.some_id)
                .parent(&parent_path)
                .object(&child_struct)
                .execute::<()>()
                .await?;
        }
    }

    println!("Query children");

    let mut objs_stream: BoxStream<MyChildStructure> = db
        .fluent()
        .select()
        .from(TEST_CHILD_COLLECTION_NAME)
        //.parent(db.parent_path(TEST_PARENT_COLLECTION_NAME, "test-parent-0")) // if you need to search for only one root you need do disable with_all_descendants below
        .all_descendants()
        .filter(|q| {
            q.for_all([q
                .field(path!(MyChildStructure::another_string))
                .eq("TestChild")])
        })
        .obj()
        .stream_query()
        .await?;

    while let Some(object) = objs_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
