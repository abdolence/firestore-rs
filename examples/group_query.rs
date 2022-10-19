use firestore::*;
use futures_util::stream::BoxStream;
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

    const TEST_PARENT_COLLECTION_NAME: &'static str = "nested-test";
    const TEST_CHILD_COLLECTION_NAME: &'static str = "test-childs";

    println!("Populating parent doc/collection");

    for parent_idx in 0..5 {
        let parent_struct = MyParentStructure {
            some_id: format!("test-parent-{}", parent_idx),
            some_string: "Test".to_string(),
        };

        // Remove if it already exist
        db.delete_by_id(TEST_PARENT_COLLECTION_NAME, &parent_struct.some_id)
            .await?;

        db.create_obj(
            TEST_PARENT_COLLECTION_NAME,
            &parent_struct.some_id,
            &parent_struct,
        )
        .await?;

        for child_idx in 0..3 {
            // Creating a child doc
            let child_struct = MyChildStructure {
                some_id: format!("test-parent{}-child-{}", parent_idx, child_idx),
                another_string: "TestChild".to_string(),
            };

            // The doc path where we store our childs
            let parent_path = format!(
                "{}/{}/{}",
                db.get_documents_path(),
                TEST_PARENT_COLLECTION_NAME,
                parent_struct.some_id
            );

            // Remove child doc if exists
            db.delete_by_id_at(
                parent_path.as_str(),
                TEST_CHILD_COLLECTION_NAME,
                &child_struct.some_id,
            )
            .await?;

            // Create a child doc
            db.create_obj_at(
                parent_path.as_str(),
                TEST_CHILD_COLLECTION_NAME,
                &child_struct.some_id,
                &child_struct,
            )
            .await?;
        }
    }

    println!("Query children");

    let mut objs_stream: BoxStream<MyChildStructure> = db
        .stream_query_obj(
            FirestoreQueryParams::new(TEST_CHILD_COLLECTION_NAME.into())
                // .with_parent(format!(
                //     "{}/{}/{}",
                //     db.get_documents_path(),
                //     TEST_PARENT_COLLECTION_NAME,
                //     "test-parent-0"
                // )) // if you need to search for only one root you need do disable with_all_descendants below
                .with_all_descendants(true)
                .with_filter(FirestoreQueryFilter::Compare(Some(
                    FirestoreQueryFilterCompare::Equal(
                        path!(MyChildStructure::another_string),
                        "TestChild".into(),
                    ),
                ))),
        )
        .await?;

    while let Some(object) = objs_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
