use firestore::*;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
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

    const TEST_CHILD_COLLECTION_NAME: FirestoreCollectionId =
        FirestoreCollectionId::from_static("test-childs");

    println!("Creating a parent doc/collection");

    // Validated once here; parent_path below takes both by reference and checks them again for
    // free, so a bad collection name or ID cannot silently retarget the sub-collection path.
    let parent_collection_id = FirestoreCollectionId::new("nested-test")?;
    let parent_id = FirestoreDocumentId::new("test-parent")?;

    let parent_struct = MyParentStructure {
        some_id: parent_id.as_str().to_string(),
        some_string: "Test".to_string(),
    };

    // Remove if it already exist
    db.fluent()
        .delete()
        .from(&parent_collection_id)
        .document_id(&parent_id)
        .execute()
        .await?;

    // Creating a parent doc
    db.fluent()
        .insert()
        .into(&parent_collection_id)
        .document_id(&parent_id)
        .object(&parent_struct)
        .execute::<()>()
        .await?;

    // Creating a child doc
    let child_struct = MyChildStructure {
        some_id: "test-child".to_string(),
        another_string: "TestChild".to_string(),
    };

    // The doc path where we store our childs
    let parent_path = db.parent_path(&parent_collection_id, &parent_id)?;

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

    println!("Listing all children");
    let list_stream: BoxStream<MyChildStructure> = db
        .fluent()
        .list()
        .from(TEST_CHILD_COLLECTION_NAME)
        .parent(&parent_path)
        .obj()
        .stream_all()
        .await?;

    let as_vec: Vec<MyChildStructure> = list_stream.collect().await;
    println!("{as_vec:?}");

    println!("Querying in children");
    let query_stream: BoxStream<MyChildStructure> = db
        .fluent()
        .select()
        .from(TEST_CHILD_COLLECTION_NAME)
        .parent(&parent_path)
        .filter(|q| {
            q.for_all([q
                .field(path!(MyChildStructure::another_string))
                .eq("TestChild")])
        })
        .obj()
        .stream_query()
        .await?;

    let as_vec: Vec<MyChildStructure> = query_stream.collect().await;
    println!("{as_vec:?}");

    Ok(())
}
