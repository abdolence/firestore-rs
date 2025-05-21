use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_ref: FirestoreReference,
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

    const TEST_COLLECTION_NAME: &str = "test-reference";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_ref: db.parent_path("test-latlng", "test-1")?.into(),
    };

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .execute()
        .await?;

    // A fluent version of create document/object
    let object_returned: MyTestStructure = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&my_struct)
        .execute()
        .await?;

    println!("Created: {:?}", object_returned);

    // Query our data
    let objects1: Vec<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .obj()
        .query()
        .await?;

    println!("Now in the list: {:?}", objects1);

    let (parent_path, collection_name, document_id) = objects1
        .first()
        .unwrap()
        .some_ref
        .split(db.get_documents_path());

    println!("Document ID: {}", document_id);
    println!("Collection name: {:?}", collection_name);
    println!("Parent Path: {:?}", parent_path);

    // Read by reference
    let object_returned: Option<FirestoreDocument> = db
        .fluent()
        .select()
        .by_id_in(&collection_name)
        .one(document_id)
        .await?;

    println!("Object by reference: {:?}", object_returned);

    Ok(())
}
