use firestore::*;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,

    // The simplest option: the wrapping type serializes as a Firestore timestamp
    // without any attribute. For serde_json it is still a string, so the same
    // model can be reused for both JSON and Firestore.
    created_at: FirestoreTimestamp,
    updated_at: Option<FirestoreTimestamp>,
    updated_at_always_none: Option<FirestoreTimestamp>,

    // Or keep a plain instant in your model and use an attribute instead
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at_attr: FirestoreInstant,

    // And one more attribute for optionals
    #[serde(default)]
    #[serde(with = "firestore::serialize_as_optional_timestamp")]
    updated_at_attr: Option<FirestoreInstant>,

    #[serde(default)]
    #[serde(with = "firestore::serialize_as_optional_timestamp")]
    updated_at_attr_always_none: Option<FirestoreInstant>,
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

    const TEST_COLLECTION_NAME: &str = "test-ts1";

    // Timestamps can be created from the library types, or converted from
    // the standard `SystemTime`
    let now_from_system_time: FirestoreTimestamp = SystemTime::now().try_into()?;

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        created_at: FirestoreTimestamp::now(),
        updated_at: Some(now_from_system_time),
        updated_at_always_none: None,
        created_at_attr: FirestoreInstant::now(),
        updated_at_attr: Some(FirestoreInstant::now()),
        updated_at_attr_always_none: None,
    };

    // And converted back to a `SystemTime` when you need to hand them over to
    // other libraries
    let created_at_system_time: SystemTime = my_struct.created_at.into();
    println!("Created at, as a SystemTime: {created_at_system_time:?}");

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

    println!("Created: {object_returned:?}");

    // Query our data
    let objects1: Vec<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .filter(|q| {
            q.for_all([q
                .field(path!(MyTestStructure::created_at))
                .less_than_or_equal(
                    FirestoreTimestamp::now(), // The wrapping type works in queries as well
                )])
        })
        .obj()
        .query()
        .await?;

    println!("Now in the list: {objects1:?}");

    Ok(())
}
