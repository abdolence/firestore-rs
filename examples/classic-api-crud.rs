use chrono::{DateTime, Utc};
use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
    one_more_string: String,
    some_num: u64,
    created_at: DateTime<Utc>,
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

    const TEST_COLLECTION_NAME: &'static str = "test";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "Test".to_string(),
        one_more_string: "Test2".to_string(),
        some_num: 41,
        created_at: Utc::now(),
    };

    // Remove if it already exist
    db.delete_by_id(TEST_COLLECTION_NAME, &my_struct.some_id)
        .await?;

    // Let's insert some data
    db.create_obj(TEST_COLLECTION_NAME, Some(&my_struct.some_id), &my_struct)
        .await?;

    // Update some field in it
    let updated_obj = db
        .update_obj(
            TEST_COLLECTION_NAME,
            &my_struct.some_id,
            &MyTestStructure {
                some_num: my_struct.some_num + 1,
                some_string: "updated-value".to_string(),
                ..my_struct.clone()
            },
            Some(paths!(MyTestStructure::{
                some_num,
                some_string
            })),
        )
        .await?;

    println!("Updated object: {:?}", updated_obj);

    // Get object by id
    let find_it_again: MyTestStructure =
        db.get_obj(TEST_COLLECTION_NAME, &my_struct.some_id).await?;

    println!("Should be the same: {:?}", find_it_again);

    // Query our data
    let objects: Vec<MyTestStructure> = db
        .query_obj(
            FirestoreQueryParams::new(TEST_COLLECTION_NAME.into()).with_filter(
                FirestoreQueryFilter::Compare(Some(FirestoreQueryFilterCompare::Equal(
                    path!(MyTestStructure::some_num),
                    find_it_again.some_num.into(),
                ))),
            ),
        )
        .await?;

    println!("Now in the list: {:?}", objects);

    Ok(())
}
