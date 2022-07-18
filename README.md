[![Cargo](https://img.shields.io/crates/v/firestore.svg)](https://crates.io/crates/firestore)
![tests and formatting](https://github.com/abdolence/firestore-rs/workflows/tests%20&amp;%20formatting/badge.svg)
![security audit](https://github.com/abdolence/firestore-rs/workflows/security%20audit/badge.svg)

# Firestore for Rust

Library provides a simple API for Google Firestore:
- Create or update documents using Rust structures and Serde; 
- Support for querying / streaming / listening documents from Firestore;
- Full async based on Tokio runtime;
- Macro that helps you use JSON paths as references to your structure fields;
- Caching Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs) 
  that automatically detects tokens or GKE environment;

## Quick start


Cargo.toml:
```toml
[dependencies]
firestore = "0.2"
```

Example code:
```rust

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    const TEST_COLLECTION_NAME: &'static str = "test";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "Test".to_string(),
        some_num: 42,
    };

    // Remove if it already exist
    db.delete_by_id(
        TEST_COLLECTION_NAME,
        &my_struct.some_id,
    ).await?;

    // Let's insert some data
    db.create_obj(
        TEST_COLLECTION_NAME,
        &my_struct.some_id,
        &my_struct,
    ).await?;

    // Update some field in it
    let updated_obj = db.update_obj(
        TEST_COLLECTION_NAME,
        &my_struct.some_id,
        &MyTestStructure {
            some_num: my_struct.some_num + 1,
            some_string: "updated-value".to_string(),
            ..my_struct.clone()
        },
        Some(
            paths!(MyTestStructure::{
                some_num,
                some_string
            })
        ),
    ).await?;

    println!("Updated object: {:?}", updated_obj);

    // Get object by id
    let find_it_again: MyTestStructure = db.get_obj(
        TEST_COLLECTION_NAME,
        &my_struct.some_id,
    ).await?;

    println!("Should be the same: {:?}", find_it_again);

    // Query our data
    let objects: Vec<MyTestStructure> = db.query_obj(
        FirestoreQueryParams::new(
            TEST_COLLECTION_NAME.into()
        ).with_filter(
            FirestoreQueryFilter::Compare(Some(
                FirestoreQueryFilterCompare::Equal(
                    path!(MyTestStructure::some_num),
                    find_it_again.some_num.into(),
                ),
            ))
        )
    ).await?;

    println!("Now in the list: {:?}", objects);
```

All examples available at examples directory.

To run example use with environment variables:
```
# PROJECT_ID=<your-google-project-id> cargo run --example simple-crud
```

## Licence
Apache Software License (ASL)

## Author
Abdulla Abdurakhmanov
