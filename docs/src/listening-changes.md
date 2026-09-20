# Listening the document changes on Firestore

To help to work with asynchronous event listener the library supports high level API for
listening the events from Firestore on a separate thread:

The listener implementation needs to be provided with a storage for the last received token for specified targets to be
able to resume listening the changes from the last handled token and to avoid receiving all previous changes.

The library provides basic implementations for storing the tokens but you can implement your own more sophisticated
storage if needed:

- `FirestoreTempFilesListenStateStorage` - resume tokens stored as temporary files on local FS;
- `FirestoreMemListenStateStorage` - in memory storage backed by HashMap (with this implementation if you restart your
  app, you will receive all notifications again);

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String }
# const TEST_COLLECTION_NAME: &str = "test-listen";
# const TEST_TARGET_ID_BY_QUERY: FirestoreListenerTarget = FirestoreListenerTarget::new(42_u32);
# const TEST_TARGET_ID_BY_DOC_IDS: FirestoreListenerTarget = FirestoreListenerTarget::new(17_u32);
# async fn example(db: FirestoreDb, doc_id1: String, doc_id2: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut listener = db
    .create_listener(
        FirestoreTempFilesListenStateStorage::new(), // or FirestoreMemListenStateStorage or your own implementation
    )
    .await?;

// Adding query listener
db.fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .listen()
    .add_target(TEST_TARGET_ID_BY_QUERY, &mut listener)?;

// Adding docs listener by IDs
db.fluent()
    .select()
    .by_id_in(TEST_COLLECTION_NAME)
    .batch_listen([doc_id1, doc_id2])
    .add_target(TEST_TARGET_ID_BY_DOC_IDS, &mut listener)?;

listener
    .start(|event| async move {
        match event {
            FirestoreListenEvent::DocumentChange(ref doc_change) => {
                println!("Doc changed: {:?}", doc_change);

                if let Some(doc) = &doc_change.document {
                    let obj: MyTestStructure =
                        FirestoreDb::deserialize_doc_to::<MyTestStructure>(doc)
                            .expect("Deserialized object");
                    println!("As object: {:?}", obj);
                }
            }
            _ => {
                println!("Received a listen response event to handle: {:?}", event);
            }
        }

        Ok(())
    })
    .await?;

// Wait some events like Ctrl-C, signals, etc
// <put-your-implementation-for-wait-here>

// and then shutdown
listener.shutdown().await?;
# Ok(())
# }
```

See complete example in examples directory.
