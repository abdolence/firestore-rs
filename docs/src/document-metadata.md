# Reading Firestore document metadata as struct fields

Firestore provides additional generated fields for each of document you create:

- `_firestore_id`: Generated document ID (when it is not specified from the client);
- `_firestore_created`: The time at which the document was created;
- `_firestore_updated`: The time at which the document was last changed;

To be able to read them the library makes them available
as system fields for the Serde deserializer with reserved names,
so you can specify them in your structures as:

```rust
# use firestore::*;
# use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    #[serde(alias = "_firestore_created")]
    created_at: Option<FirestoreTimestamp>,
    #[serde(alias = "_firestore_updated")]
    updated_at: Option<FirestoreTimestamp>,
    some_string: String,
    one_more_string: String,
    some_num: u64,
}
```

Complete example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/generated-document-id.rs).
