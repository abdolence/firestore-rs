# Document metadata as struct fields

Firestore provides additional fields for each document:

- `_firestore_id`: the document ID, whether the client chose it or Firestore generated it;
- `_firestore_full_id`: the full document name, such as
  `projects/my-project/databases/(default)/documents/my-collection/my-id`;
- `_firestore_created`: the time at which the document was created;
- `_firestore_updated`: the time at which the document was last changed.

A stored document carries its name in Core and pipeline results. A Core aggregation result, and
the result of a pipeline stage that builds new documents, such as `.fields()` or `.aggregate()`,
has no document name, and so neither `_firestore_id` nor `_firestore_full_id`. A SQL row has no
name unless the statement selects `__name__`. Make a field that reads either of them an `Option`,
see [Query and write semantics](./editions/semantics.md#results-without-a-document-name).

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
