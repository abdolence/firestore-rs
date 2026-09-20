# Document and collection IDs

`FirestoreDocumentId` and `FirestoreCollectionId` are validated newtypes for IDs that arrive from
outside your process - a request path segment, a JSON body, a runtime-chosen collection name.
Construct one where the ID enters your system; a `/`, an empty string, or `.`/`..` is rejected
there instead of silently reaching Firestore or, in the case of a collection name, retargeting the
operation at a different collection.

```rust
# use firestore::*;
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let id = FirestoreDocumentId::new("user-42")?;
let collection = FirestoreCollectionId::new("users")?;
# let _ = (id, collection);
# Ok(())
# }
```

A name known up front - the sort of thing you would today write as `const NAME: &str = "..."` -
can be declared as a validated `const` or `static` with `from_static`. An invalid literal there is
a compile error, not a runtime one:

```rust
# use firestore::*;
const USERS: FirestoreCollectionId = FirestoreCollectionId::from_static("users");
```

Use `from_static` for a literal you control, checked at compile time with no `Result` to handle;
use `new` for a value arriving at runtime, which returns a `FirestoreResult`.

Both implement `AsRef<str>`, so a reference drops straight into any call that already takes a
document or collection ID or name, with no conversion:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String }
# async fn example(
#     db: FirestoreDb,
#     collection: FirestoreCollectionId,
#     id: FirestoreDocumentId,
# ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
  .select()
  .by_id_in(&collection)
  .obj::<MyTestStructure>()
  .one(&id)
  .await?;
# Ok(())
# }
```

Store the validated type in your own structs instead of a bare `String` to carry the proof along
with the value; it deserializes through the same validation, so a `CreateSession` built from an
untrusted request body rejects a bad ID before it reaches Firestore:

```rust
# use firestore::*;
# use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize)]
struct CreateSession {
    user_id: FirestoreDocumentId,
}
```

See `examples/crud.rs` and `examples/nested_collections.rs` for a document ID and a
`parent_path`/`at` pair built from validated IDs, and `examples/dynamic_doc_level_crud.rs` /
`examples/caching_dynamic_collections.rs` for a collection name chosen at runtime.
