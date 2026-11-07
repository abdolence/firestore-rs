# Explicit null value serialisation

By default, every `Option` serialises as an absent field, which is convenient for many cases.
Some cases need explicit nulls instead.

Null and absent are not the same to a query. A Core filter or `order_by` never returns a document
that lacks the field, and `is_null()` matches only a stored null. A pipeline or SQL statement tells
them apart too: `not_equal` and `sort` keep a document that lacks the field, and SQL `IS NULL`
matches a stored null only. [Query and write semantics](./editions/semantics.md#null-and-absent)
lists every case.

To help with that there are additional attributes implemented for `serde(with)`:

* For any type:

```rust
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#[serde(default)]
#[serde(with = "firestore::serialize_as_null")]
test_null: Option<String>,
# }
```

* For Firestore timestamps attribute:

```rust
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#[serde(default)]
#[serde(with = "firestore::serialize_as_null_timestamp")]
test_null: Option<FirestoreInstant>,
# }
```
