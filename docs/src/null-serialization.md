# Explicit null value serialization

By default, all Option<> serialized as absent fields, which is convenient for many cases.
However sometimes you need to have explicit nulls.

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
