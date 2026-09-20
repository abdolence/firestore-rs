# Timestamps support

By default, date/time values serialize as a string to Firestore (while
deserialization works from Timestamps and Strings). To store them as native
Firestore timestamps there are three options.

- Using `std::time::SystemTime` directly, with no attribute and no wrapping type,
  since it is recognised automatically:

```rust
# use serde::{Deserialize, Serialize};
# use std::time::SystemTime;
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    created_at: SystemTime,
    updated_at: Option<SystemTime>,
}
```

  It works in the queries as well:

```rust,no_run
# use firestore::*;
# use std::time::SystemTime;
# #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
# struct MyTestStructure { created_at: SystemTime }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
# db.fluent().select().from(TEST_COLLECTION_NAME).filter(|q| { q.for_all([
q.field(path!(MyTestStructure::created_at))
    .less_than_or_equal(SystemTime::now())
# ]) }).obj::<MyTestStructure>().query().await?;
# Ok(())
# }
```

  Note that `SystemTime` cannot carry the instants before the Unix epoch, since
  serde itself refuses them.

- Using the type `FirestoreTimestamp`, which needs no attributes:

```rust
# use firestore::*;
# use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    created_at: FirestoreTimestamp,
    updated_at: Option<FirestoreTimestamp>,
}
```

It can be created with `FirestoreTimestamp::now()`, parsed from a string, and
converted from/to `std::time::SystemTime`:

```rust
# use firestore::*;
# use std::time::SystemTime;
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let now = FirestoreTimestamp::now();
let from_system_time: FirestoreTimestamp = SystemTime::now().try_into()?;
let back_to_system_time: SystemTime = now.into();
# let _ = (from_system_time, back_to_system_time);
# Ok(())
# }
```

Use it in your queries as well, for example:

```rust,no_run
# use firestore::*;
# #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
# struct MyTestStructure { created_at: FirestoreTimestamp }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
# db.fluent().select().from(TEST_COLLECTION_NAME).filter(|q| { q.for_all([
q.field(path!(MyTestStructure::created_at))
    .less_than_or_equal(FirestoreTimestamp::now())
# ]) }).obj::<MyTestStructure>().query().await?;
# Ok(())
# }
```

- Or, if you prefer to keep a plain instant in your model, use
  `FirestoreInstant` (an alias for `jiff::Timestamp`) with `#[serde(with)]`
  attributes:

```rust
# use firestore::*;
# use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: FirestoreInstant,

    #[serde(default)]
    #[serde(with = "firestore::serialize_as_optional_timestamp")]
    updated_at: Option<FirestoreInstant>,
}
```

Firestore stores the timestamps with microsecond precision and discards the
nanoseconds on write. `FirestoreTimestamp` truncates to that precision in all of
its constructors and conversions, so its values survive a round trip unchanged.
A `FirestoreInstant` or a `SystemTime` carrying nanoseconds does not, since those
are the standard types this library cannot change.

All of them change the representation only for Firestore serialization.
`FirestoreTimestamp` and `FirestoreInstant` still serialize as a string to JSON,
so the same model can be reused for JSON and Firestore, while a plain `SystemTime`
keeps the default serde representation.
