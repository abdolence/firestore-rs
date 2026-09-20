# Request tags

Firestore supports attaching request tags to requests. They are reported by Firestore
in its monitoring and billing breakdowns, which makes them useful to attribute reads
and writes to a specific feature, tenant or background job.

Tags can be set per operation for queries, aggregations, listings and listeners:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .request_tags(["nightly-report"])
    // or use request_options if you want to provide the options structure directly
    // .request_options(FirestoreRequestOptions::from_tags(["nightly-report"]))
    .obj::<MyTestStructure>()
    .query()
    .await?;
# Ok(())
# }
```

Or session wide, for every request issued through a client instance. This is also how
you attach tags to the CRUD operations (insert/update/delete/get):

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb, my_struct: MyTestStructure) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let tagged_db = db.clone_with_request_tags(["nightly-report"]);

tagged_db
    .fluent()
    .insert()
    .into(TEST_COLLECTION_NAME)
    .document_id(&my_struct.some_id)
    .object(&my_struct)
    .execute::<MyTestStructure>()
    .await?;
# Ok(())
# }
```

Transactions and batch writers accept them through their options:

```rust,no_run
# use firestore::*;
# use futures::FutureExt;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.run_transaction_with_options(
    |db, _transaction| {
        async move {
            db.fluent()
                .select()
                .by_id_in(TEST_COLLECTION_NAME)
                .obj::<MyTestStructure>()
                .one("test-1")
                .await?;

            Ok(())
        }
        .boxed()
    },
    FirestoreTransactionOptions::new()
        .with_request_options(FirestoreRequestOptions::from_tags(["checkout"])),
)
.await?;
# Ok(())
# }
```

A per operation value replaces the session wide default rather than merging with it.
