# Testing your own code

The Fluent API is built on `FirestoreDb` directly, so there is no built-in way to substitute a
fake database into `db.fluent()`. There are two approaches that work well.

## Abstract at your own boundary

Define a trait for what your code needs and implement it over a type holding a `FirestoreDb`.
Your business logic then depends on your trait, and tests provide their own implementation. This
keeps Firestore concerns in one place and needs nothing special from this library:

```rust,no_run
# use firestore::*;
# use firestore::errors::FirestoreError;
# #[derive(Debug, serde::Deserialize)]
# struct User;
# #[derive(Debug)]
# struct MyError;
# impl From<FirestoreError> for MyError {
#     fn from(_: FirestoreError) -> Self {
#         MyError
#     }
# }
#[async_trait::async_trait]
trait UserRepository {
    async fn find_user(&self, id: &str) -> Result<Option<User>, MyError>;
}

struct FirestoreUserRepository {
    db: FirestoreDb,
}

#[async_trait::async_trait]
impl UserRepository for FirestoreUserRepository {
    async fn find_user(&self, id: &str) -> Result<Option<User>, MyError> {
        Ok(self
            .db
            .fluent()
            .select()
            .by_id_in("users")
            .obj()
            .one(id)
            .await?)
    }
}

// In tests, implement `UserRepository` with an in-memory HashMap.
```

## Run against the Firestore emulator

For tests that should exercise real query, listener and transaction behaviour, point the library
at the [Firestore emulator](https://firebase.google.com/docs/emulator-suite):

```bash
export FIRESTORE_EMULATOR_HOST="localhost:8080"
```

No credentials are needed in that mode. This is how the caching and transaction behaviour of this
library itself is verified, and it catches things a hand-written fake cannot.

> **Changed in 0.52**: the low level `*Support` traits are no longer public, so code written to be
> generic over them no longer compiles. Note that they could never be used with the Fluent API
> from outside the crate anyway, since the builders' constructors are crate private. See the
> [migration guide](https://github.com/abdolence/firestore-rs/blob/master/MIGRATION.md).
