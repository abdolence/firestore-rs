[![Cargo](https://img.shields.io/crates/v/firestore.svg)](https://crates.io/crates/firestore)
![tests and formatting](https://github.com/abdolence/firestore-rs/workflows/tests%20&%20formatting/badge.svg)
![security audit](https://github.com/abdolence/firestore-rs/workflows/security%20audit/badge.svg)

# Firestore for Rust

Library provides a simple API for Google Firestore based on the official gRPC API.

## Documentation

Please follow to the official website: <https://firestore-rust.abdolence.dev>.

Upgrading from an older version? See the [migration guide](MIGRATION.md).

## Quick start

Cargo.toml:

```toml
[dependencies]
firestore = "0.54"
```

```rust
use firestore::*;

let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

let object_returned: MyTestStructure = db.fluent()
  .insert()
  .into("test")
  .document_id(&my_struct.some_id)
  .object(&my_struct)
  .execute()
  .await?;
```

If you see `no process-level CryptoProvider available`, see
[Crypto provider error](https://firestore-rust.abdolence.dev/getting-started.html#crypto-provider-error).

## Examples

All examples available in the [examples](examples) directory.

To run an example with environment variables:

```
PROJECT_ID=<your-google-project-id> cargo run --example crud
```

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
