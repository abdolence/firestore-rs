# Firestore for Rust

Library provides a simple API for Google Firestore based on the official gRPC API:

- Create or update documents using Rust structures and Serde;
- Support for:
    - Querying/streaming docs/objects;
    - Listing documents/objects (and auto pages scrolling support);
    - Listening changes from Firestore;
    - Transactions;
    - Aggregated Queries;
    - Streaming batch writes with automatic throttling to avoid time limits from Firestore;
    - K-nearest neighbor (KNN) vector search;
    - Explaining queries;
    - Request tags to attribute Firestore usage;
- Fluent high-level and strongly typed API;
- Full async based on Tokio runtime;
- Macro that helps you use JSON paths as references to your structure fields;
- Implements own Serde serializer to Firestore protobuf values;
- Support for multiple database IDs
- Supports for extended datatypes:
    - Firestore timestamp as a `FirestoreTimestamp` type or with `#[serde(with)]` attributes (based on [jiff](https://github.com/BurntSushi/jiff))
    - Lat/Lng
    - References
- Caching support for collections and documents:
    - In-memory cache;
    - Persistent cache;
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
  that automatically detects GCE environment or application default accounts for local development;
