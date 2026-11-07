# Firestore for Rust

Library provides a simple API for Google Firestore based on the official gRPC API:

- Fluent high-level and strongly typed API, the only public API of this library;
- Create or update documents using Rust structures and Serde;
- Support for:
    - Querying/streaming docs/objects;
    - Listing documents/objects (`stream_all` follows the page tokens for you);
    - Listening changes from Firestore, with targets added and removed on a running listener and an opt-in idle reset for silent streams;
    - Transactions;
    - Aggregated queries: count, sum, avg;
    - Streaming batch writes with automatic throttling to stay under the Firestore write rate limits;
    - K-nearest neighbor (KNN) vector search with Euclidean, Cosine and Dot Product measures;
    - Query cursors and partition queries;
    - Collection group queries and listing collection IDs;
    - Server side document transforms: increment, array append/remove, server timestamp;
    - Update/delete preconditions;
    - Consistency selectors to read at a given time or inside a transaction;
    - Explaining queries;
    - Firestore Enterprise edition [pipelines](./pipelines.md): filters, projections, [aggregations, joins and subqueries](./pipeline-aggregations-and-joins.md), unions, nearest neighbours and [writes](./pipeline-writes.md);
    - Full-text and geospatial [search in pipelines](./pipeline-search.md);
    - [SQL statements](./sql.md) in the BigQuery dialect, translated into pipelines, behind an opt-in `sql` feature;
    - Request tags to attribute Firestore usage;
    - [Opt-in request timeouts](./request-timeouts.md);
- Full async based on Tokio runtime;
- Macros that help you use your structure fields as Firestore field paths;
- Implements own Serde serializer to Firestore protobuf values;
- Support for multiple database IDs;
- Support for extended datatypes:
    - Firestore timestamp as a `FirestoreTimestamp` type or with `#[serde(with)]` attributes (based on [jiff](https://github.com/BurntSushi/jiff));
    - Lat/Lng;
    - References;
    - Explicit nulls;
- Caching support for collections and documents:
    - In-memory cache;
    - Persistent cache;
- [Declarative index management](./index-management.md) behind an opt-in `admin` feature:
  composite indexes (including one-field, unique and sparse ones), vector indexes, text and geo
  search indexes, single-field overrides and TTL policy, planned and synced with one call,
  following the edition of the database;
- [Bulk delete](./bulk-delete.md) behind the same `admin` feature: removes every document in named
  collection groups, at any depth across the whole database, in the background;
- Works with the Firestore emulator through `FIRESTORE_EMULATOR_HOST`, no credentials needed;
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
  that automatically detects GCE environment or application default accounts for local development;
