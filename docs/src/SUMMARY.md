# Table of contents

[Firestore for Rust](./intro.md)

# Getting started

- [Quick start](./getting-started.md)
- [Client instance and lifecycle](./client.md)

# Firestore editions

- [Standard and Enterprise editions](./editions.md)
    - [Query and write semantics](./editions/semantics.md)
    - [Query plans and cost](./editions/query-plans.md)
    - [Choosing Core queries, pipelines or SQL](./editions/choosing-an-api.md)

# Core API

- [Fluent API](./fluent-api.md)
- [Get and batch get](./get-and-batch-get.md)
- [Querying](./querying.md)
- [Nested collections](./nested-collections.md)
- [Aggregations](./aggregations.md)
- [Vector search](./vector-search.md)
- [Listening to document changes](./listening-changes.md)
- [Working on dynamic/document level](./dynamic-documents.md)

# Writes and transactions

- [Transactions](./transactions.md)
- [Batch writes](./batch-writes.md)
- [Document transformations](./document-transformations.md)
- [Update/delete preconditions](./preconditions.md)

# Pipelines and SQL (Enterprise edition)

- [Pipelines](./pipelines.md)
    - [Pipeline aggregations and joins](./pipeline-aggregations-and-joins.md)
    - [Pipeline search](./pipeline-search.md)
    - [Pipeline writes](./pipeline-writes.md)
- [SQL](./sql.md)
    - [Reads, joins and subqueries](./sql/reads.md)
    - [Writes and ON CONFLICT](./sql/writes.md)
    - [Index statements](./sql/indexes.md)
    - [Fluent code for a statement](./sql/codegen.md)

# Data modelling

- [Timestamps support](./timestamps.md)
- [Document metadata as struct fields](./document-metadata.md)
- [Explicit null value serialisation](./null-serialization.md)
- [Document and collection IDs](./document-collection-ids.md)
- [JSON conversion](./json.md)

# Caching

- [Caching](./caching.md)
    - [Usage](./caching/usage.md)
    - [Caching named documents instead of a whole collection](./caching/named-documents.md)
    - [Changing the cached collections at runtime](./caching/dynamic-collections.md)
    - [Choosing a cache mode](./caching/modes.md)
    - [Load modes, and why listings need preloading](./caching/load-modes.md)
    - [How the cache is updated](./caching/updates.md)

# Administration

- [Index management](./index-management.md)
    - [Composite and vector indexes](./index-management/composite.md)
    - [Search indexes (Enterprise edition)](./index-management/search.md)
    - [TTL and the Enterprise edition](./index-management/ttl-and-enterprise.md)
- [Bulk delete](./bulk-delete.md)
- [Command line tool](./cli.md)

# Environment and operations

- [Google authentication](./auth.md)
- [Firestore emulator](./emulator.md)
- [Docker images and TLS](./docker.md)
- [Request tags](./request-tags.md)
- [Request timeouts](./request-timeouts.md)
- [Testing your own code](./testing.md)
