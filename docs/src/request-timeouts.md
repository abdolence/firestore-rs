# Request timeouts

The library sets no timeout on requests by default. A request waits for Firestore for as long as its connection
lives, and a connection to a server that stopped answering is closed only by HTTP/2 keep-alive, after about two minutes.

To limit how long one request waits for Firestore, set `request_timeout` in the options:

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# fn config_env_var(name: &str) -> Result<String, String> {
#     std::env::var(name).map_err(|e| format!("{name}: {e}"))
# }
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let db = FirestoreDb::with_options(
    FirestoreDbOptions::new(config_env_var("PROJECT_ID")?)
        .with_request_timeout(Duration::from_secs(30)),
)
.await?;
# Ok(())
# }
```

What it limits depends on the request:
- unary requests (get, insert, update, delete, list pages, batch writes, transaction commits and the admin API calls):
  the whole call. The limit is also sent to Firestore as the request deadline;
- streamed reads (queries, aggregations and batch gets): the wait for the stream to start and for each next document,
  counted only while you are waiting for one, so a slow consumer of a long query is not cut;
- listeners and the streaming batch writer are not limited, since their streams stay quiet for as long as nothing changes.

The limit applies to each attempt. A request that runs out of it fails with a `DatabaseError` with the code
`DeadlineExceeded` and `retry_possible` set, and it is retried wherever a dropped connection is:
- gets, lists, list collection IDs and the start of queries and aggregations, up to `max_retries` times;
- simple batch writes, until `retry_max_elapsed_time`. A retried batch may apply twice, for example an `increment`,
  as it can after a dropped connection.

Batch gets, partition queries, a query or aggregation stream that has already started and transaction commits return
the error without a retry. A commit is never retried, because its writes may already be applied.

The reads in the first item also retry a `DEADLINE_EXCEEDED` that Firestore itself answers with, up to `max_retries`
times, with or without a request timeout set. Writes, batch gets, partition queries and streams that have already
started return it with `retry_possible` unset.

Be aware not to set `retry_max_elapsed_time` to `None` on a simple batch writer when you set a request timeout: against
a Firestore that stopped answering, it then retries forever, one timeout per attempt. The 60 second default is checked
between attempts, so the last attempt can still run for a whole timeout past it.

To use another limit for a part of your code, or none at all, clone the client with `clone_with_request_timeout`.
Batch writers and transactions created from the clone keep it:

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let report_db = db.clone_with_request_timeout(Some(Duration::from_secs(300)));
let unlimited_db = db.clone_with_request_timeout(None);
# Ok(())
# }
```
