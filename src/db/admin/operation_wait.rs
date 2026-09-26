//! The one piece of long-running Firestore admin operation polling shared by index sync and bulk
//! delete: fetching one operation's current state. What a caller does with the result differs
//! enough - index sync only needs its done/error state, bulk delete also decodes progress
//! metadata on every poll - that only the RPC call itself is shared here, not the poll loop
//! around it.

use crate::errors::FirestoreError;
use crate::{FirestoreDb, FirestoreResult};
use gcloud_sdk::google::longrunning::{GetOperationRequest, Operation};

impl FirestoreDb {
    pub(crate) async fn get_operation(&self, name: &str) -> FirestoreResult<Operation> {
        self.operations_client()
            .get_operation(GetOperationRequest {
                name: name.to_string(),
            })
            .await
            .map_err(FirestoreError::from)
            .map(|response| response.into_inner())
    }
}
