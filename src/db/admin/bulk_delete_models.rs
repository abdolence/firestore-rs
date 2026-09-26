//! Declarative bulk-delete request and result model, for [`FirestoreBulkDeleteSupport`](crate::db::support::FirestoreBulkDeleteSupport).
//!
//! Nothing here performs I/O. [`FirestoreBulkDeleteParams`] is what a fluent
//! `db.fluent().delete().bulk()...` chain assembles, and what `FirestoreDb` passes on to
//! Firestore's `BulkDeleteDocuments` admin RPC.

use crate::errors::FirestoreError;
use crate::{
    FirestoreCollectionId, FirestoreInstant, FirestoreOperationWaitOptions, FirestoreResult,
};
use rsb_derive::Builder;
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

/// One collection group's document- or byte-deletion progress, as Firestore reports it in
/// `BulkDeleteDocumentsMetadata`.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct FirestoreBulkDeleteProgress {
    /// The amount of work Firestore estimated when the operation started.
    pub estimated_work: i64,
    /// The amount of work completed so far.
    pub completed_work: i64,
}

impl Display for FirestoreBulkDeleteProgress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.completed_work, self.estimated_work)
    }
}

/// A declared bulk delete: the collection groups to remove, at any depth across the whole
/// database, and whether to wait for Firestore to finish.
///
/// Build one from [`FirestoreBulkDeleteBuilder`](crate::delete_builder::FirestoreBulkDeleteBuilder)'s
/// `.collection_groups()`, never directly: an empty or duplicated list is rejected before a
/// request can be sent, and the builder is what runs that check.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreBulkDeleteParams {
    /// The collection groups to delete. Never empty: Firestore reads an empty list as "the whole
    /// database", which this API never sends.
    pub collection_groups: Vec<FirestoreCollectionId>,
    /// Whether, and how long, to wait for the operation to reach a terminal state. `None` returns
    /// as soon as the operation is requested.
    #[default = "None"]
    pub wait: Option<FirestoreOperationWaitOptions>,
}

/// The outcome of a bulk delete: what was requested, and - when [`FirestoreBulkDeleteParams::wait`]
/// was set - what Firestore reported once the operation reached a terminal state.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct FirestoreBulkDeleteResult {
    /// The server-assigned name of the long-running operation, for logging or a manual
    /// `GetOperation` lookup.
    pub operation_name: String,
    /// The collection groups this call requested deleted.
    pub collection_groups: Vec<FirestoreCollectionId>,
    /// The database version Firestore read to decide which documents to delete. `None` until a
    /// poll observes it, so always `None` when `wait` was not set.
    pub snapshot_time: Option<FirestoreInstant>,
    /// Documents deleted so far, decoded from the last polled `BulkDeleteDocumentsMetadata`.
    /// `None` when `wait` was not set.
    pub documents: Option<FirestoreBulkDeleteProgress>,
    /// Bytes deleted so far, decoded from the last polled `BulkDeleteDocumentsMetadata`. `None`
    /// when `wait` was not set.
    pub bytes: Option<FirestoreBulkDeleteProgress>,
    /// Time elapsed between sending the request and, when waiting, the operation reaching a
    /// terminal state; otherwise the time to request it.
    pub elapsed: Duration,
}

impl Display for FirestoreBulkDeleteResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let groups: Vec<&str> = self
            .collection_groups
            .iter()
            .map(FirestoreCollectionId::as_str)
            .collect();
        write!(
            f,
            "Bulk delete {} ({}), elapsed {:?}",
            groups.join(", "),
            self.operation_name,
            self.elapsed,
        )?;
        if let Some(documents) = &self.documents {
            write!(f, ", documents {documents}")?;
        }
        if let Some(bytes) = &self.bytes {
            write!(f, ", bytes {bytes}")?;
        }
        if let Some(snapshot_time) = &self.snapshot_time {
            write!(f, ", snapshot_time {snapshot_time}")?;
        }
        Ok(())
    }
}

/// Checks the structural rules a declared [`FirestoreBulkDeleteParams`] must satisfy, independent
/// of the server.
///
/// # Errors
/// Returns [`FirestoreError::InvalidParametersError`] if `collection_groups` is empty (Firestore
/// reads an empty list as "delete the whole database", which this API never sends) or names the
/// same group more than once.
pub(crate) fn validate_bulk_delete_params(
    params: &FirestoreBulkDeleteParams,
) -> FirestoreResult<()> {
    if params.collection_groups.is_empty() {
        return Err(FirestoreError::invalid_parameters(
            "collection_groups",
            "bulk delete needs at least one collection group; an empty list means the whole \
             database in Firestore's API, and this crate never sends that",
        ));
    }
    let mut seen = HashSet::new();
    for group in &params.collection_groups {
        if !seen.insert(group.as_str()) {
            return Err(FirestoreError::invalid_parameters(
                "collection_groups",
                format!(
                    "collection group {} is declared more than once",
                    group.as_str()
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(id: &str) -> FirestoreCollectionId {
        FirestoreCollectionId::new(id).unwrap()
    }

    #[test]
    fn empty_collection_groups_is_rejected() {
        let params = FirestoreBulkDeleteParams::new(vec![]);
        let err = validate_bulk_delete_params(&params).unwrap_err();
        assert!(err.to_string().contains("collection_groups"));
    }

    #[test]
    fn duplicate_collection_group_is_rejected() {
        let params = FirestoreBulkDeleteParams::new(vec![group("users"), group("users")]);
        let err = validate_bulk_delete_params(&params).unwrap_err();
        assert!(err.to_string().contains("more than once"));
    }

    #[test]
    fn distinct_collection_groups_are_accepted() {
        let params = FirestoreBulkDeleteParams::new(vec![group("users"), group("orders")]);
        assert!(validate_bulk_delete_params(&params).is_ok());
    }
}
