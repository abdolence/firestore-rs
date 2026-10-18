use crate::errors::*;
use crate::FirestoreInstant;
use crate::{FirestoreQueryParams, FirestoreRequestOptions, FirestoreResult};
use rsb_derive::*;
use rvstruct::ValueStruct;
use std::collections::HashMap;

#[derive(Debug, Clone, Builder)]
pub struct FirestoreListenerTargetParams {
    pub target: FirestoreListenerTarget,
    pub target_type: FirestoreTargetType,
    pub resume_type: Option<FirestoreListenerTargetResumeType>,
    pub add_target_once: Option<bool>,
    pub labels: HashMap<String, String>,

    /// Request options (e.g. request tags) for the listen requests of this target.
    pub request_options: Option<FirestoreRequestOptions>,
}

impl FirestoreListenerTargetParams {
    /// Checks that the target ID is valid.
    ///
    /// Returns an error if the target ID is zero or greater than `i32::MAX` - Firestore's wire
    /// protocol represents a target ID as a signed 32-bit integer. There is no need to call this
    /// before [`FirestoreListener::add_target`](crate::FirestoreListener::add_target), which
    /// checks the ID along with the rest of the target.
    pub fn validate(&self) -> FirestoreResult<()> {
        self.target.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Builder)]
pub struct FirestoreCollectionDocuments {
    pub parent: Option<String>,
    pub collection: String,
    pub documents: Vec<String>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum FirestoreTargetType {
    Query(FirestoreQueryParams),
    Documents(FirestoreCollectionDocuments),
}

#[derive(Debug, Clone)]
pub enum FirestoreListenerTargetResumeType {
    Token(FirestoreListenerToken),
    ReadTime(FirestoreInstant),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreListenerTarget(u32);

impl FirestoreListenerTarget {
    /// Checks the target ID against Firestore's wire representation.
    ///
    /// Returns an error if the ID is zero, or greater than `i32::MAX` - Firestore's `Target`
    /// message carries it as a signed 32-bit integer, so a larger value cannot be sent.
    pub fn validate(&self) -> FirestoreResult<()> {
        if *self.value() == 0 {
            Err(FirestoreError::invalid_parameters(
                "target_id",
                "Listener target ID cannot be zero",
            ))
        } else if *self.value() > i32::MAX as u32 {
            Err(FirestoreError::invalid_parameters(
                "target_id",
                format!(
                    "Listener target ID cannot be more than: {}. {} is specified",
                    i32::MAX,
                    self.value()
                ),
            ))
        } else {
            Ok(())
        }
    }
}

impl TryInto<i32> for FirestoreListenerTarget {
    type Error = FirestoreError;

    fn try_into(self) -> FirestoreResult<i32> {
        self.validate()?;
        (*self.value()).try_into().map_err(|e| {
            FirestoreError::invalid_parameters(
                "target_id",
                format!("Invalid target ID: {} {}", self.value(), e),
            )
        })
    }
}

impl TryFrom<i32> for FirestoreListenerTarget {
    type Error = FirestoreError;

    fn try_from(value: i32) -> FirestoreResult<Self> {
        value
            .try_into()
            .map_err(|e| {
                FirestoreError::invalid_parameters(
                    "target_id",
                    format!("Invalid target ID: {value} {e}"),
                )
            })
            .map(FirestoreListenerTarget)
    }
}

#[derive(Clone, Debug, ValueStruct)]
pub struct FirestoreListenerToken(Vec<u8>);
