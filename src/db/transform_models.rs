use crate::errors::*;
use crate::FirestoreValue;

use crate::timestamp_utils::from_timestamp;
use chrono::prelude::*;
use rsb_derive::Builder;

/// The result of a Firestore write operation (create, update, delete).
///
/// This struct provides information about the outcome of a write, such as the
/// document's update time and the results of any field transformations that were applied.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreWriteResult {
    /// The time at which the document was last updated after this write operation.
    ///
    /// This is `None` if the write operation did not result in a changed document
    /// (e.g., deleting a non-existent document, or an update that didn't change any values).
    pub update_time: Option<DateTime<Utc>>,
    /// A list of values that are the result of applying field transformations.
    ///
    /// Each [`FirestoreValue`] in this list corresponds to a
    /// [`FirestoreFieldTransform`] applied in the write request.
    /// The order of results matches the order of transformations in the request.
    /// For example, if an `Increment` transform was used, this would contain the new
    /// value of the incremented field.
    pub transform_results: Vec<FirestoreValue>,
}

impl TryInto<FirestoreWriteResult> for gcloud_sdk::google::firestore::v1::WriteResult {
    type Error = FirestoreError;

    fn try_into(self) -> Result<FirestoreWriteResult, Self::Error> {
        Ok(FirestoreWriteResult::new(
            self.transform_results
                .into_iter()
                .map(FirestoreValue::from)
                .collect(),
        )
        .opt_update_time(self.update_time.map(from_timestamp).transpose()?))
    }
}

/// Represents a transformation to apply to a specific field within a document.
///
/// Field transformations allow for atomic, server-side modifications of document fields,
/// such as incrementing a number, setting a field to the server's timestamp, or
/// manipulating array elements.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreFieldTransform {
    /// The dot-separated path to the field to transform (e.g., "user.profile.age").
    pub field: String,
    /// The type of transformation to apply.
    pub transform_type: FirestoreFieldTransformType,
}

impl TryInto<gcloud_sdk::google::firestore::v1::document_transform::FieldTransform>
    for FirestoreFieldTransform
{
    type Error = FirestoreError;

    fn try_into(
        self,
    ) -> Result<gcloud_sdk::google::firestore::v1::document_transform::FieldTransform, Self::Error>
    {
        Ok(
            gcloud_sdk::google::firestore::v1::document_transform::FieldTransform {
                field_path: self.field,
                transform_type: Some(self.transform_type.try_into()?),
            },
        )
    }
}

/// Defines the specific type of transformation to apply to a field.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreFieldTransformType {
    /// Sets the field to a server-generated value, such as the request timestamp.
    SetToServerValue(FirestoreTransformServerValue),
    /// Atomically increments the field's numeric value by the given `FirestoreValue` (which must be an integer or double).
    Increment(FirestoreValue),
    /// Atomically sets the field to the maximum of its current value and the given `FirestoreValue`.
    Maximum(FirestoreValue),
    /// Atomically sets the field to the minimum of its current value and the given `FirestoreValue`.
    Minimum(FirestoreValue),
    /// Appends the given elements to an array field, but only if they are not already present.
    /// The `Vec<FirestoreValue>` contains the elements to append.
    AppendMissingElements(Vec<FirestoreValue>),
    /// Removes all instances of the given elements from an array field.
    /// The `Vec<FirestoreValue>` contains the elements to remove.
    RemoveAllFromArray(Vec<FirestoreValue>),
}

impl TryInto<gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType>
    for FirestoreFieldTransformType
{
    type Error = FirestoreError;

    fn try_into(
        self,
    ) -> Result<
        gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType,
        Self::Error,
    > {
        Ok(match self {
            FirestoreFieldTransformType::SetToServerValue(FirestoreTransformServerValue::Unspecified) => {
                gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType::SetToServerValue(
                    gcloud_sdk::google::firestore::v1::document_transform::field_transform::ServerValue::Unspecified as i32
                )
            },
            FirestoreFieldTransformType::SetToServerValue(FirestoreTransformServerValue::RequestTime) => {
                gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType::SetToServerValue(
                    gcloud_sdk::google::firestore::v1::document_transform::field_transform::ServerValue::RequestTime as i32
                )
            },
            FirestoreFieldTransformType::Increment(value) => {
                gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType::Increment(
                    value.value
                )
            },
            FirestoreFieldTransformType::Maximum(value) => {
                gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType::Maximum(
                    value.value
                )
            },
            FirestoreFieldTransformType::Minimum(value) => {
                gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType::Minimum(
                    value.value
                )
            },
            FirestoreFieldTransformType::AppendMissingElements(value) => {
                gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType::AppendMissingElements(
                    gcloud_sdk::google::firestore::v1::ArrayValue {
                        values: value.into_iter().map( | s| s.value).collect()
                    }
                )
            },
            FirestoreFieldTransformType::RemoveAllFromArray(value) => {
                gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType::RemoveAllFromArray(
                    gcloud_sdk::google::firestore::v1::ArrayValue {
                        values: value.into_iter().map( | s| s.value).collect()
                    }
                )
            },
        })
    }
}

/// Specifies a server-generated value for a field transformation.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreTransformServerValue {
    /// The server value is unspecified. This typically should not be used directly.
    Unspecified,
    /// Sets the field to the timestamp of when the server processes the request.
    /// This is commonly used for `createdAt` or `updatedAt` fields.
    RequestTime,
}
