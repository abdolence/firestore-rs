use crate::errors::*;
use crate::FirestoreValue;

use crate::timestamp_utils::from_timestamp;
use chrono::prelude::*;
use rsb_derive::Builder;

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreWriteResult {
    pub update_time: Option<DateTime<Utc>>,
    pub transform_results: Vec<FirestoreValue>,
}

impl TryInto<FirestoreWriteResult> for gcloud_sdk::google::firestore::v1::WriteResult {
    type Error = FirestoreError;

    fn try_into(self) -> Result<FirestoreWriteResult, Self::Error> {
        Ok(FirestoreWriteResult::new(
            self.transform_results
                .into_iter()
                .map(|r| FirestoreValue::from(r))
                .collect(),
        )
        .opt_update_time(self.update_time.map(from_timestamp).transpose()?))
    }
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreFieldTransform {
    pub field: String,
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

#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreFieldTransformType {
    SetToServerValue(FirestoreTransformServerValue),
    Increment(FirestoreValue),
    Maximum(FirestoreValue),
    Minimum(FirestoreValue),
    AppendMissingElements(Vec<FirestoreValue>),
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

#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreTransformServerValue {
    Unspecified,
    RequestTime,
}
