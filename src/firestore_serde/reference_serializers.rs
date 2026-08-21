use gcloud_sdk::google::firestore::v1::value;
use regex::regex;
use serde::{Deserialize, Serialize, Serializer};

use crate::{errors::*, FirestoreResult, FirestoreValue};

pub(crate) const FIRESTORE_REFERENCE_TYPE_TAG_TYPE: &str = "FirestoreReference";

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct FirestoreReference(pub String);

impl FirestoreReference {
    /// Creates a new reference
    pub fn new(reference: String) -> Self {
        Self(reference)
    }

    /// Returns the reference as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse the reference to extract commonly used accessors such as `id`, `path`, …
    pub fn parse<'a>(&'a self) -> FirestoreResult<FirestoreParsedReference<'a>> {
        self.try_into()
    }

    /// Splits the reference into parent path, collection name and document id
    /// Returns (parent_path, collection_name, document_id)
    #[deprecated(since = "0.53.0", note = "use `parse` instead")]
    pub fn split(&self, document_path: &str) -> (Option<String>, String, String) {
        let split_pos = self.0.rfind('/').map(|pos| pos + 1).unwrap_or(0);
        let (parent_raw_path, document_id) = if split_pos == 0 {
            ("", self.0.as_str())
        } else {
            (&self.0[0..split_pos - 1], &self.0[split_pos..])
        };

        let parent_path = parent_raw_path.replace(format!("{document_path}/").as_str(), "");

        let split_pos = parent_path.rfind('/').map(|pos| pos + 1).unwrap_or(0);
        if split_pos == 0 {
            (None, parent_path, document_id.to_string())
        } else {
            (
                Some(parent_path[..split_pos - 1].to_string()),
                parent_path[split_pos..].to_string(),
                document_id.to_string(),
            )
        }
    }
}

#[derive(Debug)]
pub struct FirestoreParsedReference<'a> {
    path: &'a str,
    parent: Option<&'a str>,
    id: &'a str,
}

impl<'a> FirestoreParsedReference<'a> {
    // Returns the path to the document starting from the root of the database
    pub fn path(&self) -> &'a str {
        self.path
    }

    // Returns the parent, or None when it's the root of database
    pub fn parent(&self) -> Option<&'a str> {
        self.parent
    }

    /// Returns the document id, ie the last element of the path
    pub fn id(&self) -> &'a str {
        self.id
    }
}

impl<'a> TryFrom<&'a FirestoreReference> for FirestoreParsedReference<'a> {
    type Error = FirestoreError;

    fn try_from(raw: &'a FirestoreReference) -> Result<Self, Self::Error> {
        let regex = regex!(r"^(projects/[^/]+/databases/[^/]+/documents).*/([^/]+)");
        let captures = regex
            .captures(&raw.0)
            .ok_or(FirestoreError::DeserializeError(
                FirestoreSerializationError::from_message("invalid absolute reference"),
            ))?;

        let database_offset = (captures.get(1).expect("capture database prefix").end()) + 1; // skip first slash
        let document_offset = captures.get(2).expect("capture collection prefix").start();

        let path = raw.0.split_at(database_offset).1;
        let id = path.split_at(document_offset - database_offset).1;
        let parent = if path == id {
            None
        } else {
            Some(
                path.split_at(
                    path.len() - id.len() - 1, // skip trailing slash
                )
                .0,
            )
        };

        Ok(Self { path, parent, id })
    }
}

pub mod serialize_as_reference {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(str: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_newtype_struct(
            crate::firestore_serde::FIRESTORE_REFERENCE_TYPE_TAG_TYPE,
            &str,
        )
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)
    }
}

pub fn serialize_reference_for_firestore<T: ?Sized + Serialize>(
    value: &T,
    none_as_null: bool,
) -> Result<FirestoreValue, FirestoreError> {
    struct ReferenceSerializer {
        none_as_null: bool,
    }

    impl Serializer for ReferenceSerializer {
        type Ok = FirestoreValue;
        type Error = FirestoreError;
        type SerializeSeq = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTuple = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTupleStruct = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTupleVariant = crate::firestore_serde::serializer::SerializeTupleVariant;
        type SerializeMap = crate::firestore_serde::serializer::SerializeMap;
        type SerializeStruct = crate::firestore_serde::serializer::SerializeMap;
        type SerializeStructVariant = crate::firestore_serde::serializer::SerializeStructVariant;

        fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
            Ok(FirestoreValue::from(
                gcloud_sdk::google::firestore::v1::Value {
                    value_type: Some(value::ValueType::ReferenceValue(v.to_string())),
                },
            ))
        }

        fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
            if self.none_as_null {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::NullValue(0)),
                    },
                ))
            } else {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value { value_type: None },
                ))
            }
        }

        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
            value.serialize(self)
        }

        fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
            Ok(FirestoreValue::from(
                gcloud_sdk::google::firestore::v1::Value { value_type: None },
            ))
        }

        fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
            self.serialize_unit()
        }

        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            variant: &'static str,
        ) -> Result<Self::Ok, Self::Error> {
            self.serialize_str(variant)
        }

        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<Self::Ok, Self::Error> {
            value.serialize(self)
        }

        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Reference serializer doesn't support this type",
                ),
            ))
        }
    }

    value.serialize(ReferenceSerializer { none_as_null })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reference_split() {
        let reference = FirestoreReference::new(
            concat!(
                "projects/test-project/databases/(default)/documents/",
                "test-collection/test-document-id/child-collection/child-document-id"
            )
            .to_string(),
        );
        #[allow(deprecated)]
        let (parent_path, collection_name, document_id) =
            reference.split("projects/test-project/databases/(default)/documents");

        assert_eq!(
            parent_path,
            Some("test-collection/test-document-id".to_string())
        );
        assert_eq!(collection_name, "child-collection");
        assert_eq!(document_id, "child-document-id");
    }

    #[test]
    fn test_reference_parsing() {
        let reference = FirestoreReference(
            concat!(
                "projects/test-project/databases/(default)/documents/",
                "test-collection/test-document-id/child-collection/child-document-id"
            )
            .to_string(),
        );
        let parsed = reference.parse().expect("valid ref");

        assert_eq!(
            parsed.path(),
            "test-collection/test-document-id/child-collection/child-document-id",
        );
        assert_eq!(
            parsed.parent(),
            Some("test-collection/test-document-id/child-collection"),
        );
        assert_eq!(parsed.id(), "child-document-id");
    }

    #[test]
    fn test_reference_parsing_root_document() {
        let reference = FirestoreReference(
            concat!(
                "projects/test-project/databases/(default)/documents/",
                "test-document-id"
            )
            .to_string(),
        );
        let parsed = reference.parse().expect("valid ref");

        assert_eq!(parsed.path(), "test-document-id");
        assert_eq!(parsed.parent(), None);
        assert_eq!(parsed.id(), "test-document-id");
    }
}
