use crate::FirestoreDocument;
use std::collections::HashMap;

/// Retrieves a field's value from a Firestore document using a dot-separated path.
///
/// This function allows accessing nested fields within a document's map values.
/// For example, given a document with a field `user` which is a map containing
/// a field `name`, you can retrieve the value of `name` using the path `"user.name"`.
///
/// Backticks (`) in field paths are automatically removed, as they are sometimes
/// used by the `struct_path` macro for escaping.
///
/// # Arguments
/// * `doc`: A reference to the [`FirestoreDocument`] to extract the field from.
/// * `field_path`: A dot-separated string representing the path to the desired field.
///
/// # Returns
/// Returns `Some(&gcloud_sdk::google::firestore::v1::value::ValueType)` if the field
/// is found at the specified path, otherwise `None`. The `ValueType` enum holds the
/// actual typed value (e.g., `StringValue`, `IntegerValue`).
///
/// # Examples
/// ```rust
/// use firestore::{firestore_doc_get_field_by_path, FirestoreDocument, FirestoreValue};
/// use gcloud_sdk::google::firestore::v1::MapValue;
/// use std::collections::HashMap;
///
/// let mut fields = HashMap::new();
/// let mut user_map_fields = HashMap::new();
/// user_map_fields.insert("name".to_string(), gcloud_sdk::google::firestore::v1::Value {
///     value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::StringValue("Alice".to_string())),
/// });
/// fields.insert("user".to_string(), gcloud_sdk::google::firestore::v1::Value {
///     value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::MapValue(MapValue { fields: user_map_fields })),
/// });
///
/// let doc = FirestoreDocument {
///     name: "projects/p/databases/d/documents/c/doc1".to_string(),
///     fields,
///     create_time: None,
///     update_time: None,
/// };
///
/// let name_value_type = firestore_doc_get_field_by_path(&doc, "user.name");
/// assert!(name_value_type.is_some());
/// if let Some(gcloud_sdk::google::firestore::v1::value::ValueType::StringValue(name)) = name_value_type {
///     assert_eq!(name, "Alice");
/// } else {
///     panic!("Expected StringValue");
/// }
///
/// let non_existent_value = firestore_doc_get_field_by_path(&doc, "user.age");
/// assert!(non_existent_value.is_none());
/// ```
pub fn firestore_doc_get_field_by_path<'d>(
    doc: &'d FirestoreDocument,
    field_path: &str,
) -> Option<&'d gcloud_sdk::google::firestore::v1::value::ValueType> {
    let field_path: Vec<String> = field_path
        .split('.')
        .map(|s| s.to_string().replace('`', ""))
        .collect();
    firestore_doc_get_field_by_path_arr(&doc.fields, &field_path)
}

/// Internal helper function to recursively navigate the document fields.
fn firestore_doc_get_field_by_path_arr<'d>(
    fields: &'d HashMap<String, gcloud_sdk::google::firestore::v1::Value>,
    field_path_arr: &[String],
) -> Option<&'d gcloud_sdk::google::firestore::v1::value::ValueType> {
    field_path_arr.first().and_then(|field_name| {
        fields.get(field_name).and_then(|field_value| {
            if field_path_arr.len() == 1 {
                field_value.value_type.as_ref()
            } else {
                match field_value.value_type {
                    Some(gcloud_sdk::google::firestore::v1::value::ValueType::MapValue(
                        ref map_value,
                    )) => {
                        firestore_doc_get_field_by_path_arr(&map_value.fields, &field_path_arr[1..])
                    }
                    _ => None,
                }
            }
        })
    })
}
