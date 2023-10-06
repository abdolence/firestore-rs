use crate::FirestoreDocument;
use std::collections::HashMap;

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
