use crate::common::setup_enterprise;
use serde::Serialize;
use serde_json::{json, Value as Json};

mod common;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Serialize)]
struct Row {
    id: i64,
    #[serde(with = "firestore::serialize_as_null")]
    tag: Option<&'static str>,
}

#[derive(Serialize)]
struct OptionalRow {
    id: i64,
    tag: Option<&'static str>,
    json_null: Json,
}

#[tokio::test]
async fn a_none_constant_compares_with_null() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Json> = db
        .fluent()
        .pipeline()
        .select()
        .literals([
            Row {
                id: 1,
                tag: Some("a"),
            },
            Row { id: 2, tag: None },
        ])
        .filter(|e| e.field("tag").eq(None::<String>))
        .fields(|e| [e.field("id")])
        .obj()
        .query()
        .await?;

    assert_eq!(rows, vec![json!({"id": 2})]);
    Ok(())
}

#[tokio::test]
async fn a_literals_row_leaves_out_none_and_json_null_fields() -> TestResult {
    let db = setup_enterprise().await?;

    let rows: Vec<Json> = db
        .fluent()
        .pipeline()
        .select()
        .literals([OptionalRow {
            id: 1,
            tag: None,
            json_null: Json::Null,
        }])
        .fields(|e| {
            [
                e.field("id").alias("id"),
                e.field("tag").is_absent().alias("tag_is_absent"),
                e.field("json_null")
                    .is_absent()
                    .alias("json_null_is_absent"),
            ]
        })
        .obj()
        .query()
        .await?;

    assert_eq!(
        rows,
        vec![json!({"id": 1, "tag_is_absent": true, "json_null_is_absent": true})]
    );
    Ok(())
}
