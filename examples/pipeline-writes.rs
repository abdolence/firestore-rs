use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// The example creates this collection and deletes what it wrote before it ends.
const SCRATCH: &str = "pipeline-example-scratch";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item {
    sku: String,
    stock: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

fn item(sku: &str, stock: i64) -> Item {
    Item {
        sku: sku.to_string(),
        stock,
        status: None,
    }
}

async fn delete_all(db: &FirestoreDb) -> FirestoreResult<FirestorePipelineWriteResult> {
    db.fluent()
        .pipeline()
        .delete()
        .from(SCRATCH)
        .execute()
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = FirestoreDb::with_options(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?)
            .with_database_id(config_env_var("DATABASE_ID")?),
    )
    .await?;

    delete_all(&db).await?;

    // Insert rows given in the request. Each document is named by its `sku`.
    let inserted = db
        .fluent()
        .pipeline()
        .insert()
        .into(SCRATCH)
        .document_id(|e| e.field(path!(Item::sku)))
        .literals([item("a-1", 3), item("a-2", 0), item("a-3", 0)])
        .execute()
        .await?;
    println!("Inserted {} documents", inserted.documents_modified);

    // Update the documents a filter keeps, in one transaction of their own.
    let updated = db
        .fluent()
        .pipeline()
        .update(|e| [e.constant("sold_out").alias(path!(Item::status))])
        .in_col(SCRATCH)
        .filter(|q| q.field(path!(Item::stock)).eq(0))
        .atomic()
        .execute()
        .await?;
    println!("Updated {} documents", updated.documents_modified);

    let sold_out: Vec<Item> = db
        .fluent()
        .pipeline()
        .select()
        .from(SCRATCH)
        .filter(|q| q.field(path!(Item::status)).eq("sold_out"))
        .sort(|e| [e.field(path!(Item::sku)).asc()])
        .obj()
        .query()
        .await?;
    println!("Sold out: {sold_out:?}");

    let deleted = delete_all(&db).await?;
    println!("Deleted {} documents", deleted.documents_modified);

    Ok(())
}
