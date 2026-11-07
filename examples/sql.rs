use firestore::errors::FirestoreError;
use firestore::*;
use serde::Deserialize;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// The example creates this collection and deletes what it wrote before it ends.
const SCRATCH: &str = "sql-example-scratch";

#[derive(Debug, Clone, Deserialize)]
struct Item {
    sku: String,
    stock: i64,
    // A field that a document lacks is absent from the row, so it needs an `Option`.
    status: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = FirestoreDb::with_options(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?)
            .with_database_id(config_env_var("DATABASE_ID")?),
    )
    .await?;

    db.fluent()
        .sql(format!("DELETE FROM `{SCRATCH}` WHERE TRUE"))
        .execute()
        .await?;

    let inserted = db
        .fluent()
        .sql(format!(
            "INSERT INTO `{SCRATCH}` (sku, stock) VALUES ('a-1', 3), ('a-2', 0), ('a-3', 0)"
        ))
        .execute()
        .await?;
    println!("Inserted {} documents", inserted.documents_modified);

    // A parameterised read. The value is bound as a typed constant and never enters the text.
    let empty: Vec<Item> = db
        .fluent()
        .sql(format!(
            "SELECT sku, stock, status FROM `{SCRATCH}` WHERE stock <= @max ORDER BY sku"
        ))
        .param("max", 0)
        .obj()
        .query()
        .await?;
    for item in &empty {
        println!("Out of stock: {}, {} left", item.sku, item.stock);
    }

    // Text from outside the program runs read-only, on the collection it is meant to read, and
    // returns a bounded number of rows.
    let outside_options = FirestoreSqlOptions::read_only()
        .with_collections([FirestoreCollectionId::from_static(SCRATCH)])
        .with_max_rows(2);

    let rows = db
        .fluent()
        .sql(format!("SELECT sku, stock FROM `{SCRATCH}` ORDER BY sku"))
        .options(outside_options.clone())
        .query()
        .await?;
    println!("The cap returned {} of 3 rows", rows.len());

    for refused in [
        format!("DELETE FROM `{SCRATCH}` WHERE TRUE"),
        "SELECT * FROM `pipeline-books`".to_string(),
    ] {
        match db
            .fluent()
            .sql(&refused)
            .options(outside_options.clone())
            .query()
            .await
        {
            Err(FirestoreError::InvalidParametersError(error)) => {
                println!("Refused {refused:?}: {}", error.public.error)
            }
            other => return Err(format!("{refused:?} must be refused, got {other:?}").into()),
        }
    }

    // A write. It runs atomically: it applies in full or not at all.
    let updated = db
        .fluent()
        .sql(format!(
            "UPDATE `{SCRATCH}` SET status = @status WHERE stock = 0"
        ))
        .param("status", "sold_out")
        .execute()
        .await?;
    println!("Updated {} documents", updated.documents_modified);

    let sold_out: Vec<Item> = db
        .fluent()
        .sql(format!(
            "SELECT sku, stock, status FROM `{SCRATCH}` WHERE status = ? ORDER BY sku"
        ))
        .params(["sold_out"])
        .obj()
        .query()
        .await?;
    for item in &sold_out {
        println!("Status of {}: {:?}", item.sku, item.status);
    }

    let deleted = db
        .fluent()
        .sql(format!("DELETE FROM `{SCRATCH}` WHERE TRUE"))
        .execute()
        .await?;
    println!("Deleted {} documents", deleted.documents_modified);

    Ok(())
}
