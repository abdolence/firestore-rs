mod emit;
use emit::FirestoreSqlEmitter;

mod row;
use row::{FirestoreSqlRow, FirestoreSqlRustType};

use crate::errors::FirestoreError;
use crate::{
    FirestoreDocument, FirestorePipeline, FirestorePipelineSource, FirestoreResult,
    FirestoreSqlOptions, FirestoreSqlParameters, FirestoreSqlStatement,
};
use heck::{ToSnakeCase, ToUpperCamelCase};
use std::collections::BTreeSet;

// The documents path codegen translates against. The emitter writes paths relative to it, and
// the snippet reads its own `db`'s documents path at run time.
const CODEGEN_DOCUMENTS_PATH: &str = "projects/codegen/databases/(default)/documents";

impl FirestoreSqlStatement {
    /// The fluent pipeline code of `sql`, a `SELECT`, as a formatted Rust snippet: a row
    /// struct, and an async function that runs the pipeline and takes the statement's
    /// parameters as its arguments, with the SQL in a comment on top.
    ///
    /// `parameters` binds every parameter to a sample value, whose type is the argument's
    /// type. `sample` holds rows the statement returned, which type the fields of the row
    /// struct; with no rows, every field is an `Option<serde_json::Value>`, and a statement
    /// without a select list returns documents. Nothing is sent.
    ///
    /// Fails with [`InvalidParametersError`](FirestoreError::InvalidParametersError) for text
    /// the translator refuses, for a statement that is not a `SELECT`, for a `SELECT` that
    /// uses a `JOIN`, `UNION`, `UNNEST`, a subquery, `* EXCEPT`, `documents(..)`, `database()`
    /// or `subcollection(..)`, and for a parameter in a place where the translation reads its
    /// value, such as `LIMIT @n`, `__name__ = @id`, `x[@key]` or the argument of a search.
    pub fn fluent_rust(
        sql: &str,
        parameters: &FirestoreSqlParameters,
        sample: &[FirestoreDocument],
    ) -> FirestoreResult<String> {
        // The sample values come first: a placeholder is not the constant that the `VALUES` of
        // a write needs, so a write with parameters would fail as something other than a write.
        Self::codegen_pipeline(sql, parameters)?;
        let pipeline = Self::codegen_pipeline(sql, &parameters.placeholders())?;
        let arguments = FirestoreSqlArgument::all(parameters);

        let collection_id = pipeline.source_collection_id();
        let row_name = match collection_id.to_upper_camel_case() {
            name if name.starts_with(|first: char| first.is_ascii_digit()) => {
                format!("Collection{name}Row")
            }
            name => format!("{name}Row"),
        };
        let function_name = match collection_id.to_snake_case() {
            name if name.is_empty() => "query".to_string(),
            name => format!("query_{name}"),
        };
        let row = FirestoreSqlRow::new(&row_name, &pipeline, sample)?;
        let (rows_type, run) = match &row {
            Some(_) => (
                format!("Vec<{row_name}>"),
                format!(".obj::<{row_name}>().query().await"),
            ),
            None => (
                "Vec<FirestoreDocument>".to_string(),
                ".query().await".to_string(),
            ),
        };
        let body = FirestoreSqlEmitter::new(&arguments)
            .pipeline("db.fluent().pipeline().select()", &pipeline)?;
        let signature: String = arguments
            .iter()
            .map(|argument| {
                format!(
                    ", {}: {}",
                    argument.identifier,
                    argument.rust_type.argument_type()
                )
            })
            .collect();
        let function = format!(
            "async fn {function_name}(db: &FirestoreDb{signature}) -> FirestoreResult<{rows_type}> {{ {body}{run} }}"
        );
        let uses = match row {
            Some(_) => "use firestore::*;\nuse serde::Deserialize;",
            None => "use firestore::*;",
        };

        let mut snippet: String = sql
            .lines()
            .map(|line| format!("// {}\n", line.trim_end()))
            .collect();
        let items = [
            Some(uses.to_string()),
            row.map(|row| row.to_string()),
            Some(function),
        ];
        for (index, item) in items.into_iter().flatten().enumerate() {
            if index > 0 {
                snippet.push('\n');
            }
            snippet.push_str(&formatted(&item)?);
        }
        Ok(snippet)
    }

    fn codegen_pipeline(
        sql: &str,
        parameters: &FirestoreSqlParameters,
    ) -> FirestoreResult<FirestorePipeline> {
        // The options guard text that runs, and codegen runs nothing. A caller's options could
        // also add a `LIMIT`, from `with_max_rows`, that the snippet must not show.
        let options = FirestoreSqlOptions::new()
            .allow_subqueries()
            .allow_index_statements();
        match Self::translate_on(CODEGEN_DOCUMENTS_PATH, sql, parameters, &options)? {
            Self::Query(pipeline) => Ok(pipeline),
            _ => Err(FirestoreError::invalid_sql(
                "statement",
                "codegen shows the fluent code of a SELECT; run any other statement with the SQL builder",
                None,
            )),
        }
    }
}

impl FirestorePipeline {
    fn source_collection_id(&self) -> &str {
        match &self.source {
            FirestorePipelineSource::Collection(source) => &source.collection_id,
            FirestorePipelineSource::CollectionGroup(source) => &source.collection_id,
            _ => "",
        }
    }
}

// A parameter of the statement, as an argument of the snippet's function.
struct FirestoreSqlArgument {
    // What the pipeline holds in the parameter's place: `@name`, or `?1`, `?2`...
    placeholder: String,
    identifier: String,
    rust_type: FirestoreSqlRustType,
}

impl FirestoreSqlArgument {
    fn all(parameters: &FirestoreSqlParameters) -> Vec<Self> {
        // `db` is the function's own argument, and the others are the closure parameters that
        // the emitted code binds, which an argument of the same name would shadow.
        let mut identifiers = FirestoreSqlIdentifiers::reserving(&["db", "e", "q", "s"]);
        parameters
            .samples()
            .map(|(placeholder, sample)| {
                let name = match placeholder.strip_prefix('?') {
                    Some(position) => format!("param_{position}"),
                    None => placeholder.trim_start_matches('@').to_string(),
                };
                Self {
                    identifier: identifiers.name(&name),
                    // A null sample says nothing of the type, and JSON holds any value.
                    rust_type: sample
                        .constant_value()
                        .and_then(FirestoreSqlRustType::of)
                        .unwrap_or(FirestoreSqlRustType::Json),
                    placeholder,
                }
            })
            .collect()
    }
}

// `rust`, one item group of the snippet, as prettyplease prints it. The parse also proves that
// the emitter wrote valid Rust.
fn formatted(rust: &str) -> FirestoreResult<String> {
    syn::parse_file(rust)
        .map(|file| prettyplease::unparse(&file))
        .map_err(|error| {
            FirestoreError::system_error(format!(
                "codegen wrote Rust that does not parse: {error}: {rust}"
            ))
        })
}

// The identifiers given out in one scope, the row struct's fields or the function's arguments,
// so that no two names share one.
#[derive(Debug, Default)]
struct FirestoreSqlIdentifiers(BTreeSet<String>);

impl FirestoreSqlIdentifiers {
    fn reserving(identifiers: &[&str]) -> Self {
        Self(identifiers.iter().map(|name| name.to_string()).collect())
    }

    // The snake_case identifier for `name`, a column or a parameter. A name that starts with a
    // digit gets `field_` before it, a keyword is written raw, and a name that an earlier one
    // took gets `_2`, `_3`...
    fn name(&mut self, name: &str) -> String {
        let base = match name.to_snake_case() {
            base if base.is_empty() => "field".to_string(),
            base if base.starts_with(|first: char| first.is_ascii_digit()) => {
                format!("field_{base}")
            }
            base => base,
        };
        let mut candidate = escaped(base.clone());
        let mut count = 1;
        while !self.0.insert(candidate.clone()) {
            count += 1;
            candidate = escaped(format!("{base}_{count}"));
        }
        candidate
    }
}

// `identifier` as Rust accepts it: a keyword is a raw identifier, Rust's `r#` form, except the
// path keywords, which cannot be raw. `gen` is reserved from edition 2024 on but not by syn,
// which parses the 2021 grammar, so it is written raw to build in both.
fn escaped(identifier: String) -> String {
    match identifier.as_str() {
        "self" | "super" | "crate" => format!("{identifier}_"),
        "gen" => format!("r#{identifier}"),
        _ if syn::parse_str::<syn::Ident>(&identifier).is_err() => format!("r#{identifier}"),
        _ => identifier,
    }
}
