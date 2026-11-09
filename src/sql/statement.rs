use super::ddl::starts_ddl;
use super::parameters::FirestoreSqlBinding;
use super::sample::rename_reservoir;
use super::translator::FirestoreSqlTranslator;
use crate::errors::FirestoreError;
use crate::{
    FirestoreCollectionId, FirestoreDb, FirestoreDocument, FirestoreExplainOptions,
    FirestoreListCollectionIdsParams, FirestorePipeline, FirestorePipelineWriteParams,
    FirestorePipelineWriteResult, FirestoreResult, FirestoreSqlOptions, FirestoreSqlParameters,
    FirestoreWithPipelineMetadata,
};
#[cfg(feature = "admin")]
use crate::{
    FirestoreFieldUpdate, FirestoreIndexDefinition, FirestoreIndexDeleteTarget,
    FirestoreIndexListing, FirestoreListedCompositeIndex, FirestoreListedField,
};
use futures::stream::BoxStream;
use rsb_derive::Builder;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

// How deep sqlparser may nest while it parses. Its own default of 50 overflows a 2 MiB stack in
// a debug build: each level of a nested `NOT` or function call takes about 90 KB there.
const SQL_PARSER_RECURSION_LIMIT: usize = 16;

// The longest statement accepted, in bytes. sqlparser nests a chain of infix operators one level
// per operator, and its parsed statement is dropped recursively: a chain of 8,192 operators
// drops well within a 2 MiB stack.
const SQL_MAX_LENGTH: usize = 16 * 1024;

/// A SQL statement translated into the request that runs it.
#[derive(Debug, Clone, PartialEq)]
pub enum FirestoreSqlStatement {
    /// A `SELECT`, as the pipeline that reads its rows.
    Query(FirestorePipeline),
    /// An `INSERT`, `UPDATE` or `DELETE`, as the pipeline write that runs it atomically.
    Write(Box<FirestorePipelineWriteParams>),
    /// `EXPLAIN [ANALYZE]` and a `SELECT`, as the pipeline it runs with Query Explain.
    Explain(FirestoreSqlExplain),
    /// `SHOW COLLECTIONS [IN path]`, as the listing of the collection IDs under the documents
    /// root or under a document.
    ShowCollections(FirestoreListCollectionIdsParams),
    /// `SHOW INDEXES [FROM collection_id]`, as the listing of one collection group's indexes, or
    /// of every group's.
    #[cfg(feature = "admin")]
    ShowIndexes(Option<FirestoreCollectionId>),
    /// `CREATE [UNIQUE | SEARCH | VECTOR] INDEX`.
    #[cfg(feature = "admin")]
    CreateIndex(FirestoreSqlCreateIndex),
    /// `DROP INDEX`, by ID or by the definition `CREATE` took.
    #[cfg(feature = "admin")]
    DropIndex(FirestoreSqlDropIndex),
    /// `ALTER COLLECTION`, as one write of a field's single-field indexes, its TTL, or both.
    #[cfg(feature = "admin")]
    AlterCollection(FirestoreFieldUpdate),
}

/// `EXPLAIN [ANALYZE]` and the `SELECT` it explains.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSqlExplain {
    /// The pipeline of the `SELECT`.
    pub pipeline: FirestorePipeline,
    /// Query Explain's options: `analyze` is set for `EXPLAIN ANALYZE`.
    pub options: FirestoreExplainOptions,
}

/// A `CREATE [UNIQUE | SEARCH | VECTOR] INDEX` statement.
#[cfg(feature = "admin")]
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSqlCreateIndex {
    /// The index the statement creates.
    pub definition: FirestoreIndexDefinition,
    /// Whether the statement says `IF NOT EXISTS`, so that an index that already exists is the
    /// statement's outcome rather than its error.
    #[default = "false"]
    pub if_not_exists: bool,
}

/// A `DROP [UNIQUE | SEARCH | VECTOR] INDEX` statement.
#[cfg(feature = "admin")]
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSqlDropIndex {
    /// The index the statement deletes, by ID or by definition.
    pub target: FirestoreIndexDeleteTarget,
    /// Whether the statement says `IF EXISTS`, so that no such index is the statement's outcome
    /// rather than its error.
    #[default = "false"]
    pub if_exists: bool,
}

/// What [`run`](crate::sql_builder::FirestoreSqlBuilder::run) returns: one variant per kind of
/// statement.
pub enum FirestoreSqlOutcome<'b> {
    /// The rows of a `SELECT` or an `EXPLAIN [ANALYZE] SELECT`, each with the metadata of its
    /// response. Query Explain's output is in the metadata of the last item.
    Rows(BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<FirestoreDocument>>>),
    /// What an `INSERT`, `UPDATE` or `DELETE` wrote.
    Written(FirestorePipelineWriteResult),
    /// The collection IDs `SHOW COLLECTIONS` lists, in the order Firestore lists them.
    Collections(Vec<FirestoreCollectionId>),
    /// The indexes and field overrides `SHOW INDEXES` lists.
    #[cfg(feature = "admin")]
    Indexes(FirestoreIndexListing),
    /// The index `CREATE .. INDEX` created, as Firestore lists it.
    #[cfg(feature = "admin")]
    IndexCreated(FirestoreListedCompositeIndex),
    /// The index `DROP INDEX` deleted, as it was listed just before, or `None` when the
    /// statement says `IF EXISTS` and no such index exists.
    #[cfg(feature = "admin")]
    IndexDropped(Option<FirestoreListedCompositeIndex>),
    /// The field `ALTER COLLECTION` wrote, as Firestore reads it after the write.
    #[cfg(feature = "admin")]
    FieldUpdated(FirestoreListedField),
}

impl FirestoreSqlStatement {
    /// Translates `sql`, one statement in the BigQuery (GoogleSQL) dialect, into the request that
    /// runs it for `db`, with `parameters` bound and `options` checked. Nothing is sent.
    ///
    /// A query's pipeline runs through
    /// [`from_pipeline`](crate::pipeline_builder::FirestorePipelineInitialBuilder::from_pipeline).
    /// Every statement runs with [`run`](crate::sql_builder::FirestoreSqlBuilder::run) of the SQL
    /// builder, which translates the statement itself.
    ///
    /// Fails with [`InvalidParametersError`](FirestoreError::InvalidParametersError) for text that
    /// does not parse, a construct without a pipeline form, a parameter bound or used on one
    /// side only, or a statement the options refuse. Its `field` names the construct and its
    /// `location` says where the construct starts, when that is known.
    pub fn translate(
        db: &FirestoreDb,
        sql: &str,
        parameters: &FirestoreSqlParameters,
        options: &FirestoreSqlOptions,
    ) -> FirestoreResult<Self> {
        Self::translate_on(db.get_documents_path(), sql, parameters, options)
    }

    pub(crate) fn translate_on(
        documents_path: &str,
        sql: &str,
        parameters: &FirestoreSqlParameters,
        options: &FirestoreSqlOptions,
    ) -> FirestoreResult<Self> {
        if sql.len() > SQL_MAX_LENGTH {
            return Err(FirestoreError::invalid_sql(
                "max_length",
                format!(
                    "the statement is {} bytes, longer than the {SQL_MAX_LENGTH} allowed",
                    sql.len()
                ),
                None,
            ));
        }
        let dialect = BigQueryDialect {};
        let mut tokens = Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .map_err(FirestoreError::from_sql_tokenizer)?;
        rename_reservoir(&mut tokens);
        let binding = FirestoreSqlBinding::new(&tokens, parameters)?;
        let mut translator = FirestoreSqlTranslator::new(documents_path, options, binding);
        let mut parser = Parser::new(&dialect)
            .with_recursion_limit(SQL_PARSER_RECURSION_LIMIT)
            .with_tokens_with_locations(tokens);
        if starts_ddl(&parser) {
            return translator.ddl_statement(&mut parser);
        }
        let statements = parser
            .parse_statements()
            .map_err(FirestoreError::from_sql_parser)?;
        match <[_; 1]>::try_from(statements) {
            Ok([statement]) => translator.statement(&statement),
            Err(statements) => Err(FirestoreError::invalid_sql(
                "sql",
                format!(
                    "the text holds {} statements; it must hold exactly one",
                    statements.len()
                ),
                None,
            )),
        }
    }
}
