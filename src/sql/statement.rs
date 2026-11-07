use super::parameters::FirestoreSqlBinding;
use super::translator::FirestoreSqlTranslator;
use crate::errors::FirestoreError;
use crate::{
    FirestoreDb, FirestorePipeline, FirestorePipelineWriteParams, FirestoreResult,
    FirestoreSqlOptions, FirestoreSqlParameters,
};
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

/// How deep sqlparser may nest while it parses. Its own default of 50 overflows a 2 MiB stack in
/// a debug build: each level of a nested `NOT` or function call takes about 90 KB there.
const SQL_PARSER_RECURSION_LIMIT: usize = 16;

/// The longest statement accepted, in bytes. sqlparser nests a chain of infix operators one level
/// per operator, and its parsed statement is dropped recursively: a chain of 8,192 operators
/// drops well within a 2 MiB stack.
const SQL_MAX_LENGTH: usize = 16 * 1024;

/// A SQL statement translated into the pipeline model.
#[derive(Debug, Clone, PartialEq)]
pub enum FirestoreSqlStatement {
    /// A `SELECT`, as the pipeline that reads its rows.
    Query(FirestorePipeline),
    /// An `INSERT`, `UPDATE` or `DELETE`, as the pipeline write that runs it atomically.
    Write(Box<FirestorePipelineWriteParams>),
}

impl FirestoreSqlStatement {
    /// Translates `sql`, one statement in the BigQuery (GoogleSQL) dialect, into the pipeline
    /// model for `db`, with `parameters` bound and `options` checked. Nothing is sent.
    ///
    /// A query's pipeline runs through
    /// [`from_pipeline`](crate::pipeline_builder::FirestorePipelineInitialBuilder::from_pipeline).
    /// A write runs with [`execute`](crate::sql_builder::FirestoreSqlBuilder::execute) of the SQL
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
        let tokens = Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .map_err(FirestoreError::from_sql_tokenizer)?;
        let binding = FirestoreSqlBinding::new(&tokens, parameters)?;
        let statements = Parser::new(&dialect)
            .with_recursion_limit(SQL_PARSER_RECURSION_LIMIT)
            .with_tokens_with_locations(tokens)
            .parse_statements()
            .map_err(FirestoreError::from_sql_parser)?;
        match <[_; 1]>::try_from(statements) {
            Ok([statement]) => {
                FirestoreSqlTranslator::new(documents_path, options, binding).statement(&statement)
            }
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
