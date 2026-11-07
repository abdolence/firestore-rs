//! The statements sqlparser does not read into a usable form: `SHOW`, and, with the `admin`
//! feature, `CREATE [UNIQUE | SEARCH | VECTOR] INDEX`, `DROP INDEX` and `ALTER COLLECTION`.
//! sqlparser parses neither `CREATE SEARCH INDEX`, `CREATE VECTOR INDEX`, `ALTER COLLECTION`, an
//! `OPTIONS (..)` after an index nor `collection_group('..')` as an index target, and it drops
//! every token of `SHOW INDEXES FROM 'orders'` that is not a word.
//!
//! These statements are read by keyword dispatch: the leading words are read here, and what they
//! hold goes to sqlparser's own sub-parsers.

use super::translator::{at, name_start, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{FirestoreListCollectionIdsParams, FirestoreResult, FirestoreSqlStatement};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithSpan};

#[cfg(feature = "admin")]
use super::query::string_literal;
#[cfg(feature = "admin")]
use super::translator::{expr_start, is_parameter};
#[cfg(feature = "admin")]
use crate::{
    FirestoreCollectionId, FirestoreFieldOverrideIndex, FirestoreFieldOverrideUpdate,
    FirestoreFieldTtlUpdate, FirestoreIndexFieldMode, FirestoreIndexQueryScope,
    FirestoreQueryDirection, FirestoreSqlCreateIndex, FirestoreSqlDropIndex,
};
#[cfg(feature = "admin")]
use sqlparser::ast::{Expr, SqlOption, Value as SqlValue};
#[cfg(feature = "admin")]
use sqlparser::keywords::Keyword;

const DDL_LEADING_WORDS: [&str; 4] = ["CREATE", "DROP", "ALTER", "SHOW"];

pub(super) fn starts_ddl(parser: &Parser) -> bool {
    DDL_LEADING_WORDS
        .iter()
        .any(|word| is_word(parser.peek_token_ref(), word))
}

fn is_word(token: &TokenWithSpan, word: &str) -> bool {
    matches!(&token.token, Token::Word(read)
        if read.quote_style.is_none() && read.value.eq_ignore_ascii_case(word))
}

pub(super) fn next_start(parser: &Parser) -> Option<FirestoreSqlLocation> {
    at(parser.peek_token_ref().span)
}

fn parse_word(parser: &mut Parser, word: &str) -> bool {
    let next = is_word(parser.peek_token_ref(), word);
    if next {
        parser.next_token();
    }
    next
}

/// Reads `words` when they are the next tokens, all of them or none.
#[cfg(feature = "admin")]
fn parse_words(parser: &mut Parser, words: &[&str]) -> bool {
    let next = words
        .iter()
        .enumerate()
        .all(|(position, word)| is_word(&parser.peek_nth_token(position), word));
    if next {
        for _ in words {
            parser.next_token();
        }
    }
    next
}

/// Reads the `;`s that may end the statement, and refuses anything after them.
fn end_of_statement(parser: &mut Parser) -> FirestoreResult<()> {
    let mut ended = false;
    while parser.consume_token(&Token::SemiColon) {
        ended = true;
    }
    let next = parser.peek_token();
    match next.token {
        Token::EOF => Ok(()),
        _ if ended => Err(FirestoreError::invalid_sql(
            "sql",
            "the text holds more than one statement; it must hold exactly one",
            at(next.span),
        )),
        token => Err(FirestoreError::invalid_sql(
            "sql",
            format!("expected the end of the statement, found {token}"),
            at(next.span),
        )),
    }
}

/// The word between `CREATE` or `DROP` and `INDEX`, when there is one.
#[cfg(feature = "admin")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FirestoreSqlIndexKind {
    Unique,
    Search,
    Vector,
}

#[cfg(feature = "admin")]
impl FirestoreSqlIndexKind {
    fn parse(parser: &mut Parser) -> Option<Self> {
        [
            ("UNIQUE", Self::Unique),
            ("SEARCH", Self::Search),
            ("VECTOR", Self::Vector),
        ]
        .into_iter()
        .find_map(|(word, kind)| parse_word(parser, word).then_some(kind))
    }
}

/// What an index statement names its index on: a collection ID, for an index of one collection,
/// or `collection_group('collection_id')`, for an index of every collection with the ID.
#[cfg(feature = "admin")]
pub(super) struct FirestoreSqlIndexTarget {
    pub(super) collection_group: FirestoreCollectionId,
    pub(super) query_scope: FirestoreIndexQueryScope,
}

/// One `key = value` of an `OPTIONS (..)` list.
#[cfg(feature = "admin")]
pub(super) struct FirestoreSqlOption {
    /// The key in lower case, since keys match without regard to case.
    pub(super) key: String,
    pub(super) value: Expr,
    pub(super) start: Option<FirestoreSqlLocation>,
}

/// The word that scopes an entry of `indexes` to collection-group queries.
#[cfg(feature = "admin")]
const COLLECTION_GROUP_WORD: &str = "COLLECTION_GROUP";

#[cfg(feature = "admin")]
impl FirestoreSqlOption {
    pub(super) fn ttl_update(&self) -> FirestoreResult<FirestoreFieldTtlUpdate> {
        match &self.value {
            Expr::Value(value) => match value.value {
                SqlValue::Boolean(true) => Some(FirestoreFieldTtlUpdate::Enable),
                SqlValue::Boolean(false) => Some(FirestoreFieldTtlUpdate::Disable),
                _ => None,
            },
            _ => None,
        }
        .ok_or_else(|| {
            FirestoreError::invalid_sql("ttl", "ttl is TRUE or FALSE", expr_start(&self.value))
        })
    }

    pub(super) fn override_update(&self) -> FirestoreResult<FirestoreFieldOverrideUpdate> {
        match &self.value {
            Expr::Value(value) if value.value == SqlValue::Null => {
                Ok(FirestoreFieldOverrideUpdate::Revert)
            }
            Expr::Array(array) => array
                .elem
                .iter()
                .map(|entry| self.override_index(entry))
                .collect::<FirestoreResult<_>>()
                .map(FirestoreFieldOverrideUpdate::Set),
            value => Err(FirestoreError::invalid_sql(
                "indexes",
                "indexes is a list of strings, or NULL",
                expr_start(value),
            )),
        }
    }

    /// One entry of `indexes`: `ASC`, `DESC` or `CONTAINS`, optionally after `COLLECTION_GROUP`.
    fn override_index(&self, entry: &Expr) -> FirestoreResult<FirestoreFieldOverrideIndex> {
        let text = option_string(&self.key, entry)?;
        let start = expr_start(entry);
        let words: Vec<&str> = text.split_whitespace().collect();
        let (query_scope, mode) = match words.as_slice() {
            [mode] => (FirestoreIndexQueryScope::Collection, *mode),
            [scope, mode] if scope.eq_ignore_ascii_case(COLLECTION_GROUP_WORD) => {
                (FirestoreIndexQueryScope::AllDescendants, *mode)
            }
            _ => return Err(Self::unknown_index_entry(&text, start)),
        };
        let mode = match mode.to_ascii_uppercase().as_str() {
            "ASC" => FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
            "DESC" => FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending),
            "CONTAINS" => FirestoreIndexFieldMode::ArrayContains,
            _ => return Err(Self::unknown_index_entry(&text, start)),
        };
        Ok(FirestoreFieldOverrideIndex { query_scope, mode })
    }

    fn unknown_index_entry(entry: &str, start: Option<FirestoreSqlLocation>) -> FirestoreError {
        FirestoreError::invalid_sql(
            "indexes",
            format!(
                "`{entry}` is not an index; an entry is ASC, DESC or CONTAINS, optionally after \
                 {COLLECTION_GROUP_WORD}"
            ),
            start,
        )
    }
}

/// The string literal of the option `key`'s `value`. A parameter is refused: an option is a
/// literal, never a parameter.
#[cfg(feature = "admin")]
pub(super) fn option_string(key: &str, value: &Expr) -> FirestoreResult<String> {
    let start = expr_start(value);
    let parameter = match value {
        Expr::Identifier(ident) if is_parameter(ident) => Some(ident.value.as_str()),
        Expr::Value(value) => match &value.value {
            SqlValue::Placeholder(placeholder) => Some(placeholder.as_str()),
            _ => None,
        },
        _ => None,
    };
    if let Some(parameter) = parameter {
        return Err(FirestoreError::invalid_sql(
            parameter,
            "an option is a literal, never a parameter",
            start,
        ));
    }
    string_literal(value).map(str::to_string).ok_or_else(|| {
        FirestoreError::invalid_sql(key, format!("{key} takes a string literal"), start)
    })
}

impl FirestoreSqlTranslator<'_> {
    /// Translates the statement `parser` reads, which [`starts_ddl`].
    pub(super) fn ddl_statement(
        &self,
        parser: &mut Parser,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        let leading = parser.next_token();
        let start = at(leading.span);
        let statement = if is_word(&leading, "SHOW") {
            self.show(parser, start)?
        } else if self.options.read_only {
            return Err(FirestoreError::invalid_sql(
                "read_only",
                "a statement that changes an index or a field is refused by the read-only options",
                start,
            ));
        } else if !self.options.index_statements {
            return Err(FirestoreError::invalid_sql(
                "allow_index_statements",
                "a statement that changes an index or a field runs only with \
                 FirestoreSqlOptions::allow_index_statements()",
                start,
            ));
        } else {
            self.index_statement(parser, &leading)?
        };
        end_of_statement(parser)?;
        Ok(statement)
    }

    fn show(
        &self,
        parser: &mut Parser,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        if parse_word(parser, "COLLECTIONS") {
            return self.show_collections(parser, start);
        }
        if parse_word(parser, "INDEXES") {
            return self.show_indexes(parser, start);
        }
        Err(FirestoreError::invalid_sql(
            "SHOW",
            "only SHOW INDEXES and SHOW COLLECTIONS are statements here",
            next_start(parser),
        ))
    }

    fn show_collections(
        &self,
        parser: &mut Parser,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        if self.options.collections.is_some() {
            return Err(FirestoreError::invalid_sql(
                "SHOW COLLECTIONS",
                "SHOW COLLECTIONS lists collection IDs outside the ones the options allow",
                start,
            ));
        }
        let parent = if parse_word(parser, "IN") {
            let name = parser
                .parse_object_name(false)
                .map_err(FirestoreError::from_sql_parser)?;
            let path = Self::collection_name(&name)?;
            let document = self.document_path(&path, name_start(&name))?;
            Some(format!("{}/{document}", self.documents_path))
        } else {
            None
        };
        Ok(FirestoreSqlStatement::ShowCollections(
            FirestoreListCollectionIdsParams::new().opt_parent(parent),
        ))
    }

    #[cfg(feature = "admin")]
    fn show_indexes(
        &self,
        parser: &mut Parser,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        let collection_group = if parse_word(parser, "FROM") {
            Some(self.collection_group_id(parser)?)
        } else if self.options.collections.is_some() {
            return Err(FirestoreError::invalid_sql(
                "SHOW INDEXES",
                "SHOW INDEXES without FROM lists every collection group, outside the ones the \
                 options allow",
                start,
            ));
        } else {
            None
        };
        Ok(FirestoreSqlStatement::ShowIndexes(collection_group))
    }

    #[cfg(not(feature = "admin"))]
    fn show_indexes(
        &self,
        _parser: &mut Parser,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        Err(FirestoreError::sql_needs_admin("SHOW INDEXES", start))
    }

    /// `CREATE [UNIQUE | SEARCH | VECTOR] INDEX [IF NOT EXISTS] ..`,
    /// `DROP [UNIQUE | SEARCH | VECTOR] INDEX [IF EXISTS] ..` and `ALTER COLLECTION ..`, after
    /// `leading`: the head is read here, and the rest by the statement's own translation.
    #[cfg(feature = "admin")]
    fn index_statement(
        &self,
        parser: &mut Parser,
        leading: &TokenWithSpan,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        if is_word(leading, "ALTER") {
            if !parse_word(parser, "COLLECTION") {
                return Err(FirestoreError::sql_not_a_statement(next_start(parser)));
            }
            return Ok(FirestoreSqlStatement::AlterCollection(
                self.field_update(parser)?,
            ));
        }
        let kind = FirestoreSqlIndexKind::parse(parser);
        if !parse_word(parser, "INDEX") {
            return Err(FirestoreError::sql_not_a_statement(next_start(parser)));
        }
        if is_word(leading, "CREATE") {
            let if_not_exists = parse_words(parser, &["IF", "NOT", "EXISTS"]);
            Ok(FirestoreSqlStatement::CreateIndex(
                FirestoreSqlCreateIndex::new(self.index_definition(parser, kind)?)
                    .with_if_not_exists(if_not_exists),
            ))
        } else {
            let if_exists = parse_words(parser, &["IF", "EXISTS"]);
            Ok(FirestoreSqlStatement::DropIndex(
                FirestoreSqlDropIndex::new(self.index_delete_target(parser, kind)?)
                    .with_if_exists(if_exists),
            ))
        }
    }

    #[cfg(not(feature = "admin"))]
    fn index_statement(
        &self,
        parser: &mut Parser,
        leading: &TokenWithSpan,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        let statement = if is_word(leading, "ALTER") {
            parse_word(parser, "COLLECTION").then(|| "ALTER COLLECTION".to_string())
        } else {
            ["UNIQUE", "SEARCH", "VECTOR"]
                .iter()
                .any(|word| parse_word(parser, word));
            parse_word(parser, "INDEX")
                .then(|| format!("{} INDEX", leading.token.to_string().to_uppercase()))
        };
        Err(match statement {
            Some(statement) => FirestoreError::sql_needs_admin(&statement, at(leading.span)),
            None => FirestoreError::sql_not_a_statement(next_start(parser)),
        })
    }

    /// The collection group ID `parser` reads next. A path is refused: an index and a field
    /// override apply to every collection with the ID, wherever it is.
    #[cfg(feature = "admin")]
    pub(super) fn collection_group_id(
        &self,
        parser: &mut Parser,
    ) -> FirestoreResult<FirestoreCollectionId> {
        let name = parser
            .parse_object_name(false)
            .map_err(FirestoreError::from_sql_parser)?;
        let start = name_start(&name);
        let collection_id = Self::collection_name(&name)?;
        if collection_id.contains('/') {
            return Err(FirestoreError::invalid_sql(
                "collection_group",
                format!(
                    "`{collection_id}` is a path; an index and a field override belong to a \
                     collection group, named by its ID"
                ),
                start,
            ));
        }
        self.check_collection(&collection_id, start)
    }

    /// The target `parser` reads next: a collection group ID, or
    /// `collection_group('collection_id')`, which the options can refuse.
    #[cfg(feature = "admin")]
    pub(super) fn index_target(
        &self,
        parser: &mut Parser,
    ) -> FirestoreResult<FirestoreSqlIndexTarget> {
        let start = next_start(parser);
        let group_function = is_word(parser.peek_token_ref(), "collection_group")
            && parser.peek_nth_token(1).token == Token::LParen;
        if !group_function {
            return Ok(FirestoreSqlIndexTarget {
                collection_group: self.collection_group_id(parser)?,
                query_scope: FirestoreIndexQueryScope::Collection,
            });
        }
        if !self.options.collection_groups {
            return Err(FirestoreError::invalid_sql(
                "collection_group",
                "the options refuse collection groups",
                start,
            ));
        }
        parser.next_token();
        let collection_id = (|| {
            parser.expect_token(&Token::LParen)?;
            let collection_id = parser.parse_literal_string()?;
            parser.expect_token(&Token::RParen)?;
            Ok(collection_id)
        })()
        .map_err(FirestoreError::from_sql_parser)?;
        Ok(FirestoreSqlIndexTarget {
            collection_group: self.check_collection(&collection_id, start)?,
            query_scope: FirestoreIndexQueryScope::AllDescendants,
        })
    }

    /// The `OPTIONS (key = value, ..)` list `parser` reads next, empty when there is none. A key
    /// given twice and an option not written `key = value` are refused.
    #[cfg(feature = "admin")]
    pub(super) fn statement_options(
        &self,
        parser: &mut Parser,
    ) -> FirestoreResult<Vec<FirestoreSqlOption>> {
        let start = next_start(parser);
        let options = parser
            .maybe_parse_options(Keyword::OPTIONS)
            .map_err(FirestoreError::from_sql_parser)?
            .unwrap_or_default();
        let mut read: Vec<FirestoreSqlOption> = Vec::with_capacity(options.len());
        for option in options {
            let SqlOption::KeyValue { key, value } = option else {
                return Err(FirestoreError::invalid_sql(
                    "OPTIONS",
                    "an option is written key = value",
                    start,
                ));
            };
            let name = key.value.to_lowercase();
            if read.iter().any(|option| option.key == name) {
                return Err(FirestoreError::invalid_sql(
                    &key.value,
                    format!("the option {name} is given twice"),
                    at(key.span),
                ));
            }
            read.push(FirestoreSqlOption {
                key: name,
                value,
                start: at(key.span),
            });
        }
        Ok(read)
    }
}
