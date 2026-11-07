//! The index statements: `CREATE [UNIQUE | SEARCH | VECTOR] INDEX` and both forms of `DROP INDEX`.

use super::ddl::{
    next_start, option_string, FirestoreSqlIndexKind, FirestoreSqlIndexTarget, FirestoreSqlOption,
};
use super::translator::{at, expr_start, field_path, is_parameter, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{
    FirestoreCompositeIndex, FirestoreGeoIndexConfig, FirestoreIndexDefinition,
    FirestoreIndexDeleteTarget, FirestoreIndexDensity, FirestoreIndexField,
    FirestoreIndexFieldMode, FirestoreIndexId, FirestoreIndexIdTarget, FirestoreIndexQueryScope,
    FirestoreQueryDirection, FirestoreResult, FirestoreSearchIndexOptions,
    FirestoreVectorIndexConfig,
};
use sqlparser::ast::{
    Expr, ObjectName, ObjectNamePart, OrderBySort, Value as SqlValue, ValueWithSpan,
};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

/// How a column of an index definition is written. The kind of index gives each form its mode:
/// a bare field is ascending in an ordinary index, text in a search index, and the vector field
/// of a vector index.
enum FirestoreSqlIndexColumnForm {
    Bare,
    Order(FirestoreQueryDirection),
    Contains,
    Geo,
}

impl FirestoreSqlIndexColumnForm {
    /// The form an operator class names: `CONTAINS` or `GEO`, whatever its case.
    fn from_operator_class(
        name: &ObjectName,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<Self> {
        let word = match name.0.as_slice() {
            [ObjectNamePart::Identifier(ident)] if ident.quote_style.is_none() => {
                Some(ident.value.to_ascii_uppercase())
            }
            _ => None,
        };
        match word.as_deref() {
            Some("CONTAINS") => Ok(Self::Contains),
            Some("GEO") => Ok(Self::Geo),
            _ => Err(FirestoreError::invalid_sql(
                "column",
                format!("`{name}` is not a column mode; the modes are CONTAINS and GEO"),
                start,
            )),
        }
    }
}

struct FirestoreSqlIndexColumn {
    field_path: String,
    form: FirestoreSqlIndexColumnForm,
    start: Option<FirestoreSqlLocation>,
}

impl FirestoreSqlTranslator<'_> {
    /// The index a `CREATE .. INDEX` defines, read after `INDEX` and any `IF NOT EXISTS`.
    pub(super) fn index_definition(
        &self,
        parser: &mut Parser,
        kind: Option<FirestoreSqlIndexKind>,
    ) -> FirestoreResult<FirestoreIndexDefinition> {
        if !parser.parse_keyword(Keyword::ON) {
            let next = parser.peek_token();
            return Err(match next.token {
                Token::Word(_) => FirestoreError::invalid_sql(
                    "index_name",
                    "Firestore assigns the ID of an index; an index cannot be named",
                    at(next.span),
                ),
                _ => FirestoreError::invalid_sql("CREATE INDEX", "expected ON", at(next.span)),
            });
        }
        self.index_on_target(parser, kind)
    }

    /// The index a `DROP .. INDEX` deletes, by ID or by definition, read after `INDEX` and any
    /// `IF EXISTS`.
    pub(super) fn index_delete_target(
        &self,
        parser: &mut Parser,
        kind: Option<FirestoreSqlIndexKind>,
    ) -> FirestoreResult<FirestoreIndexDeleteTarget> {
        if parser.parse_keyword(Keyword::ON) {
            return Ok(FirestoreIndexDeleteTarget::Definition(
                self.index_on_target(parser, kind)?,
            ));
        }
        if kind.is_some() {
            return Err(FirestoreError::invalid_sql(
                "DROP INDEX",
                "UNIQUE, SEARCH and VECTOR belong to the definition of the index to drop: DROP \
                 .. INDEX ON target (columns)",
                next_start(parser),
            ));
        }
        let id = parser
            .parse_identifier()
            .map_err(FirestoreError::from_sql_parser)?;
        let start = at(id.span);
        if is_parameter(&id) {
            return Err(FirestoreError::sql_parameter_names_field(&id.value, start));
        }
        let target = FirestoreIndexIdTarget::new(
            FirestoreIndexId::new(id.value).map_err(|error| error.located(start))?,
        );
        Ok(FirestoreIndexDeleteTarget::Id(
            if parser.parse_keyword(Keyword::ON) {
                target.with_collection_group(self.collection_group_id(parser)?)
            } else {
                target
            },
        ))
    }

    /// The definition read after `ON`: the target, the columns and the options.
    fn index_on_target(
        &self,
        parser: &mut Parser,
        kind: Option<FirestoreSqlIndexKind>,
    ) -> FirestoreResult<FirestoreIndexDefinition> {
        let FirestoreSqlIndexTarget {
            collection_group,
            query_scope,
        } = self.index_target(parser)?;
        let columns = Self::index_columns(parser)?;
        let options = Self::index_options(kind, self.statement_options(parser)?)?;
        let mut index =
            FirestoreCompositeIndex::new(Self::index_fields(kind, columns, options.dimension)?);
        if query_scope == FirestoreIndexQueryScope::AllDescendants {
            index = index.all_descendants();
        }
        if kind == Some(FirestoreSqlIndexKind::Unique) {
            index = index.unique();
        }
        index.density = options.density;
        index.search_options = options.search;
        Ok(FirestoreIndexDefinition::new(collection_group, index))
    }

    fn index_columns(parser: &mut Parser) -> FirestoreResult<Vec<FirestoreSqlIndexColumn>> {
        parser
            .expect_token(&Token::LParen)
            .map_err(FirestoreError::from_sql_parser)?;
        let mut columns = Vec::new();
        loop {
            columns.push(Self::index_column(parser)?);
            if !parser.consume_token(&Token::Comma) {
                break;
            }
        }
        parser
            .expect_token(&Token::RParen)
            .map_err(FirestoreError::from_sql_parser)?;
        Ok(columns)
    }

    fn index_column(parser: &mut Parser) -> FirestoreResult<FirestoreSqlIndexColumn> {
        let start = next_start(parser);
        let column = parser
            .parse_create_index_expr()
            .map_err(FirestoreError::from_sql_parser)?;
        let field_path = match &column.column.expr {
            Expr::Identifier(ident) if is_parameter(ident) => {
                return Err(FirestoreError::sql_parameter_names_field(
                    &ident.value,
                    at(ident.span),
                ))
            }
            Expr::Identifier(ident) => field_path(std::slice::from_ref(ident)),
            Expr::CompoundIdentifier(parts) => field_path(parts),
            other => {
                return Err(FirestoreError::invalid_sql(
                    "column",
                    "a column of an index is a field path",
                    expr_start(other).or(start),
                ))
            }
        };
        let options = &column.column.options;
        let direction = match &options.sort {
            None => None,
            Some(OrderBySort::Asc) => Some(FirestoreQueryDirection::Ascending),
            Some(OrderBySort::Desc) => Some(FirestoreQueryDirection::Descending),
            Some(OrderBySort::Using(_)) => {
                return Err(FirestoreError::invalid_sql(
                    "column",
                    "a column of an index takes ASC or DESC, not USING",
                    start,
                ))
            }
        };
        if options.nulls_first.is_some() || column.column.with_fill.is_some() {
            return Err(FirestoreError::invalid_sql(
                "column",
                "a column of an index takes no NULLS or WITH FILL clause",
                start,
            ));
        }
        let mode = column
            .operator_class
            .as_ref()
            .map(|name| FirestoreSqlIndexColumnForm::from_operator_class(name, start))
            .transpose()?;
        let form = match (direction, mode) {
            (Some(_), Some(_)) => {
                return Err(FirestoreError::invalid_sql(
                    "column",
                    "a column takes a direction or a mode, not both",
                    start,
                ))
            }
            (Some(direction), None) => FirestoreSqlIndexColumnForm::Order(direction),
            (None, Some(mode)) => mode,
            (None, None) => FirestoreSqlIndexColumnForm::Bare,
        };
        Ok(FirestoreSqlIndexColumn {
            field_path,
            form,
            start,
        })
    }

    /// The fields of an index of `kind`: the columns of an ordinary index are ordered or array
    /// fields, those of a search index are text or geo fields, and the last column of a vector
    /// index is its vector field, written bare, of `dimension`.
    fn index_fields(
        kind: Option<FirestoreSqlIndexKind>,
        mut columns: Vec<FirestoreSqlIndexColumn>,
        dimension: Option<u32>,
    ) -> FirestoreResult<Vec<FirestoreIndexField>> {
        let vector = match kind {
            Some(FirestoreSqlIndexKind::Vector) => columns.pop(),
            _ => None,
        };
        let mut fields = Vec::with_capacity(columns.len() + 1);
        for column in columns {
            let mode = match (kind, column.form) {
                (Some(FirestoreSqlIndexKind::Search), FirestoreSqlIndexColumnForm::Bare) => {
                    FirestoreIndexFieldMode::Text
                }
                (Some(FirestoreSqlIndexKind::Search), FirestoreSqlIndexColumnForm::Geo) => {
                    FirestoreIndexFieldMode::Geo(FirestoreGeoIndexConfig::new())
                }
                (Some(FirestoreSqlIndexKind::Search), _) => {
                    return Err(FirestoreError::invalid_sql(
                        "column",
                        "a column of a search index is a field, or a field with GEO",
                        column.start,
                    ))
                }
                (_, FirestoreSqlIndexColumnForm::Bare) => {
                    FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending)
                }
                (_, FirestoreSqlIndexColumnForm::Order(direction)) => {
                    FirestoreIndexFieldMode::Order(direction)
                }
                (_, FirestoreSqlIndexColumnForm::Contains) => {
                    FirestoreIndexFieldMode::ArrayContains
                }
                (_, FirestoreSqlIndexColumnForm::Geo) => {
                    return Err(FirestoreError::invalid_sql(
                        "column",
                        "GEO belongs to a column of a search index",
                        column.start,
                    ))
                }
            };
            fields.push(FirestoreIndexField::new(column.field_path, mode));
        }
        if let Some(column) = vector {
            let FirestoreSqlIndexColumnForm::Bare = column.form else {
                return Err(FirestoreError::invalid_sql(
                    "column",
                    "the vector field of a vector index is written bare, last",
                    column.start,
                ));
            };
            let dimension = dimension.ok_or_else(|| {
                FirestoreError::invalid_sql(
                    "dimension",
                    "CREATE VECTOR INDEX takes its dimension in OPTIONS (dimension = n)",
                    column.start,
                )
            })?;
            fields.push(FirestoreIndexField::new(
                column.field_path,
                FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(dimension)),
            ));
        }
        Ok(fields)
    }

    /// The options of an index of `kind`, each of which only some kinds take.
    fn index_options(
        kind: Option<FirestoreSqlIndexKind>,
        options: Vec<FirestoreSqlOption>,
    ) -> FirestoreResult<FirestoreSqlIndexOptions> {
        let mut read = FirestoreSqlIndexOptions::default();
        let mut text_language = None;
        let mut text_language_override_field_path = None;
        for option in options {
            match (kind, option.key.as_str()) {
                (
                    None | Some(FirestoreSqlIndexKind::Unique | FirestoreSqlIndexKind::Vector),
                    "density",
                ) => read.density = Some(Self::density(&option)?),
                (Some(FirestoreSqlIndexKind::Vector), "dimension") => {
                    read.dimension = Some(Self::dimension(&option)?)
                }
                (Some(FirestoreSqlIndexKind::Search), "text_language") => {
                    text_language = Some(option_string(&option.key, &option.value)?)
                }
                (Some(FirestoreSqlIndexKind::Search), "text_language_override_field_path") => {
                    text_language_override_field_path =
                        Some(option_string(&option.key, &option.value)?)
                }
                _ => {
                    return Err(FirestoreError::invalid_sql(
                        &option.key,
                        format!("this statement does not take the option {}", option.key),
                        option.start,
                    ))
                }
            }
        }
        if text_language.is_some() || text_language_override_field_path.is_some() {
            read.search = Some(FirestoreSearchIndexOptions {
                text_language,
                text_language_override_field_path,
            });
        }
        Ok(read)
    }

    fn density(option: &FirestoreSqlOption) -> FirestoreResult<FirestoreIndexDensity> {
        let density = option_string(&option.key, &option.value)?;
        match density.to_ascii_uppercase().as_str() {
            "SPARSE_ALL" => Ok(FirestoreIndexDensity::SparseAll),
            "SPARSE_ANY" => Ok(FirestoreIndexDensity::SparseAny),
            "DENSE" => Ok(FirestoreIndexDensity::Dense),
            _ => Err(FirestoreError::invalid_sql(
                &option.key,
                format!("density is SPARSE_ALL, SPARSE_ANY or DENSE, not '{density}'"),
                option.start,
            )),
        }
    }

    fn dimension(option: &FirestoreSqlOption) -> FirestoreResult<u32> {
        match &option.value {
            Expr::Value(ValueWithSpan {
                value: SqlValue::Number(text, _),
                ..
            }) => text.parse().ok(),
            _ => None,
        }
        .ok_or_else(|| {
            FirestoreError::invalid_sql(
                &option.key,
                "dimension takes a non-negative integer literal",
                option.start,
            )
        })
    }
}

/// The options of an index statement, read for its kind of index.
#[derive(Default)]
struct FirestoreSqlIndexOptions {
    density: Option<FirestoreIndexDensity>,
    dimension: Option<u32>,
    search: Option<FirestoreSearchIndexOptions>,
}
