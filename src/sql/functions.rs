use super::scope::FirestoreSqlScope;
use super::translator::{name_start, refuse_set, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{FirestoreExpr, FirestoreResult};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments,
    ObjectName, ObjectNamePart,
};

impl FirestoreSqlTranslator<'_> {
    /// A call of a scalar function. A name the alias table does not rename passes through to the
    /// pipeline function of that name, and Firestore rejects one it does not have.
    pub(super) fn function(
        &mut self,
        scope: &FirestoreSqlScope,
        function: &Function,
    ) -> FirestoreResult<FirestoreExpr> {
        let (name, arguments) = Self::call(function)?;
        let start = name_start(&function.name);
        if is_aggregate(&name) {
            return Err(FirestoreError::unsupported_sql(
                &format!("the aggregate function {name}"),
                start,
            ));
        }
        if matches!(name.as_str(), "document_matches" | "score") {
            return Err(FirestoreError::unsupported_sql("search", start));
        }
        let arguments = Self::positional_arguments(arguments, start)?
            .into_iter()
            .map(|argument| self.expr(scope, argument))
            .collect::<FirestoreResult<Vec<_>>>()?;
        match name.as_str() {
            // `string_index_of` counts from 0 and gives -1 for no match, where SQL counts from 1
            // and gives 0: one more is both.
            "strpos" => match <[FirestoreExpr; 2]>::try_from(arguments) {
                Ok([text, search]) => Ok(text.string_index_of(search).add(1)),
                Err(_) => Err(FirestoreError::invalid_sql(
                    "strpos",
                    "STRPOS takes a string and the string to find",
                    start,
                )),
            },
            _ => Ok(FirestoreExpr::raw(scalar_function(&name), arguments)),
        }
    }

    /// The position `OFFSET(n)` or `ORDINAL(n)` gives inside `x[..]`, counted from 0, or `None`
    /// for any other call.
    pub(super) fn array_position(
        &mut self,
        scope: &FirestoreSqlScope,
        function: &Function,
    ) -> FirestoreResult<Option<FirestoreExpr>> {
        let (name, arguments) = Self::call(function)?;
        let start = name_start(&function.name);
        let ordinal = match name.as_str() {
            "offset" | "safe_offset" => false,
            "ordinal" | "safe_ordinal" => true,
            _ => return Ok(None),
        };
        let [position] = Self::positional_arguments(arguments, start)?[..] else {
            return Err(FirestoreError::invalid_sql(
                name,
                "an array position takes one argument",
                start,
            ));
        };
        if ordinal {
            self.zero_based(scope, position).map(Some)
        } else {
            self.expr(scope, position).map(Some)
        }
    }

    /// The name, in lower case, and the arguments of a plain function call. A call with a
    /// window, a filter, an ordering or another clause is refused.
    fn call(function: &Function) -> FirestoreResult<(String, &[FunctionArg])> {
        let Function {
            name,
            uses_odbc_syntax,
            parameters,
            args,
            within_group,
            filter,
            null_treatment,
            over,
        } = function;
        let start = name_start(name);
        refuse_set(
            &[
                (over.is_some(), "a window function"),
                (*uses_odbc_syntax, "an ODBC call"),
                (
                    !matches!(parameters, FunctionArguments::None),
                    "a parametric function",
                ),
                (!within_group.is_empty(), "WITHIN GROUP"),
                (filter.is_some(), "FILTER"),
                (null_treatment.is_some(), "IGNORE NULLS or RESPECT NULLS"),
            ],
            start,
        )?;
        let name = Self::function_name(name, start)?;
        let arguments = match args {
            FunctionArguments::None => &[][..],
            FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment,
                args,
                clauses,
            }) => {
                if duplicate_treatment.is_some() {
                    return Err(FirestoreError::unsupported_sql(
                        "DISTINCT or ALL in a call",
                        start,
                    ));
                }
                if !clauses.is_empty() {
                    return Err(FirestoreError::unsupported_sql("a clause in a call", start));
                }
                args.as_slice()
            }
            FunctionArguments::Subquery(_) => {
                return Err(FirestoreError::unsupported_sql("a subquery", start))
            }
        };
        Ok((name, arguments))
    }

    /// A function's name in lower case: SQL names functions without regard to case.
    pub(super) fn function_name(
        name: &ObjectName,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<String> {
        match name.0.as_slice() {
            [ObjectNamePart::Identifier(ident)] => Ok(ident.value.to_lowercase()),
            _ => Err(FirestoreError::unsupported_sql(
                "a qualified function name",
                start,
            )),
        }
    }

    /// The expressions of `arguments`, which must all be positional.
    pub(super) fn positional_arguments(
        arguments: &[FunctionArg],
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<Vec<&Expr>> {
        arguments
            .iter()
            .map(|argument| match argument {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
                FunctionArg::Unnamed(_) => {
                    Err(FirestoreError::unsupported_sql("a * argument", start))
                }
                FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. } => {
                    Err(FirestoreError::unsupported_sql("a named argument", start))
                }
            })
            .collect()
    }
}

/// The pipeline function a SQL scalar function calls. The SQL names that differ from the
/// pipeline's are renamed, and every other name passes through.
fn scalar_function(name: &str) -> &str {
    match name {
        "lower" => "to_lower",
        "upper" => "to_upper",
        "power" => "pow",
        "ifnull" => "if_null",
        "regexp_contains" => "regex_contains",
        other => other,
    }
}

/// Whether `name` is an aggregate function in SQL or an accumulator of a pipeline. `minimum` and
/// `maximum` are left out: pipelines also have scalar functions of those names.
fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "countif"
            | "count_if"
            | "count_distinct"
            | "sum"
            | "avg"
            | "average"
            | "min"
            | "max"
            | "array_agg"
            | "array_agg_distinct"
            | "any_value"
            | "first"
            | "last"
            | "logical_and"
            | "logical_or"
            | "string_agg"
    )
}
