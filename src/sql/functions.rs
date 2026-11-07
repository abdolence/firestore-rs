use super::aggregation::is_aggregate_function;
use super::scope::FirestoreSqlScope;
use super::translator::{name_start, refuse_set, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{FirestoreExpr, FirestoreResult};
use sqlparser::ast::{
    DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, ObjectName, ObjectNamePart, Query,
};

pub(super) struct FirestoreSqlCall<'f> {
    pub(super) name: String,
    pub(super) arguments: &'f [FunctionArg],
    pub(super) duplicate_treatment: Option<&'f DuplicateTreatment>,
    pub(super) subquery: Option<&'f Query>,
}

impl FirestoreSqlCall<'_> {
    fn refuse_duplicate_treatment(
        &self,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        match self.duplicate_treatment {
            Some(_) => Err(FirestoreError::unsupported_sql(
                "DISTINCT or ALL in a call",
                start,
            )),
            None => Ok(()),
        }
    }
}

impl FirestoreSqlTranslator<'_> {
    /// A call of a scalar function. A name the alias table does not rename passes through to the
    /// pipeline function of that name, and Firestore rejects one it does not have.
    pub(super) fn function(
        &mut self,
        scope: &FirestoreSqlScope,
        function: &Function,
    ) -> FirestoreResult<FirestoreExpr> {
        self.named_function(scope, function, None)
    }

    pub(super) fn named_function(
        &mut self,
        scope: &FirestoreSqlScope,
        function: &Function,
        output: Option<&str>,
    ) -> FirestoreResult<FirestoreExpr> {
        let call = Self::call_parts(function)?;
        let start = name_start(&function.name);
        if let Some(query) = call.subquery {
            return match call.name.as_str() {
                "array" => self.array_subquery(scope, query),
                name => Err(FirestoreError::unsupported_sql(
                    &format!("a subquery as the argument of {name}"),
                    start,
                )),
            };
        }
        if is_aggregate_function(&call.name) {
            return self.aggregate(scope, &call, output, start);
        }
        call.refuse_duplicate_treatment(start)?;
        let FirestoreSqlCall {
            name, arguments, ..
        } = call;
        match name.as_str() {
            "score" => return self.score(arguments, output, start),
            "document_matches" | "geo_distance" => {
                return Err(Self::search_misplaced(&name, start))
            }
            _ => {}
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

    pub(super) fn call(function: &Function) -> FirestoreResult<(String, &[FunctionArg])> {
        let call = Self::call_parts(function)?;
        call.refuse_duplicate_treatment(name_start(&function.name))?;
        if call.subquery.is_some() {
            return Err(FirestoreError::unsupported_sql(
                "a subquery as an argument",
                name_start(&function.name),
            ));
        }
        Ok((call.name, call.arguments))
    }

    fn call_parts(function: &Function) -> FirestoreResult<FirestoreSqlCall<'_>> {
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
        let (arguments, duplicate_treatment, subquery) = match args {
            FunctionArguments::None => (&[][..], None, None),
            FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment,
                args,
                clauses,
            }) => {
                if !clauses.is_empty() {
                    return Err(FirestoreError::unsupported_sql("a clause in a call", start));
                }
                (args.as_slice(), duplicate_treatment.as_ref(), None)
            }
            FunctionArguments::Subquery(query) => (&[][..], None, Some(query.as_ref())),
        };
        Ok(FirestoreSqlCall {
            name,
            arguments,
            duplicate_treatment,
            subquery,
        })
    }

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

pub(super) fn names_its_output(function: &Function) -> bool {
    matches!(
        FirestoreSqlTranslator::function_name(&function.name, None),
        Ok(name) if is_aggregate_function(&name) || name == "score"
    )
}
