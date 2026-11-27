/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fs::read_to_string;

use typeql::{
    Variable,
    query::{
        QueryStructure,
        pipeline::stage::{Stage, given::Given},
    },
    schema::definable::function::Argument,
    type_::{NamedType, NamedTypeAny},
};

use crate::fatal;

#[derive(Debug, Clone, Copy)]
pub(crate) enum CellType {
    Boolean,
    Integer,
    Double,
    Decimal,
    String,
    Date,
    Datetime,
    DatetimeTz,
    Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct GivenSpec {
    pub(crate) name: String,
    pub(crate) cell_type: CellType,
    pub(crate) optional: bool,
}

/// Reads the query file and extracts its `given` input specs. Exits on file I/O failure or
/// parse failure — both are user-input errors at this stage.
pub(crate) fn load_query(query_path: &str) -> (String, Vec<GivenSpec>) {
    let query_text = read_to_string(query_path)
        .unwrap_or_else(|err| fatal(format!("failed to read query file '{query_path}': {err}")));
    let inputs = parse_query_inputs(&query_text).unwrap_or_else(|err| fatal(err));
    (query_text, inputs)
}

fn parse_query_inputs(query_text: &str) -> Result<Vec<GivenSpec>, String> {
    let parsed = typeql::parse_query(query_text).map_err(|err| format!("failed to parse query: {err}"))?;
    let pipeline = match parsed.into_structure() {
        QueryStructure::Pipeline(p) => p,
        QueryStructure::Schema(_) => return Err("schema queries do not accept input rows".to_owned()),
    };
    let given = pipeline
        .stages
        .into_iter()
        .find_map(|stage| match stage {
            Stage::Given(given) => Some(given),
            _ => None,
        })
        .ok_or_else(|| "query has no `given` stage; cannot bind input rows".to_owned())?;
    let Given { variables, .. } = given;
    variables.into_iter().map(into_given_input).collect()
}

fn into_given_input(arg: Argument) -> Result<GivenSpec, String> {
    let Argument { var, type_, .. } = arg;
    let name = match var {
        Variable::Named { ident, .. } => ident.as_str_unchecked().to_owned(),
        Variable::Anonymous { .. } => {
            return Err("anonymous variables are not supported in `given` inputs".to_owned());
        }
    };
    let (named_type, optional) = match type_ {
        NamedTypeAny::Simple(named) => (named, false),
        NamedTypeAny::Optional(opt) => (opt.inner, true),
        NamedTypeAny::List(_) => return Err(format!("list-typed `given` input '${name}' is not supported")),
    };
    let token = match named_type {
        NamedType::BuiltinValueType(t) => t.token,
        other @ (NamedType::Label(_) | NamedType::Vector(_)) => {
            return Err(format!(
                "`given` input '${name}' has type '{other}'; only built-in value types can be loaded from CSV"
            ));
        }
    };
    let cell_type = match token {
        typeql::token::ValueType::Boolean => CellType::Boolean,
        typeql::token::ValueType::Integer => CellType::Integer,
        typeql::token::ValueType::Double => CellType::Double,
        typeql::token::ValueType::Decimal => CellType::Decimal,
        typeql::token::ValueType::String => CellType::String,
        typeql::token::ValueType::Date => CellType::Date,
        typeql::token::ValueType::DateTime => CellType::Datetime,
        typeql::token::ValueType::DateTimeTZ => CellType::DatetimeTz,
        typeql::token::ValueType::Duration => CellType::Duration,
    };
    Ok(GivenSpec { name, cell_type, optional })
}
