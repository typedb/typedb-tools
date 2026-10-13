/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod catalog;
mod diagnostics;
mod emit;
mod iid;
mod mapping;
mod schema;
mod validate;
mod value;

use std::{fs::File, path::Path};

use duckdb::{AccessMode, Config, Connection};

pub use crate::{
    diagnostics::{Diagnostic, Diagnostics, ValidationStage},
    emit::Checksums,
};

/// The TypeDB version whose storage encodings the synthesised ids mirror.
pub const TYPEDB_FORMAT_VERSION: &str = "3.13.0";

#[derive(Debug)]
pub enum PrepareError {
    Setup(String),
    Invalid { errors: Vec<Diagnostic>, warnings: Vec<Diagnostic>, mapping_summary: Vec<String> },
}

pub struct Migration {
    connection: Connection,
    plan: mapping::MappingPlan,
    warnings: Vec<Diagnostic>,
}

impl Migration {
    pub fn prepare(db_path: &Path, schema_text: &str) -> Result<Migration, PrepareError> {
        let schema = schema::Schema::parse(schema_text).map_err(PrepareError::Setup)?;
        let duckdb_connection = Config::default()
            .access_mode(AccessMode::ReadOnly)
            .and_then(|config| Connection::open_with_flags(db_path, config))
            .map_err(|err| {
                PrepareError::Setup(format!("failed to open DuckDB database '{}': {err}", db_path.display()))
            })?;
        let catalog = catalog::Catalog::read(&duckdb_connection).map_err(PrepareError::Setup)?;

        let mut diagnostics = Diagnostics::default();
        let plan = mapping::resolve(&catalog, &schema, &mut diagnostics);
        diagnostics.ensure_valid(|| plan.summary())?;
        validate::validate_data(&duckdb_connection, &schema, &plan, &mut diagnostics);
        diagnostics.ensure_valid(|| plan.summary())?;
        Ok(Migration { connection: duckdb_connection, plan, warnings: diagnostics.warnings })
    }

    pub fn warnings(&self) -> &[Diagnostic] {
        &self.warnings
    }

    pub fn mapping_summary(&self) -> Vec<String> {
        self.plan.summary()
    }

    pub fn run(&self, output: File, original_database: &str) -> Result<Checksums, String> {
        emit::emit(&self.connection, &self.plan, TYPEDB_FORMAT_VERSION, original_database, output)
    }
}
