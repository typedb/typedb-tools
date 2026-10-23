/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Prepares TypeDB import data (the `typedb export` format) from a DuckDB database and a
//! TypeQL schema. The mapping and the values are checked here; the schema's constraints are
//! the server's to enforce at import. README.md describes the mapping conventions.

mod catalog;
mod emit;
mod mapping;
mod schema;
mod value;

use std::{io::Write, path::Path};

use duckdb::{AccessMode, Config, Connection};
pub use typedb_protocol::migration::item::Checksums;

use crate::mapping::MappingPlan;

#[derive(Debug)]
pub enum PrepareError {
    /// The inputs could not be read: file access, an unopenable database, an unparseable schema.
    Setup(String),
    /// Validation found errors; the warnings and the mapping resolved so far go with them.
    Invalid { errors: Vec<String>, warnings: Vec<String>, mapping_summary: Vec<String> },
}

#[derive(Debug, Default)]
pub(crate) struct Diagnostics {
    errors: Vec<String>,
    warnings: Vec<String>,
}

impl Diagnostics {
    pub(crate) fn error(&mut self, message: impl Into<String>) {
        self.errors.push(message.into());
    }

    pub(crate) fn warning(&mut self, message: impl Into<String>) {
        self.warnings.push(message.into());
    }
}

/// A validated migration, ready to run.
pub struct Migration {
    connection: Connection,
    plan: MappingPlan,
    warnings: Vec<String>,
}

impl Migration {
    pub fn prepare(db_path: &Path, schema_text: &str) -> Result<Migration, PrepareError> {
        let schema = schema::Schema::parse(schema_text).map_err(PrepareError::Setup)?;
        let connection = Config::default()
            .access_mode(AccessMode::ReadOnly)
            .and_then(|config| Connection::open_with_flags(db_path, config))
            .map_err(|err| {
                PrepareError::Setup(format!("failed to open DuckDB database '{}': {err}", db_path.display()))
            })?;
        let catalog = catalog::Catalog::read(&connection).map_err(PrepareError::Setup)?;

        let mut diagnostics = Diagnostics::default();
        let plan = mapping::resolve(&catalog, &schema, &mut diagnostics);
        let Diagnostics { errors, warnings } = diagnostics;
        if !errors.is_empty() {
            return Err(PrepareError::Invalid { errors, warnings, mapping_summary: plan.summary() });
        }
        Ok(Migration { connection, plan, warnings })
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn mapping_summary(&self) -> Vec<String> {
        self.plan.summary()
    }

    /// Streams the items to `output`, under a header naming the producing version and the
    /// source; the same database and schema always produce the same bytes.
    pub fn run(&self, output: impl Write, typedb_version: &str, original_database: &str) -> Result<Checksums, String> {
        emit::emit(&self.connection, &self.plan, typedb_version, original_database, output)
    }
}
