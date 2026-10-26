/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
    Setup(String),
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

pub struct Migration {
    connection: Connection,
    plan: MappingPlan,
    warnings: Vec<String>,
}

impl Migration {
    // Scans larger than memory spill into `spill_dir`, which DuckDB would otherwise put next to the database.
    pub fn prepare(db_path: &Path, schema_text: &str, spill_dir: &Path) -> Result<Migration, PrepareError> {
        let schema = schema::Schema::parse(schema_text).map_err(PrepareError::Setup)?;
        let connection = Config::default()
            .access_mode(AccessMode::ReadOnly)
            .and_then(|config| config.with("temp_directory", spill_dir.to_string_lossy()))
            .and_then(|config| config.with("preserve_insertion_order", "true"))
            .and_then(|config| Connection::open_with_flags(db_path, config))
            .map_err(|err| {
                PrepareError::Setup(format!(
                    "failed to open DuckDB database '{}': {err} (other formats are staged into DuckDB first; \
                     see the loader's migration README)",
                    db_path.display()
                ))
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

    pub fn run(&self, output: impl Write, typedb_version: &str, original_database: &str) -> Result<Checksums, String> {
        emit::emit(&self.connection, &self.plan, typedb_version, original_database, output)
    }
}
