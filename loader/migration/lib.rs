/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod catalog;
mod emit;
mod mapping;
mod schema;
mod validate;
mod value;

use std::{
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
};

use duckdb::{AccessMode, Config, Connection};
pub use typedb_protocol::migration::item::Checksums;

use crate::mapping::MappingPlan;

pub enum Source {
    DuckDb(PathBuf),
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::DuckDb(path) => write!(f, "DuckDB file '{}'", path.display()),
        }
    }
}

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
    memory_limit: String,
    // Declared after the connection, which closes before its files are removed.
    _spill_dir: SpillDir,
}

// The loader's own tables live in a scratch database here, beside DuckDB's spill files: DuckDB 2.0 runs out of
// memory joining temporary tables that it would spill as tables of a database file.
pub(crate) const SCRATCH: &str = "typedb_loader_scratch";

struct SpillDir(PathBuf);

impl Drop for SpillDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

impl Migration {
    // Maps the source onto the schema and checks the data against both before anything is converted.
    // Scans larger than memory spill into `spill_dir`, created here and removed with the migration; DuckDB would
    // otherwise spill next to the database.
    // `memory_limit` is in DuckDB's syntax (`8GB`, `512MiB`); by default half of the memory DuckDB detects.
    pub fn prepare(
        source: &Source,
        schema_text: &str,
        spill_dir: &Path,
        memory_limit: Option<&str>,
    ) -> Result<Migration, PrepareError> {
        let schema = schema::Schema::parse(schema_text).map_err(PrepareError::Setup)?;
        fs::create_dir_all(spill_dir).map_err(|err| {
            PrepareError::Setup(format!("failed to create the spill directory '{}': {err}", spill_dir.display()))
        })?;
        let spill_dir_guard = SpillDir(spill_dir.to_path_buf());
        let connection = match source {
            Source::DuckDb(path) => open_duckdb(path, spill_dir)?,
        };
        let memory_limit = limit_memory(&connection, memory_limit)?;
        let catalog = catalog::Catalog::read(&connection).map_err(PrepareError::Setup)?;

        let mut diagnostics = Diagnostics::default();
        let plan = mapping::resolve(&catalog, &schema, &mut diagnostics);
        if diagnostics.errors.is_empty() {
            validate::check_data(&connection, &plan, &mut diagnostics);
        }
        let Diagnostics { errors, warnings } = diagnostics;
        if !errors.is_empty() {
            return Err(PrepareError::Invalid { errors, warnings, mapping_summary: plan.summary() });
        }
        Ok(Migration { connection, plan, warnings, memory_limit, _spill_dir: spill_dir_guard })
    }

    pub fn memory_limit(&self) -> &str {
        &self.memory_limit
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn mapping_summary(&self) -> Vec<String> {
        self.plan.summary()
    }

    // The loader stands in for the server in the header: they release in lockstep.
    pub fn run(
        &self,
        output: impl Write + Send,
        typedb_version: &str,
        original_database: &str,
    ) -> Result<Checksums, String> {
        emit::emit(&self.connection, &self.plan, typedb_version, original_database, output)
    }
}

fn open_duckdb(path: &Path, spill_dir: &Path) -> Result<Connection, PrepareError> {
    let connection = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .and_then(|config| config.with("temp_directory", spill_dir.to_string_lossy()))
        // Order comes from the queries' ORDER BY; preserving insertion order would keep joins from spilling.
        .and_then(|config| config.with("preserve_insertion_order", "false"))
        .and_then(|config| Connection::open_with_flags(path, config))
        .map_err(|err| {
            PrepareError::Setup(format!(
                "failed to open DuckDB database '{}': {err} (other formats are staged into DuckDB first; \
                 see the loader's DuckDB README)",
                path.display()
            ))
        })?;
    let scratch = spill_dir.join("scratch.duckdb").to_string_lossy().replace('\'', "''");
    connection.execute_batch(&format!("ATTACH '{scratch}' AS {SCRATCH} (READ_WRITE)")).map_err(|err| {
        PrepareError::Setup(format!("failed to create a scratch database in '{}': {err}", spill_dir.display()))
    })?;
    Ok(connection)
}

// Unlimited, DuckDB fills 80% of the machine's memory before it spills; half leaves room for the conversion's own
// buffers and for whatever runs alongside, such as a server.
fn limit_memory(connection: &Connection, memory_limit: Option<&str>) -> Result<String, PrepareError> {
    let current = || -> duckdb::Result<String> {
        connection.query_row("SELECT current_setting('memory_limit')", [], |row| row.get(0))
    };
    let limit = match memory_limit {
        Some(limit) => limit.to_owned(),
        None => {
            let default =
                current().map_err(|err| PrepareError::Setup(format!("failed to read DuckDB's memory: {err}")))?;
            // DuckDB reports an unlimited size when it cannot tell the machine's memory; that default is kept.
            let Some(bytes) = size_in_bytes(&default) else { return Ok(default) };
            format!("{}MiB", (bytes / 8 * 5) >> 20)
        }
    };
    connection
        .execute_batch(&format!("SET memory_limit = '{}'", limit.replace('\'', "''")))
        .map_err(|err| PrepareError::Setup(format!("invalid memory limit '{limit}': {err}")))?;
    current().map_err(|err| PrepareError::Setup(format!("failed to read DuckDB's memory limit: {err}")))
}

// DuckDB prints sizes as `19.3 GiB` or `512.0 MiB`.
fn size_in_bytes(size: &str) -> Option<u64> {
    let (number, unit) = size.trim().split_once(' ')?;
    let unit: u64 = match unit {
        "bytes" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return None,
    };
    let number: f64 = number.parse().ok()?;
    Some((number * unit as f64) as u64)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn duckdb_sizes_are_read_in_bytes() {
        assert_eq!(size_in_bytes("19.3 GiB"), Some((19.3 * (1u64 << 30) as f64) as u64));
        assert_eq!(size_in_bytes("512.0 MiB"), Some(512 << 20));
        assert_eq!(size_in_bytes("16383.9 PiB"), None);
        assert_eq!(size_in_bytes("lots"), None);
        assert_eq!(size_in_bytes("3 parsecs"), None);
    }

    #[test]
    fn the_memory_limit_defaults_to_half_of_the_machine_and_can_be_set() {
        let connection = Connection::open_in_memory().unwrap();
        let default: String =
            connection.query_row("SELECT current_setting('memory_limit')", [], |row| row.get(0)).unwrap();
        let halved = size_in_bytes(&limit_memory(&connection, None).unwrap()).unwrap();
        let expected = size_in_bytes(&default).unwrap() / 8 * 5;
        assert!(halved.abs_diff(expected) < 64 << 20, "{halved} vs {expected}");
        assert_eq!(limit_memory(&connection, Some("256MiB")).unwrap(), "256.0 MiB");
        assert!(matches!(limit_memory(&connection, Some("lots")), Err(PrepareError::Setup(_))));
    }
}
