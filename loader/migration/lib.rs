/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod catalog;
mod chunks;
mod emit;
mod mapping;
mod schema;
mod sql;
mod validate;
mod value;

use std::{
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use duckdb::{AccessMode, Config, Connection};
use typedb_cli_common::constants::{GB, KB, MB, TB};
pub use typedb_protocol::migration::item::Checksums;

use crate::{mapping::MappingPlan, sql::setting};

const DUCKDB_DEFAULT_MEMORY_PERCENT: u64 = 80;
// Half of the machine, leaving room for the conversion's buffers and for a server running alongside.
const DEFAULT_MEMORY_PERCENT: u64 = 50;
const MEMORY_FLOOR: u64 = 512 * MB;
// A hash aggregate or join fails instead of spilling below a few tens of MB per thread.
const MEMORY_PER_THREAD: u64 = 64 * MB;

pub struct Migration {
    connection: Connection,
    plan: MappingPlan,
    warnings: Vec<String>,
    memory_limit: String,
    threads: usize,
    cancellation: Arc<Cancellation>,
    // Declared after the connection, which closes before its files are removed.
    _spill_dir: SpillDir,
}

impl Migration {
    pub fn prepare(
        source: &Path,
        schema_text: &str,
        spill_dir: &Path,
        memory_limit: Option<&str>,
        cancellation: Arc<Cancellation>,
    ) -> Result<Migration, PrepareError> {
        let prepared = Self::map_and_check(source, schema_text, spill_dir, memory_limit, cancellation.clone());
        if cancellation.is_cancelled() { Err(PrepareError::Cancelled) } else { prepared }
    }

    pub fn memory_limit(&self) -> &str {
        &self.memory_limit
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn describe_mapping(&self) -> Vec<String> {
        self.plan.describe()
    }

    // The loader stands in for the server in the header: they release in lockstep.
    pub fn run(
        &self,
        output: impl Write + Send,
        typedb_version: &str,
        original_database: &str,
        progress: &dyn ProgressListener,
    ) -> Result<Checksums, String> {
        emit::emit(self, output, typedb_version, original_database, progress)
    }

    fn map_and_check(
        source: &Path,
        schema_text: &str,
        spill_dir: &Path,
        memory_limit: Option<&str>,
        cancellation: Arc<Cancellation>,
    ) -> Result<Migration, PrepareError> {
        let schema = schema::Schema::parse(schema_text).map_err(PrepareError::Setup)?;
        create_private_dir(spill_dir).map_err(|err| {
            PrepareError::Setup(format!("failed to create the spill directory '{}': {err}", spill_dir.display()))
        })?;
        let spill_dir_guard = SpillDir(spill_dir.to_path_buf());
        let connection = open_duckdb(source, spill_dir)?;
        let interrupt = connection.interrupt_handle();
        cancellation.set_cancel_action(move || interrupt.interrupt());
        let memory_limit = set_memory_limit(&connection, memory_limit)?;
        let threads = fit_threads(&connection, &memory_limit)?;
        let catalog = catalog::Catalog::read(&connection).map_err(PrepareError::Setup)?;

        let mut findings = Findings::default();
        let plan = mapping::resolve(&catalog, &schema, &mut findings);
        if !findings.has_errors() {
            validate::check_data(&connection, &plan, &cancellation, &mut findings);
        }
        let Findings { errors, warnings } = findings;
        if !errors.is_empty() {
            return Err(PrepareError::Invalid { errors, warnings, mapping_description: plan.describe() });
        }
        Ok(Migration { connection, plan, warnings, memory_limit, threads, cancellation, _spill_dir: spill_dir_guard })
    }
}

#[derive(Debug)]
pub enum PrepareError {
    Setup(String),
    Invalid { errors: Vec<String>, warnings: Vec<String>, mapping_description: Vec<String> },
    Cancelled,
}

pub trait ProgressListener: Sync {
    fn report(&self, progress: &ScanProgress<'_>);
}

pub struct ScanProgress<'a> {
    pub scan: &'a str,
    pub rows: u64,
    pub total_rows: Option<u64>,
    pub encoded_bytes: u64,
}

impl fmt::Display for ScanProgress<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.total_rows.map(|total| format!(" of {total}")).unwrap_or_default();
        let encoded = self.encoded_bytes as f64 / GB as f64;
        write!(f, "{}: {}{total} rows, {encoded:.1} GB encoded", self.scan, self.rows)
    }
}

#[derive(Default)]
pub struct Cancellation {
    is_cancelled: AtomicBool,
    cancel_action: OnceLock<Box<dyn Fn() + Send + Sync>>,
}

impl Cancellation {
    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst);
        if let Some(action) = self.cancel_action.get() {
            action();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    fn set_cancel_action(&self, action: impl Fn() + Send + Sync + 'static) {
        assert!(self.cancel_action.set(Box::new(action)).is_ok(), "a cancellation serves one migration");
        if self.is_cancelled() {
            self.cancel_action.get().expect("just set")();
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Findings {
    errors: Vec<String>,
    warnings: Vec<String>,
}

impl Findings {
    pub(crate) fn error(&mut self, message: impl Into<String>) {
        self.errors.push(message.into());
    }

    pub(crate) fn warning(&mut self, message: impl Into<String>) {
        self.warnings.push(message.into());
    }

    pub(crate) fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

struct SpillDir(PathBuf);

impl Drop for SpillDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// Created afresh, so that removing it removes only what DuckDB spilled, and private, as it holds the source's data.
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(path)
}

fn open_duckdb(path: &Path, spill_dir: &Path) -> Result<Connection, PrepareError> {
    Config::default()
        .access_mode(AccessMode::ReadOnly)
        // Nothing is fetched from the network, and a file reads the same on every machine.
        .and_then(|config| config.with(setting::AUTOINSTALL_KNOWN_EXTENSIONS, "false"))
        .and_then(|config| config.with(setting::AUTOLOAD_KNOWN_EXTENSIONS, "false"))
        .and_then(|config| config.with(setting::TEMP_DIRECTORY, spill_dir.to_string_lossy()))
        // No scan depends on the order of its rows; preserving it would keep DuckDB from spilling.
        .and_then(|config| config.with(setting::PRESERVE_INSERTION_ORDER, "false"))
        .and_then(|config| Connection::open_with_flags(path, config))
        .map_err(|err| {
            PrepareError::Setup(format!(
                "failed to open DuckDB database '{}': {err} (the loader's DuckDB README covers staging other \
                 formats and files of other DuckDB versions)",
                path.display()
            ))
        })
}

fn set_memory_limit(connection: &Connection, requested: Option<&str>) -> Result<String, PrepareError> {
    let limit = match requested {
        Some(limit) => limit.to_owned(),
        None => {
            let default = current_setting(connection, setting::MEMORY_LIMIT)?;
            // DuckDB reports an unlimited size when it cannot tell the machine's memory; that default is kept.
            let Some(bytes) = size_in_bytes(&default) else { return Ok(default) };
            let half_of_the_machine = bytes / DUCKDB_DEFAULT_MEMORY_PERCENT * DEFAULT_MEMORY_PERCENT;
            format!("{}MiB", half_of_the_machine.max(MEMORY_FLOOR) / MB)
        }
    };
    connection
        .execute_batch(&setting::set_memory_limit(&limit))
        .map_err(|err| PrepareError::Setup(format!("invalid memory limit '{limit}': {err}")))?;
    let limit = current_setting(connection, setting::MEMORY_LIMIT)?;
    if size_in_bytes(&limit).is_some_and(|bytes| bytes < MEMORY_FLOOR) {
        return Err(PrepareError::Setup(format!(
            "the memory limit {limit} is too small: the conversion needs at least {} MiB",
            MEMORY_FLOOR / MB
        )));
    }
    Ok(limit)
}

fn fit_threads(connection: &Connection, memory_limit: &str) -> Result<usize, PrepareError> {
    let threads: usize = current_setting(connection, setting::THREADS)?.parse().unwrap_or(1);
    let Some(bytes) = size_in_bytes(memory_limit) else { return Ok(threads) };
    let affordable = usize::try_from(bytes / MEMORY_PER_THREAD).unwrap_or(usize::MAX).max(1);
    if affordable < threads {
        connection
            .execute_batch(&setting::set_threads(affordable))
            .map_err(|err| PrepareError::Setup(format!("failed to set DuckDB's threads: {err}")))?;
    }
    Ok(threads.min(affordable))
}

fn current_setting(connection: &Connection, name: &str) -> Result<String, PrepareError> {
    connection
        .query_row(&setting::current(name), [], |row| row.get(0))
        .map_err(|err| PrepareError::Setup(format!("failed to read DuckDB's {name}: {err}")))
}

// DuckDB prints sizes as `19.3 GiB` or `512.0 MiB`.
fn size_in_bytes(size: &str) -> Option<u64> {
    let (number, unit) = size.trim().split_once(' ')?;
    let unit: u64 = match unit {
        "bytes" => 1,
        "KiB" => KB,
        "MiB" => MB,
        "GiB" => GB,
        "TiB" => TB,
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
        assert_eq!(size_in_bytes("19.3 GiB"), Some((19.3 * GB as f64) as u64));
        assert_eq!(size_in_bytes("512.0 MiB"), Some(512 * MB));
        assert_eq!(size_in_bytes("16383.9 PiB"), None);
        assert_eq!(size_in_bytes("lots"), None);
        assert_eq!(size_in_bytes("3 parsecs"), None);
    }

    #[test]
    fn the_memory_limit_defaults_to_half_of_the_machine_and_can_be_set() {
        let connection = Connection::open_in_memory().unwrap();
        let default: String =
            connection.query_row("SELECT current_setting('memory_limit')", [], |row| row.get(0)).unwrap();
        let halved = size_in_bytes(&set_memory_limit(&connection, None).unwrap()).unwrap();
        let expected = size_in_bytes(&default).unwrap() / DUCKDB_DEFAULT_MEMORY_PERCENT * DEFAULT_MEMORY_PERCENT;
        assert!(halved.abs_diff(expected) < 64 * MB, "{halved} vs {expected}");
        assert_eq!(set_memory_limit(&connection, Some("512MiB")).unwrap(), "512.0 MiB");
        assert!(matches!(set_memory_limit(&connection, Some("lots")), Err(PrepareError::Setup(_))));
    }

    #[test]
    fn the_source_is_read_without_extensions() {
        let path = std::env::temp_dir().join(format!("typedb-migration-extensions-{}.duckdb", std::process::id()));
        drop(Connection::open(&path).unwrap());
        let connection = open_duckdb(&path, &std::env::temp_dir()).unwrap();
        for setting in ["autoinstall_known_extensions", "autoload_known_extensions"] {
            let enabled: bool =
                connection.query_row(&format!("SELECT current_setting('{setting}')"), [], |row| row.get(0)).unwrap();
            assert!(!enabled, "{setting}");
        }
        drop(connection);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn a_small_memory_limit_runs_fewer_threads() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("SET threads = 8").unwrap();
        assert_eq!(fit_threads(&connection, "256.0 MiB").unwrap(), 4);
        let threads: i64 = connection.query_row("SELECT current_setting('threads')", [], |row| row.get(0)).unwrap();
        assert_eq!(threads, 4);
        assert!(matches!(set_memory_limit(&connection, Some("256MiB")), Err(PrepareError::Setup(message))
            if message.contains("at least 512 MiB")));
    }
}
