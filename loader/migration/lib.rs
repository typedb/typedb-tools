/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod catalog;
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
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use duckdb::{AccessMode, Config, Connection, InterruptHandle};
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

pub struct Migration {
    connection: Connection,
    plan: MappingPlan,
    warnings: Vec<String>,
    memory_limit: String,
    cancellation: Arc<Cancellation>,
    // Declared after the connection, which closes before its files are removed.
    _spill_dir: SpillDir,
}

impl Migration {
    // Maps and checks the source before anything is converted; `spill_dir` is created here and removed with it.
    pub fn prepare(
        source: &Source,
        schema_text: &str,
        spill_dir: &Path,
        memory_limit: Option<&str>,
        cancellation: Arc<Cancellation>,
    ) -> Result<Migration, PrepareError> {
        let prepared = Self::map_and_check(source, schema_text, spill_dir, memory_limit, cancellation.clone());
        // An interrupted query fails like any other.
        if cancellation.is_cancelled() { Err(PrepareError::Cancelled) } else { prepared }
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
        progress: &dyn ProgressListener,
    ) -> Result<Checksums, String> {
        emit::emit(
            &self.connection,
            &self.plan,
            typedb_version,
            original_database,
            output,
            progress,
            &self.cancellation,
        )
    }

    fn map_and_check(
        source: &Source,
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
        let connection = match source {
            Source::DuckDb(path) => open_duckdb(path, spill_dir)?,
        };
        cancellation.interrupts(&connection);
        let memory_limit = limit_memory(&connection, memory_limit)?;
        let catalog = catalog::Catalog::read(&connection).map_err(PrepareError::Setup)?;

        let mut findings = Findings::default();
        let plan = mapping::resolve(&catalog, &schema, &mut findings);
        if !findings.has_errors() {
            validate::check_data(&connection, &plan, &cancellation, &mut findings);
        }
        let Findings { errors, warnings } = findings;
        if !errors.is_empty() {
            return Err(PrepareError::Invalid { errors, warnings, mapping_summary: plan.summary() });
        }
        Ok(Migration { connection, plan, warnings, memory_limit, cancellation, _spill_dir: spill_dir_guard })
    }
}

#[derive(Debug)]
pub enum PrepareError {
    Setup(String),
    Invalid { errors: Vec<String>, warnings: Vec<String>, mapping_summary: Vec<String> },
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
        write!(f, "{}: {}{total} rows, {:.1} GB encoded", self.scan, self.rows, self.encoded_bytes as f64 / 1e9)
    }
}

// Stops a migration from another thread: the running query fails, and the checks and scans start no further one.
#[derive(Default)]
pub struct Cancellation {
    is_cancelled: AtomicBool,
    interrupt: Mutex<Option<Arc<InterruptHandle>>>,
}

impl Cancellation {
    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst);
        if let Some(interrupt) = self.interrupt_handle().as_ref() {
            interrupt.interrupt();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    // The handle is in place before the flag is read, so that a concurrent `cancel` sees one or the other.
    fn interrupts(&self, connection: &Connection) {
        let interrupt = connection.interrupt_handle();
        *self.interrupt_handle() = Some(interrupt.clone());
        if self.is_cancelled() {
            interrupt.interrupt();
        }
    }

    fn interrupt_handle(&self) -> std::sync::MutexGuard<'_, Option<Arc<InterruptHandle>>> {
        self.interrupt.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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
        // An extension loaded mid-run would change what the generated columns read after the checks.
        .and_then(|config| config.with("autoinstall_known_extensions", "false"))
        .and_then(|config| config.with("autoload_known_extensions", "false"))
        .and_then(|config| config.with("temp_directory", spill_dir.to_string_lossy()))
        // No scan depends on the order of its rows; preserving it would keep DuckDB from spilling.
        .and_then(|config| config.with("preserve_insertion_order", "false"))
        .and_then(|config| Connection::open_with_flags(path, config))
        .map_err(|err| {
            PrepareError::Setup(format!(
                "failed to open DuckDB database '{}': {err} (the loader's DuckDB README covers staging other \
                 formats and files of other DuckDB versions)",
                path.display()
            ))
        })
}

// Five eighths of DuckDB's default 80%: half the machine, leaving room for the conversion and for a server.
fn limit_memory(connection: &Connection, memory_limit: Option<&str>) -> Result<String, PrepareError> {
    let setting = |name: &str| -> duckdb::Result<String> {
        connection.query_row(&sql::setting::current(name), [], |row| row.get(0))
    };
    let limit = match memory_limit {
        Some(limit) => limit.to_owned(),
        None => {
            let default = setting("memory_limit")
                .map_err(|err| PrepareError::Setup(format!("failed to read DuckDB's memory: {err}")))?;
            // DuckDB reports an unlimited size when it cannot tell the machine's memory; that default is kept.
            let Some(bytes) = size_in_bytes(&default) else { return Ok(default) };
            format!("{}MiB", (bytes / 8 * 5) >> 20)
        }
    };
    connection
        .execute_batch(&sql::setting::set_memory_limit(&limit))
        .map_err(|err| PrepareError::Setup(format!("invalid memory limit '{limit}': {err}")))?;
    let limit = setting("memory_limit")
        .map_err(|err| PrepareError::Setup(format!("failed to read DuckDB's memory limit: {err}")))?;
    // A hash aggregate or join fails below a few tens of MB per thread, so a small limit runs fewer threads.
    if let Some(bytes) = size_in_bytes(&limit) {
        if bytes < MEMORY_FLOOR {
            return Err(PrepareError::Setup(format!(
                "the memory limit {limit} is too small: the conversion needs at least {} MiB",
                MEMORY_FLOOR >> 20
            )));
        }
        let threads: i64 = connection
            .query_row(&sql::setting::current("threads"), [], |row| row.get(0))
            .map_err(|err| PrepareError::Setup(format!("failed to read DuckDB's threads: {err}")))?;
        let affordable = i64::try_from(bytes / MEMORY_PER_THREAD).unwrap_or(i64::MAX).max(1);
        if affordable < threads {
            connection
                .execute_batch(&sql::setting::set_threads(affordable))
                .map_err(|err| PrepareError::Setup(format!("failed to set DuckDB's threads: {err}")))?;
        }
    }
    Ok(limit)
}

const MEMORY_PER_THREAD: u64 = 64 << 20;
const MEMORY_FLOOR: u64 = 256 << 20;

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
        limit_memory(&connection, Some("256MiB")).unwrap();
        let threads: i64 = connection.query_row("SELECT current_setting('threads')", [], |row| row.get(0)).unwrap();
        assert_eq!(threads, 4);
        assert!(matches!(limit_memory(&connection, Some("128MiB")), Err(PrepareError::Setup(message))
            if message.contains("at least 256 MiB")));
    }
}
