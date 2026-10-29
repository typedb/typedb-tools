/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    io::{self, Write},
    path::{self, Path, PathBuf},
    process,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use typedb_cli_common::{build_tls_config, parse_addresses, println_error, println_warning};
use typedb_driver::{Addresses, Credentials, DriverOptions, TypeDBDriver};
use typedb_migration::{Checksums, Migration, PrepareError, Source};

use crate::{
    ExitCode, VERSION,
    cli::Args,
    fatal_with,
    progress::format_duration,
    startup::{Termination, prompt_password_if_missing},
};

pub(crate) async fn run_migration(args: &Args) -> ! {
    let db_path = PathBuf::from(args.duckdb.as_deref().expect("dispatched on --duckdb"));
    check_source_file(&db_path);
    let Some(schema_path) = args.schema_file.as_deref() else {
        fatal_with(ExitCode::UserInputError, "--schema-file is required with --duckdb");
    };
    let schema_text = fs::read_to_string(schema_path).unwrap_or_else(|err| {
        fatal_with(ExitCode::UserInputError, format!("failed to read schema file '{schema_path}': {err}"))
    });
    let database = database_name(args, &db_path);
    let original_database = format!("duckdb:{}", db_path.file_name().unwrap_or_default().to_string_lossy());

    let work_dir = match args.output_dir.as_deref() {
        _ if args.dry_run => std::env::temp_dir(),
        Some(dir) => PathBuf::from(dir),
        None => staging_dir(&db_path),
    };
    let spill_dir = work_dir.join(format!(".{database}.{}.tmp", process::id()));
    let source = Source::DuckDb(db_path.clone());
    println!("Migrating from {source}");
    let cleanup = on_interrupt(spill_dir.clone());
    let migration = {
        let schema_text = schema_text.clone();
        let memory_limit = args.memory_limit.clone();
        blocking(move || prepare(&source, &schema_text, &spill_dir, memory_limit.as_deref())).await
    };
    if args.dry_run {
        let original_database = original_database.clone();
        let (result, elapsed) = blocking(move || {
            let started = Instant::now();
            let result = migration.run(io::sink(), VERSION.trim(), &original_database);
            drop(migration); // closes DuckDB and removes its spill directory; exiting would not
            (result, started.elapsed())
        })
        .await;
        match result {
            Ok(checksums) => print_summary(&checksums, elapsed),
            Err(message) => fatal_with(ExitCode::MigrationError, message),
        }
        println!("Nothing was written or loaded (--dry-run).");
        process::exit(0);
    }

    let data_path = output_data_path(&work_dir, &database, args.overwrite);
    let server = if args.files_only { None } else { Some(connect(args, &database).await) };
    let import_command = console_import_command(args, &database, schema_path, &data_path);
    let kept_hint = format!(
        "The prepared data file is kept: {}\nImport it without converting again with:\n  {import_command}",
        data_path.display()
    );
    lock(&cleanup).leftovers.push(partial_path(&data_path));
    let written = {
        let data_path = data_path.clone();
        blocking(move || write_data_file(migration, &data_path, &original_database)).await
    };
    if let Err(message) = written {
        fatal_with(ExitCode::MigrationError, message);
    }
    lock(&cleanup).phase = Phase::Converted { kept_hint: kept_hint.clone() };

    let Some((connection, driver)) = server else {
        println!("  Data file:      {}", data_path.display());
        println!("Import with: {import_command}");
        process::exit(0);
    };

    println!("Importing into '{database}' from {} (kept if the import fails)...", data_path.display());
    let started = Instant::now();
    lock(&cleanup).phase = Phase::Importing { kept_hint: kept_hint.clone() };
    match import(&connection, driver, &database, &schema_text, &data_path).await {
        Ok(()) => {
            remove_if_exists(&data_path);
            println!("Imported in {}.", format_duration(started.elapsed()));
            process::exit(0);
        }
        Err((code, message)) => {
            println_error!("{message}");
            println!("{kept_hint}");
            process::exit(code as i32);
        }
    }
}

// The conversion is CPU- and IO-bound for up to hours; it runs off the runtime's workers, which serve the
// interrupt handler.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.unwrap_or_else(|err| std::panic::resume_unwind(err.into_panic()))
}

const RECONNECT_ATTEMPTS: u32 = 12;
const RECONNECT_DELAY: Duration = Duration::from_secs(10);

// The driver reports server errors by code only: DatabaseCreateError::AlreadyExists and ::IsBeingImported.
const ALREADY_EXISTS: &str = "DBC6";
const IS_BEING_IMPORTED: &str = "DBC8";

// The conversion before the import can take hours; a connection that did not survive them is reopened.
async fn import(
    connection: &ConnectionSettings,
    driver: TypeDBDriver,
    database: &str,
    schema: &str,
    data_path: &Path,
) -> Result<(), (ExitCode, String)> {
    let mut driver = Ok(driver);
    let mut retries = 0;
    loop {
        let result = match driver {
            Ok(driver) => driver.databases().import_from_file(database, schema, data_path).await,
            Err(err) => Err(err),
        };
        match result {
            Ok(()) => return Ok(()),
            // The connection was lost after the server had completed an earlier attempt.
            Err(err) if retries > 0 && err.code() == ALREADY_EXISTS => {
                println_warning!("'{database}' exists: the import completed before the connection was lost");
                return Ok(());
            }
            Err(err) if retries < RECONNECT_ATTEMPTS && is_transient(&err) => {
                retries += 1;
                let delay = RECONNECT_DELAY.as_secs();
                println_warning!("{err}; retrying in {delay}s (attempt {retries} of {RECONNECT_ATTEMPTS})");
                tokio::time::sleep(RECONNECT_DELAY).await;
                driver = connection.connect().await;
            }
            Err(err) => {
                let code = match err {
                    typedb_driver::Error::Connection(_) => ExitCode::ConnectionError,
                    _ => ExitCode::GeneralError,
                };
                return Err((code, format!("import into '{database}' failed: {err}")));
            }
        }
    }
}

fn is_transient(err: &typedb_driver::Error) -> bool {
    match err {
        typedb_driver::Error::Connection(_) => true,
        // The server refuses the name while it discards a broken-off import.
        typedb_driver::Error::Server(_) => err.code() == IS_BEING_IMPORTED,
        _ => false,
    }
}

fn prepare(source: &Source, schema_text: &str, spill_dir: &Path, memory_limit: Option<&str>) -> Migration {
    let started = Instant::now();
    match Migration::prepare(source, schema_text, spill_dir, memory_limit) {
        Ok(migration) => {
            print_mapping(&migration.mapping_summary());
            print_warnings(migration.warnings());
            println!(
                "Checked in {} (DuckDB memory limit {}).",
                format_duration(started.elapsed()),
                migration.memory_limit()
            );
            migration
        }
        Err(PrepareError::Setup(message)) => fatal_with(ExitCode::UserInputError, message),
        Err(PrepareError::Invalid { errors, warnings, mapping_summary }) => {
            print_mapping(&mapping_summary);
            print_warnings(&warnings);
            for error in &errors {
                println_error!("{error}");
            }
            fatal_with(
                ExitCode::UserInputError,
                format!("validation failed with {} error(s); nothing was written", errors.len()),
            );
        }
    }
}

// Consumes the migration so that DuckDB is closed, and its spill directory removed, before the caller exits.
fn write_data_file(migration: Migration, path: &Path, original_database: &str) -> Result<(), String> {
    let partial = PartialFile(partial_path(path));
    let started = Instant::now();
    let mut file =
        fs::File::create(&partial.0).map_err(|err| format!("failed to create '{}': {err}", partial.0.display()))?;
    let checksums = migration.run(&mut file, VERSION.trim(), original_database)?;
    file.sync_all().map_err(|err| format!("failed to write '{}': {err}", partial.0.display()))?;
    fs::rename(&partial.0, path)
        .map_err(|err| format!("failed to move the data file into '{}': {err}", path.display()))?;
    print_summary(&checksums, started.elapsed());
    Ok(())
}

struct PartialFile(PathBuf);

impl Drop for PartialFile {
    fn drop(&mut self) {
        remove_if_exists(&self.0);
    }
}

fn partial_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().expect("data paths are built with a file name").to_string_lossy();
    path.with_file_name(format!(".{}.partial", file_name.trim_start_matches('.')))
}

// Paths are made absolute so that the command can be run from anywhere.
fn console_import_command(args: &Args, database: &str, schema_path: &str, data_path: &Path) -> String {
    let mut command = String::from("typedb console");
    if let Some(addresses) = &args.addresses {
        command += &format!(" --address {addresses}");
    }
    if let Some(username) = &args.username {
        command += &format!(" --username {username}");
    }
    if args.tls_disabled.unwrap_or(false) {
        command += " --tls-disabled";
    }
    if let Some(root_ca) = &args.tls_root_ca {
        command += &format!(" --tls-root-ca {root_ca}");
    }
    let schema_path = path::absolute(schema_path).unwrap_or_else(|_| PathBuf::from(schema_path));
    let data_path = path::absolute(data_path).unwrap_or_else(|_| data_path.to_path_buf());
    format!("{command} --command=\"database import {database} {} {}\"", schema_path.display(), data_path.display())
}

// What an interrupt removes grows as the run proceeds: the spill directory from the start, the partial data file
// once the conversion writes. A finished data file is kept, also when the import is interrupted: the server
// discards the broken-off import.
struct Cleanup {
    leftovers: Vec<PathBuf>,
    phase: Phase,
}

enum Phase {
    Converting,
    Converted { kept_hint: String },
    Importing { kept_hint: String },
}

fn lock(cleanup: &Mutex<Cleanup>) -> MutexGuard<'_, Cleanup> {
    cleanup.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn on_interrupt(spill_dir: PathBuf) -> Arc<Mutex<Cleanup>> {
    let cleanup = Arc::new(Mutex::new(Cleanup { leftovers: vec![spill_dir], phase: Phase::Converting }));
    let mut signals = match Termination::listen() {
        Ok(signals) => signals,
        Err(err) => {
            println_warning!("interrupts will not be handled: {err}");
            return cleanup;
        }
    };
    let state = cleanup.clone();
    tokio::spawn(async move {
        let Some(exit_code) = signals.recv().await else { return };
        let cleanup = lock(&state);
        for path in &cleanup.leftovers {
            remove_if_exists(path);
        }
        match &cleanup.phase {
            Phase::Converting => println_error!("interrupted; nothing was loaded"),
            Phase::Converted { kept_hint } => {
                println_error!("interrupted before the import; nothing was loaded");
                println!("{kept_hint}");
            }
            Phase::Importing { kept_hint } => {
                println_error!("interrupted during the import; the server discards the partial database");
                println!("{kept_hint}");
            }
        }
        exit_now(exit_code);
    });
    cleanup
}

// The conversion's DuckDB threads may still be running: `process::exit` would run C++ static destructors under
// them ("pure virtual method called"), so the process ends without running them.
fn exit_now(code: i32) -> ! {
    unsafe extern "C" {
        fn _exit(status: std::ffi::c_int) -> !;
    }
    let _ = io::stdout().flush();
    unsafe { _exit(code) }
}

fn output_data_path(dir: &Path, database: &str, overwrite: bool) -> PathBuf {
    fs::create_dir_all(dir).unwrap_or_else(|err| {
        fatal_with(ExitCode::MigrationError, format!("failed to create output directory '{}': {err}", dir.display()))
    });
    let path = dir.join(format!("{database}.data.typedb"));
    if !overwrite && path.exists() {
        fatal_with(
            ExitCode::UserInputError,
            format!("'{}' already exists; pass --overwrite to replace it", path.display()),
        );
    }
    path
}

fn staging_dir(db_path: &Path) -> PathBuf {
    match db_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn remove_if_exists(path: &Path) {
    let removed = if path.is_dir() { fs::remove_dir_all(path) } else { fs::remove_file(path) };
    match removed {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => println_warning!("failed to remove '{}': {err}", path.display()),
    }
}

fn check_source_file(db_path: &Path) {
    if !db_path.is_file() {
        let problem = if db_path.exists() { "is not a file" } else { "does not exist" };
        fatal_with(ExitCode::UserInputError, format!("DuckDB file '{}' {problem}", db_path.display()));
    }
    let extension = db_path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    if matches!(extension.as_str(), "csv" | "tsv") {
        fatal_with(ExitCode::UserInputError, "CSV files are loaded with --data and a --query template, not --duckdb");
    }
}

fn database_name(args: &Args, db_path: &Path) -> String {
    let name = args.database.clone().unwrap_or_else(|| {
        let stem = db_path.file_stem().expect("the source was checked to be a file, which has a name");
        stem.to_string_lossy().into_owned()
    });
    if !args.dry_run && !typeql::common::identifier::is_valid_label(&name) {
        let origin = if args.database.is_some() { "" } else { " (taken from the file name; pass --database)" };
        fatal_with(ExitCode::UserInputError, format!("'{name}' is not a valid TypeDB database name{origin}"));
    }
    name
}

struct ConnectionSettings {
    addresses: Addresses,
    credentials: Credentials,
    options: DriverOptions,
}

impl ConnectionSettings {
    async fn connect(&self) -> typedb_driver::Result<TypeDBDriver> {
        TypeDBDriver::new(self.addresses.clone(), self.credentials.clone(), self.options.clone()).await
    }
}

async fn connect(args: &Args, database: &str) -> (ConnectionSettings, TypeDBDriver) {
    let Some(addresses) = args.addresses.as_deref() else {
        fatal_with(
            ExitCode::UserInputError,
            "--address is required to load into a server (use --files-only to only write the data file)",
        );
    };
    let Some(username) = args.username.as_deref() else {
        fatal_with(ExitCode::UserInputError, "--username is required to load into a server");
    };
    let password = prompt_password_if_missing(args, username);
    let addresses = parse_addresses(addresses).unwrap_or_else(|err| fatal_with(ExitCode::UserInputError, err));
    let tls_config = build_tls_config(args.tls_disabled.unwrap_or(false), args.tls_root_ca.as_deref().map(Path::new))
        .unwrap_or_else(|err| fatal_with(ExitCode::UserInputError, err));
    let connection = ConnectionSettings {
        addresses,
        credentials: Credentials::new(username, &password),
        options: DriverOptions::new(tls_config),
    };
    let driver = connection
        .connect()
        .await
        .unwrap_or_else(|err| fatal_with(ExitCode::ConnectionError, format!("failed to connect to TypeDB: {err}")));
    match driver.databases().contains(database).await {
        Ok(false) => (connection, driver),
        Ok(true) => fatal_with(
            ExitCode::UserInputError,
            format!("database '{database}' already exists; the import creates it, so choose another --database"),
        ),
        Err(err) => fatal_with(ExitCode::ConnectionError, format!("failed to list databases: {err}")),
    }
}

fn print_mapping(lines: &[String]) {
    println!("Mapping:");
    for line in lines {
        println!("  {line}");
    }
}

fn print_warnings(warnings: &[String]) {
    for warning in warnings {
        println_warning!("{warning}");
    }
}

fn print_summary(checksums: &Checksums, elapsed: Duration) {
    println!("Converted in {}.", format_duration(elapsed));
    println!("  Attributes:     {:>8}", checksums.attribute_count);
    println!("  Entities:       {:>8}", checksums.entity_count);
    println!("  Relations:      {:>8}", checksums.relation_count);
    println!("  Ownerships:     {:>8}", checksums.ownership_count);
    println!("  Role players:   {:>8}", checksums.role_count);
}
