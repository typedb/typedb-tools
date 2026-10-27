/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs, io,
    path::{self, Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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

    let work_dir = match args.files_only.as_deref() {
        _ if args.dry_run => std::env::temp_dir(),
        Some(dir) => PathBuf::from(dir),
        None => staging_dir(&db_path),
    };
    let spill_dir = work_dir.join(format!(".{database}.{}.tmp", process::id()));
    let source = Source::DuckDb(db_path.clone());
    println!("Migrating from {source}");
    let migration = prepare(&source, &schema_text, &spill_dir);
    if args.dry_run {
        on_interrupt(vec![spill_dir], None);
        let started = Instant::now();
        let result = migration.run(io::sink(), VERSION.trim(), &original_database);
        drop(migration); // closing DuckDB removes its spill directory; exiting would not
        match result {
            Ok(checksums) => print_summary(&checksums, started),
            Err(message) => fatal_with(ExitCode::MigrationError, message),
        }
        println!("Nothing was written or loaded (--dry-run).");
        process::exit(0);
    }

    let data_path = output_data_path(&work_dir, &database, args.overwrite);
    let server = match args.files_only {
        Some(_) => None,
        None => Some(connect(args, &database).await),
    };
    let import_command = console_import_command(args, &database, schema_path, &data_path);
    let retry_hint = format!(
        "The prepared data file is kept: {}\nRetry the import without converting again with:\n  {import_command}",
        data_path.display()
    );
    let importing = on_interrupt(vec![spill_dir, partial_path(&data_path)], Some(retry_hint.clone()));
    if let Err(message) = write_data_file(migration, &data_path, &original_database) {
        fatal_with(ExitCode::MigrationError, message);
    }

    let Some((connection, driver)) = server else {
        println!("  Data file:      {}", data_path.display());
        println!("Import with: {import_command}");
        process::exit(0);
    };

    println!("Importing into '{database}' from {} (kept if the import fails)...", data_path.display());
    let started = Instant::now();
    importing.store(true, Ordering::SeqCst);
    match import(&connection, driver, &database, &schema_text, &data_path).await {
        Ok(()) => {
            remove_if_exists(&data_path);
            println!("Imported in {}.", format_duration(started.elapsed()));
            process::exit(0);
        }
        Err((code, message)) => {
            println_error!("{message}");
            println!("{retry_hint}");
            process::exit(code as i32);
        }
    }
}

const RECONNECT_ATTEMPTS: u32 = 12;
const RECONNECT_DELAY: Duration = Duration::from_secs(10);

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
        // DatabaseCreateError::IsBeingImported: the server refuses the name while it discards a broken-off import.
        typedb_driver::Error::Server(_) => err.code() == "DBC8",
        _ => false,
    }
}

fn prepare(source: &Source, schema_text: &str, spill_dir: &Path) -> Migration {
    let started = Instant::now();
    match Migration::prepare(source, schema_text, spill_dir) {
        Ok(migration) => {
            print_mapping(&migration.mapping_summary());
            print_warnings(migration.warnings());
            println!("Checked in {}.", format_duration(started.elapsed()));
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
    print_summary(&checksums, started);
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

// An interrupt during the import keeps the data file for a retry, as a failed import does; the server discards
// the broken-off import. The returned flag tells the handler that the import has started.
fn on_interrupt(leftovers: Vec<PathBuf>, retry_hint: Option<String>) -> Arc<AtomicBool> {
    let importing = Arc::new(AtomicBool::new(false));
    let mut signals = match Termination::listen() {
        Ok(signals) => signals,
        Err(err) => {
            println_warning!("interrupts will not be handled: {err}");
            return importing;
        }
    };
    let importing_signal = importing.clone();
    tokio::spawn(async move {
        let Some(exit_code) = signals.recv().await else { return };
        for path in &leftovers {
            remove_if_exists(path);
        }
        match retry_hint {
            Some(retry_hint) if importing_signal.load(Ordering::SeqCst) => {
                println_error!("interrupted during the import; the server discards the partial database");
                println!("{retry_hint}");
            }
            _ => println_error!("interrupted; nothing was loaded"),
        }
        process::exit(exit_code);
    });
    importing
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
    let name = args
        .database
        .clone()
        .or_else(|| db_path.file_stem().map(|stem| stem.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "loaded".to_owned());
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

fn print_summary(checksums: &Checksums, started: Instant) {
    println!("Converted in {}.", format_duration(started.elapsed()));
    println!("  Attributes:     {:>8}", checksums.attribute_count);
    println!("  Entities:       {:>8}", checksums.entity_count);
    println!("  Relations:      {:>8}", checksums.relation_count);
    println!("  Ownerships:     {:>8}", checksums.ownership_count);
    println!("  Role players:   {:>8}", checksums.role_count);
}
