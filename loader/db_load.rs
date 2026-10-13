/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs, io,
    path::{Path, PathBuf},
    time::Instant,
};

use typedb_cli_common::{build_tls_config, parse_addresses, println_error, println_warning};
use typedb_driver::{Credentials, DriverOptions, TypeDBDriver};
use typedb_migration::{Checksums, Diagnostic, Migration, PrepareError};

use crate::{ExitCode, cli::Args, fatal_with, progress::format_duration, startup::prompt_password_if_missing};

pub(crate) async fn run_db_load(args: &Args) -> ! {
    let db_path = PathBuf::from(args.db.as_deref().expect("dispatched on --db"));
    recognise_source(&db_path);
    let Some(schema_path) = args.schema_file.as_deref() else {
        fatal_with(ExitCode::UserInputError, "--schema-file is required when loading from a database file");
    };
    let schema_text = fs::read_to_string(schema_path).unwrap_or_else(|err| {
        fatal_with(ExitCode::UserInputError, format!("failed to read schema file '{schema_path}': {err}"))
    });
    let database = database_name(args, &db_path);

    let migration = prepare(&db_path, &schema_text);
    if args.dry_run {
        println!("Validation passed; nothing was loaded (--dry-run).");
        std::process::exit(0);
    }

    let driver = match args.files_only {
        Some(_) => None,
        None => Some(connect(args, &database).await),
    };

    let data_path = match args.files_only.as_deref() {
        Some(dir) => output_data_path(Path::new(dir), &database, args.overwrite),
        None => staging_data_path(&db_path, &database),
    };
    let mut interrupted_leftovers = vec![partial_path(&data_path)];
    if driver.is_some() {
        interrupted_leftovers.push(data_path.clone());
    }
    remove_on_interrupt(interrupted_leftovers);
    let original_database = format!("duckdb:{}", db_path.file_name().unwrap_or_default().to_string_lossy());
    write_data_file(&migration, &data_path, &original_database);

    let Some(driver) = driver else {
        println!("  Data file:      {}", data_path.display());
        println!(
            "Import with: typedb console --command=\"database import {database} {} {}\"",
            schema_path,
            data_path.display()
        );
        std::process::exit(0);
    };

    println!("Importing into '{database}'...");
    let started = Instant::now();
    let result = driver.databases().import_from_file(database.clone(), schema_text, &data_path).await;
    remove_if_exists(&data_path);
    match result {
        Ok(()) => {
            println!("Imported in {}.", format_duration(started.elapsed()));
            std::process::exit(0);
        }
        Err(err) => fatal_with(ExitCode::GeneralError, format!("import into '{database}' failed: {err}")),
    }
}

fn prepare(db_path: &Path, schema_text: &str) -> Migration {
    match Migration::prepare(db_path, schema_text) {
        Ok(migration) => {
            print_mapping(&migration.mapping_summary());
            print_warnings(migration.warnings());
            migration
        }
        Err(PrepareError::Setup(message)) => fatal_with(ExitCode::UserInputError, message),
        Err(PrepareError::Invalid { errors, warnings, mapping_summary }) => {
            print_mapping(&mapping_summary);
            print_warnings(&warnings);
            for error in &errors {
                println_error!("{}: {}", error.stage, error.message);
            }
            fatal_with(
                ExitCode::UserInputError,
                format!("validation failed with {} error(s); nothing was written", errors.len()),
            );
        }
    }
}

fn write_data_file(migration: &Migration, path: &Path, original_database: &str) {
    let partial = PartialFile(partial_path(path));
    let started = Instant::now();
    let result = fs::File::create(&partial.0)
        .map_err(|err| format!("failed to create '{}': {err}", partial.0.display()))
        .and_then(|file| migration.run(file, original_database))
        .and_then(|checksums| {
            fs::rename(&partial.0, path)
                .map_err(|err| format!("failed to move the data file into '{}': {err}", path.display()))?;
            Ok(checksums)
        });
    match result {
        Ok(checksums) => print_summary(&checksums, started),
        Err(message) => {
            drop(partial);
            fatal_with(ExitCode::MigrationError, message);
        }
    }
}

// TODO: Move to common? Use an existing crate?
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

fn remove_on_interrupt(paths: Vec<PathBuf>) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            for path in &paths {
                remove_if_exists(path);
            }
            println_error!("interrupted; nothing was loaded");
            std::process::exit(130);
        }
    });
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

fn staging_data_path(db_path: &Path, database: &str) -> PathBuf {
    let dir = match db_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    dir.join(format!(".{database}.{}.data.typedb", std::process::id()))
}

fn remove_if_exists(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => println_warning!("failed to remove '{}': {err}", path.display()),
    }
}

fn recognise_source(db_path: &Path) {
    if !db_path.is_file() {
        let problem = if db_path.exists() { "is not a file" } else { "does not exist" };
        fatal_with(ExitCode::UserInputError, format!("database file '{}' {problem}", db_path.display()));
    }
    let extension = db_path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    match extension.as_str() {
        "duckdb" | "ddb" | "db" => {}
        "csv" | "tsv" => {
            fatal_with(ExitCode::UserInputError, "CSV files are loaded with --data and a --query template, not --db")
        }
        "sqlite" | "sqlite3" | "parquet" | "json" => fatal_with(
            ExitCode::UserInputError,
            format!(
                "'.{extension}' files are not loaded directly: stage them into a DuckDB database first \
                 (see the loader's migration README)"
            ),
        ),
        _ => fatal_with(
            ExitCode::UserInputError,
            format!("unrecognised database file extension '.{extension}' (supported: .duckdb, .ddb, .db)"),
        ),
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

async fn connect(args: &Args, database: &str) -> TypeDBDriver {
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
    let driver = TypeDBDriver::new(addresses, Credentials::new(username, &password), DriverOptions::new(tls_config))
        .await
        .unwrap_or_else(|err| fatal_with(ExitCode::ConnectionError, format!("failed to connect to TypeDB: {err}")));
    match driver.databases().contains(database).await {
        Ok(false) => driver,
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

fn print_warnings(warnings: &[Diagnostic]) {
    for warning in warnings {
        println_warning!("{}: {}", warning.stage, warning.message);
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
