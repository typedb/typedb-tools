/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    future::Future,
    io::{self, Write},
    path::{self, Path, PathBuf},
    process,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use typedb_cli_common::{build_tls_config, format_error, parse_addresses, println_error, println_warning};
use typedb_driver::{Addresses, Credentials, DriverOptions, TypeDBDriver};
use typedb_migration::{Cancellation, Checksums, Migration, PrepareError, ProgressListener, ScanProgress, Source};

use crate::{
    ExitCode, VERSION,
    cli::Args,
    fatal_with,
    progress::format_duration,
    startup::{Termination, prompt_password_if_missing},
};

pub(crate) async fn run_migration(args: &Args) -> ! {
    let input = MigrationInput::read(args);
    let exit_code = match migrate(args, &input).await {
        Ok(()) => 0,
        Err(failure) => failure.report(),
    };
    process::exit(exit_code)
}

async fn migrate(args: &Args, input: &MigrationInput) -> Result<(), Failure> {
    let output = Output::prepare(args, input)?;
    let server = match &output {
        Output::DataFile(_) if !args.files_only => Some(connect(args, &input.database).await?),
        Output::DataFile(_) | Output::Discarded { .. } => None,
    };
    let mut interruptions = Interruptions::listen();
    say(format_args!("Migrating from {}", input.source()));
    let progress = Progress::new();
    let heartbeat = progress.keep_alive();
    let cancellation = Arc::new(Cancellation::default());
    let migration = prepare(args, input, output.spill_dir(), &progress, &cancellation, &mut interruptions).await?;
    let (checksums, elapsed) = convert(migration, input, &output, &progress, &cancellation, &mut interruptions).await?;
    drop(heartbeat);
    print_summary(&checksums, elapsed);

    let data_file = match output {
        Output::Discarded { .. } => {
            say(format_args!("Nothing was written or loaded (--dry-run)."));
            return Ok(());
        }
        Output::DataFile(data_file) => data_file,
    };
    let import_command = console_import_command(args, &input.database, &input.schema_path, &data_file.path);
    let Some((connection, driver)) = server else {
        say(format_args!("  Data file:      {}", data_file.path.display()));
        say(format_args!("Import with: {import_command}"));
        return Ok(());
    };
    let kept_hint = format!(
        "The prepared data file is kept: {}\nImport it without converting again with:\n  {import_command}",
        data_file.path.display()
    );
    let kept = |failure: Failure| Failure { kept_hint: Some(kept_hint.clone()), ..failure };
    let database = &input.database;
    say(format_args!("Importing into '{database}' from {} (kept if the import fails)...", data_file.path.display()));
    let started = Instant::now();
    let imported = interruptions
        .guard(import(&connection, driver, database, &input.schema_text, &data_file.path))
        .await
        .map_err(|signal| kept(Failure::interrupted_import(signal)))?;
    match imported.map_err(kept)? {
        Imported::Confirmed => {
            remove_if_exists(&data_file.path);
            say(format_args!("Imported in {}.", format_duration(started.elapsed())));
            Ok(())
        }
        Imported::Unconfirmed => Err(kept(Failure::new(
            ExitCode::ConnectionError,
            format!(
                "'{database}' exists, possibly imported by an attempt of this run that lost its connection; the data \
                 file is kept until you have checked the database"
            ),
        ))),
    }
}

struct MigrationInput {
    db_path: PathBuf,
    schema_path: String,
    schema_text: String,
    database: String,
    original_database: String,
}

impl MigrationInput {
    fn read(args: &Args) -> MigrationInput {
        let db_path = PathBuf::from(args.duckdb.as_deref().expect("dispatched on --duckdb"));
        check_source_file(&db_path);
        let Some(schema_path) = args.schema_file.clone() else {
            fatal_with(ExitCode::UserInputError, "--schema-file is required with --duckdb");
        };
        let schema_text = fs::read_to_string(&schema_path).unwrap_or_else(|err| {
            fatal_with(ExitCode::UserInputError, format!("failed to read schema file '{schema_path}': {err}"))
        });
        let database = database_name(args, &db_path);
        let original_database = format!("duckdb:{}", db_path.file_name().unwrap_or_default().to_string_lossy());
        MigrationInput { db_path, schema_path, schema_text, database, original_database }
    }

    fn source(&self) -> Source {
        Source::DuckDb(self.db_path.clone())
    }
}

fn check_source_file(db_path: &Path) {
    if !db_path.is_file() {
        let problem = if db_path.exists() { "is not a file" } else { "does not exist" };
        fatal_with(ExitCode::UserInputError, format!("DuckDB file '{}' {problem}", db_path.display()));
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

// A dry run spills into the system's temporary directory.
enum Output {
    Discarded { spill_dir: PathBuf },
    DataFile(DataFileOutput),
}

struct DataFileOutput {
    path: PathBuf,
    spill_dir: PathBuf,
    _lock: OutputLock,
}

impl Output {
    fn prepare(args: &Args, input: &MigrationInput) -> Result<Output, Failure> {
        if args.dry_run {
            let spill_dir = std::env::temp_dir().join(format!(".typedb-loader.{}.tmp", process::id()));
            return Ok(Output::Discarded { spill_dir });
        }
        let work_dir = match args.output_dir.as_deref() {
            Some(dir) => PathBuf::from(if dir.is_empty() { "." } else { dir }),
            None => staging_dir(&input.db_path),
        };
        fs::create_dir_all(&work_dir).map_err(|err| {
            let message = format!("failed to create output directory '{}': {err}", work_dir.display());
            Failure::new(ExitCode::MigrationError, message)
        })?;
        let lock = OutputLock::take(&work_dir, &input.database)?;
        let path = output_data_path(&work_dir, &input.database, args.overwrite)?;
        let spill_dir = work_dir.join(format!(".{}.tmp", input.database));
        remove_spill_left_behind(&spill_dir)?;
        remove_if_exists(&partial_path(&path));
        Ok(Output::DataFile(DataFileOutput { path, spill_dir, _lock: lock }))
    }

    fn spill_dir(&self) -> &Path {
        match self {
            Output::Discarded { spill_dir } | Output::DataFile(DataFileOutput { spill_dir, .. }) => spill_dir,
        }
    }
}

fn staging_dir(db_path: &Path) -> PathBuf {
    match db_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

// One run per database and directory owns the data file, partial file and spill directory; a dead run's are stale.
struct OutputLock {
    path: PathBuf,
    _file: fs::File,
}

impl OutputLock {
    fn take(dir: &Path, database: &str) -> Result<OutputLock, Failure> {
        let path = dir.join(format!(".{database}.lock"));
        let failed = |err: io::Error| {
            let message = format!(
                "failed to create '{}': {err} (files are written next to the DuckDB file, or in --output-dir)",
                path.display()
            );
            Failure::new(ExitCode::UserInputError, message)
        };
        loop {
            let file = fs::OpenOptions::new().create(true).truncate(false).write(true).open(&path).map_err(failed)?;
            match file.try_lock() {
                Ok(()) => {}
                Err(fs::TryLockError::WouldBlock) => {
                    let message =
                        format!("another loader is preparing or importing '{database}' in '{}'", dir.display());
                    return Err(Failure::new(ExitCode::UserInputError, message));
                }
                Err(fs::TryLockError::Error(err)) => return Err(failed(err)),
            }
            // A run removes its lock file as it ends, and a file opened before the removal is no lock any more.
            if is_at(&file, &path) {
                return Ok(OutputLock { path, _file: file });
            }
        }
    }
}

// Where a removed file cannot be told from the file at its path, the lock file stays.
impl Drop for OutputLock {
    fn drop(&mut self) {
        if cfg!(unix) {
            remove_if_exists(&self.path);
        }
    }
}

#[cfg(unix)]
fn is_at(file: &fs::File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (file.metadata(), fs::metadata(path)) {
        (Ok(opened), Ok(current)) => (opened.dev(), opened.ino()) == (current.dev(), current.ino()),
        _ => false,
    }
}

#[cfg(not(unix))]
fn is_at(_: &fs::File, _: &Path) -> bool {
    true
}

fn output_data_path(dir: &Path, database: &str, overwrite: bool) -> Result<PathBuf, Failure> {
    let path = dir.join(format!("{database}.data.typedb"));
    if !overwrite && path.exists() {
        let message = format!("'{}' already exists; pass --overwrite to replace it", path.display());
        return Err(Failure::new(ExitCode::UserInputError, message));
    }
    Ok(path)
}

// DuckDB names its spill files `duckdb_temp_*`; anything else in the directory is not the loader's to remove.
fn remove_spill_left_behind(spill_dir: &Path) -> Result<(), Failure> {
    let Ok(metadata) = fs::symlink_metadata(spill_dir) else { return Ok(()) };
    if metadata.is_dir()
        && let Ok(entries) = fs::read_dir(spill_dir)
    {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("duckdb_temp_") {
                remove_if_exists(&entry.path());
            }
        }
    }
    fs::remove_dir(spill_dir).map_err(|err| {
        let message = format!("'{}' is left from an earlier run and cannot be removed: {err}", spill_dir.display());
        Failure::new(ExitCode::UserInputError, message)
    })
}

fn remove_if_exists(path: &Path) {
    let removed = if path.is_dir() { fs::remove_dir_all(path) } else { fs::remove_file(path) };
    match removed {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            let warning = typedb_cli_common::format_warning!("warning:");
            let _ = writeln!(io::stderr(), "{warning} failed to remove '{}': {err}", path.display());
        }
    }
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

async fn connect(args: &Args, database: &str) -> Result<(ConnectionSettings, TypeDBDriver), Failure> {
    let input_error = |message: String| Failure::new(ExitCode::UserInputError, message);
    let Some(addresses) = args.addresses.as_deref() else {
        return Err(input_error(
            "--address is required to load into a server (use --files-only to only write the data file)".to_owned(),
        ));
    };
    let Some(username) = args.username.as_deref() else {
        return Err(input_error("--username is required to load into a server".to_owned()));
    };
    let password = prompt_password_if_missing(args, username);
    let addresses = parse_addresses(addresses).map_err(input_error)?;
    let tls_config = build_tls_config(args.tls_disabled.unwrap_or(false), args.tls_root_ca.as_deref().map(Path::new))
        .map_err(input_error)?;
    let connection = ConnectionSettings {
        addresses,
        credentials: Credentials::new(username, &password),
        options: DriverOptions::new(tls_config),
    };
    let driver = connection
        .connect()
        .await
        .map_err(|err| Failure::new(ExitCode::ConnectionError, format!("failed to connect to TypeDB: {err}")))?;
    match driver.databases().contains(database).await {
        Ok(false) => Ok((connection, driver)),
        Ok(true) => Err(input_error(format!(
            "database '{database}' already exists; the import creates it, so choose another --database"
        ))),
        Err(err) => Err(Failure::new(ExitCode::ConnectionError, format!("failed to list databases: {err}"))),
    }
}

struct Failure {
    exit_code: i32,
    message: String,
    kept_hint: Option<String>,
}

impl Failure {
    fn new(code: ExitCode, message: impl Into<String>) -> Failure {
        Failure { exit_code: code as i32, message: message.into(), kept_hint: None }
    }

    fn interrupted(signal: i32) -> Failure {
        Failure { exit_code: signal, message: "interrupted; nothing was loaded".to_owned(), kept_hint: None }
    }

    fn interrupted_import(signal: i32) -> Failure {
        let message = "interrupted before the import completed; the server discards a partial import".to_owned();
        Failure { exit_code: signal, message, kept_hint: None }
    }

    // Writes that fail are ignored: the terminal or the pipe may be gone.
    fn report(self) -> i32 {
        let _ = writeln!(io::stderr(), "{} {}", format_error!("error:"), self.message);
        if let Some(kept_hint) = self.kept_hint {
            say(format_args!("{kept_hint}"));
        }
        self.exit_code
    }
}

// SIGINT or SIGTERM, as the exit code a shell reports for each. Without signal handling, nothing arrives.
struct Interruptions(Option<Termination>);

impl Interruptions {
    fn listen() -> Self {
        match Termination::listen() {
            Ok(signals) => Interruptions(Some(signals)),
            Err(err) => {
                println_warning!("interrupts will not be handled: {err}");
                Interruptions(None)
            }
        }
    }

    async fn guard<T>(&mut self, work: impl Future<Output = T>) -> Result<T, i32> {
        tokio::select! {
            result = work => Ok(result),
            signal = self.next() => Err(signal),
        }
    }

    // Cancels and awaits the work, interrupting again as DuckDB forgets it between queries; a second signal exits.
    async fn guard_blocking<T: Send + 'static>(
        &mut self,
        cancellation: &Cancellation,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, (i32, T)> {
        let mut task = tokio::task::spawn_blocking(work);
        let signal = tokio::select! {
            outcome = &mut task => return Ok(joined(outcome)),
            signal = self.next() => signal,
        };
        let mut repeat = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                outcome = &mut task => return Err((signal, joined(outcome))),
                _ = repeat.tick() => cancellation.cancel(),
                second = self.next() => {
                    let _ = writeln!(io::stderr(), "Force-exiting.");
                    process::exit(second)
                }
            }
        }
    }

    async fn next(&mut self) -> i32 {
        match &mut self.0 {
            Some(signals) => match signals.recv().await {
                Some(signal) => signal,
                None => std::future::pending().await,
            },
            None => std::future::pending().await,
        }
    }
}

fn joined<T>(outcome: Result<T, tokio::task::JoinError>) -> T {
    outcome.unwrap_or_else(|err| std::panic::resume_unwind(err.into_panic()))
}

async fn prepare(
    args: &Args,
    input: &MigrationInput,
    spill_dir: &Path,
    progress: &Progress,
    cancellation: &Arc<Cancellation>,
    interruptions: &mut Interruptions,
) -> Result<Migration, Failure> {
    let (source, schema_text) = (input.source(), input.schema_text.clone());
    let (spill_dir, memory_limit, task_cancellation) =
        (spill_dir.to_path_buf(), args.memory_limit.clone(), cancellation.clone());
    let started = Instant::now();
    let prepared = interruptions
        .guard_blocking(cancellation, move || {
            Migration::prepare(&source, &schema_text, &spill_dir, memory_limit.as_deref(), task_cancellation)
        })
        .await
        .map_err(|(signal, _)| Failure::interrupted(signal))?;
    progress.mark_reported();
    match prepared {
        Ok(migration) => {
            print_mapping(&migration.mapping_summary());
            print_warnings(migration.warnings());
            say(format_args!(
                "Checked in {} (source DB memory limit {}).",
                format_duration(started.elapsed()),
                migration.memory_limit()
            ));
            Ok(migration)
        }
        Err(PrepareError::Setup(message)) => Err(Failure::new(ExitCode::UserInputError, message)),
        Err(PrepareError::Invalid { errors, warnings, mapping_summary }) => {
            print_mapping(&mapping_summary);
            print_warnings(&warnings);
            for error in &errors {
                println_error!("{error}");
            }
            let message = format!("validation failed with {} error(s); nothing was written", errors.len());
            Err(Failure::new(ExitCode::UserInputError, message))
        }
        Err(PrepareError::Cancelled) => Err(Failure::new(ExitCode::GeneralError, "the checks were cancelled")),
    }
}

// The migration is consumed, so that DuckDB is closed and its spill directory removed before the caller goes on.
async fn convert(
    migration: Migration,
    input: &MigrationInput,
    output: &Output,
    progress: &Progress,
    cancellation: &Cancellation,
    interruptions: &mut Interruptions,
) -> Result<(Checksums, Duration), Failure> {
    let data_path = match output {
        Output::Discarded { .. } => None,
        Output::DataFile(data_file) => Some(data_file.path.clone()),
    };
    let (original_database, progress) = (input.original_database.clone(), progress.clone());
    let data_file = data_path.clone();
    let converted = interruptions
        .guard_blocking(cancellation, move || {
            let started = Instant::now();
            let checksums = match &data_file {
                None => migration.run(io::sink(), VERSION.trim(), &original_database, &progress),
                Some(path) => write_data_file(&migration, path, &original_database, &progress),
            };
            drop(migration);
            checksums.map(|checksums| (checksums, started.elapsed()))
        })
        .await;
    match (converted, data_path) {
        (Ok(converted), _) => converted.map_err(|message| Failure::new(ExitCode::MigrationError, message)),
        (Err((signal, Ok(_))), Some(path)) => Err(Failure {
            exit_code: signal,
            message: format!(
                "interrupted after the conversion, before anything was loaded: the data file is complete at {}",
                path.display()
            ),
            kept_hint: None,
        }),
        (Err((signal, _)), _) => Err(Failure::interrupted(signal)),
    }
}

fn write_data_file(
    migration: &Migration,
    path: &Path,
    original_database: &str,
    progress: &Progress,
) -> Result<Checksums, String> {
    let partial_path = partial_path(path);
    // Exclusively, so that nothing put at the path since the sweep is written through, or removed.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial_path)
        .map_err(|err| format!("failed to create '{}': {err}", partial_path.display()))?;
    let partial = PartialFile(partial_path);
    let checksums = migration.run(&mut file, VERSION.trim(), original_database, progress)?;
    file.sync_all().map_err(|err| format!("failed to write '{}': {err}", partial.0.display()))?;
    fs::rename(&partial.0, path)
        .map_err(|err| format!("failed to move the data file into '{}': {err}", path.display()))?;
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        fs::File::open(dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| format!("failed to write '{}': {err}", dir.display()))?;
    }
    Ok(checksums)
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

#[derive(Clone)]
struct Progress {
    started: Instant,
    last_reported: Arc<Mutex<Instant>>,
}

impl Progress {
    const QUIET: Duration = Duration::from_secs(60);

    fn new() -> Progress {
        let now = Instant::now();
        Progress { started: now, last_reported: Arc::new(Mutex::new(now)) }
    }

    // Checks and sorts report nothing for up to hours: whenever nothing was reported for a while, says it works.
    fn keep_alive(&self) -> Heartbeat {
        let progress = self.clone();
        Heartbeat(tokio::spawn(async move {
            loop {
                let last_reported = *progress.last_reported();
                tokio::time::sleep_until((last_reported + Progress::QUIET).into()).await;
                if *progress.last_reported() == last_reported {
                    progress.say("working");
                }
            }
        }))
    }

    fn mark_reported(&self) {
        *self.last_reported() = Instant::now();
    }

    fn say(&self, message: &str) {
        self.mark_reported();
        say(format_args!("  {message} ({} elapsed)", format_duration(self.started.elapsed())));
    }

    fn last_reported(&self) -> MutexGuard<'_, Instant> {
        self.last_reported.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ProgressListener for Progress {
    fn report(&self, progress: &ScanProgress<'_>) {
        self.say(&progress.to_string());
    }
}

struct Heartbeat(tokio::task::JoinHandle<()>);

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// Writes that fail are ignored: the terminal or the pipe may be gone.
fn say(line: std::fmt::Arguments<'_>) {
    let _ = writeln!(io::stdout(), "{line}");
}

const RECONNECT_ATTEMPTS: u32 = 12;
const RECONNECT_DELAY: Duration = Duration::from_secs(10);

const ALREADY_EXISTS: &str = "DBC6";
const IS_BEING_IMPORTED: &str = "DBC8";

enum Imported {
    Confirmed,
    // The database exists after an attempt of this run lost its connection: imported by it, or by another run.
    Unconfirmed,
}

async fn import(
    connection: &ConnectionSettings,
    driver: TypeDBDriver,
    database: &str,
    schema: &str,
    data_path: &Path,
) -> Result<Imported, Failure> {
    let mut driver = Ok(driver);
    let mut retries = 0;
    let mut has_broken_off = false;
    loop {
        let (result, is_attempted) = match driver {
            Ok(driver) => (driver.databases().import_from_file(database, schema, data_path).await, true),
            Err(err) => (Err(err), false),
        };
        match result {
            Ok(()) => return Ok(Imported::Confirmed),
            Err(err) if has_broken_off && err.code() == ALREADY_EXISTS => return Ok(Imported::Unconfirmed),
            Err(err) if retries < RECONNECT_ATTEMPTS && is_transient(&err, has_broken_off) => {
                has_broken_off |= is_attempted && matches!(err, typedb_driver::Error::Connection(_));
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
                let cause = match err.code().as_str() {
                    IS_BEING_IMPORTED if has_broken_off => {
                        format!("an import of '{database}', possibly this run's broken-off one, is still in progress")
                    }
                    IS_BEING_IMPORTED => format!("another import of '{database}' is in progress"),
                    ALREADY_EXISTS => format!("'{database}' was created by another import meanwhile"),
                    _ => err.to_string(),
                };
                return Err(Failure::new(code, format!("import into '{database}' failed: {cause}")));
            }
        }
    }
}

fn is_transient(err: &typedb_driver::Error, has_broken_off: bool) -> bool {
    match err {
        typedb_driver::Error::Connection(_) => true,
        // The server refuses the name while it completes or discards this run's broken-off import.
        typedb_driver::Error::Server(_) => has_broken_off && err.code() == IS_BEING_IMPORTED,
        _ => false,
    }
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

fn print_mapping(lines: &[String]) {
    say(format_args!("Mapping:"));
    for line in lines {
        say(format_args!("  {line}"));
    }
}

fn print_warnings(warnings: &[String]) {
    for warning in warnings {
        println_warning!("{warning}");
    }
}

fn print_summary(checksums: &Checksums, elapsed: Duration) {
    say(format_args!("Converted in {}.", format_duration(elapsed)));
    say(format_args!("  Attributes:     {:>8}", checksums.attribute_count));
    say(format_args!("  Entities:       {:>8}", checksums.entity_count));
    say(format_args!("  Relations:      {:>8}", checksums.relation_count));
    say(format_args!("  Ownerships:     {:>8}", checksums.ownership_count));
    say(format_args!("  Role players:   {:>8}", checksums.role_count));
}
