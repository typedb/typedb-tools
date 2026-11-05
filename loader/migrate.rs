/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    future::Future,
    io,
    path::{self, Path, PathBuf},
    process,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use typedb_cli_common::{build_tls_config, parse_addresses, println_error, println_warning, try_println};
use typedb_driver::{Addresses, Credentials, DriverOptions, TypeDBDriver};
use typedb_migration::{Cancellation, Checksums, Migration, PrepareError, ProgressListener, ScanProgress};

use crate::{
    ExitCode, VERSION,
    cli::Args,
    fatal_with,
    progress::format_duration,
    startup::{FORCE_EXIT_MESSAGE, Termination, prompt_password_if_missing},
};

const INTERRUPT_REPEAT: Duration = Duration::from_millis(100);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

const ALREADY_EXISTS: &str = "DBC6";
const IS_BEING_IMPORTED: &str = "DBC8";
const DUCKDB_SPILL_FILE_PREFIX: &str = "duckdb_temp_";
const CURRENT_DIRECTORY: &str = ".";
const CONSOLE_COMMAND: &str = "typedb console";

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
        Output::DataFile(_) if !args.files_only => Some(connect_to_new_database(args, &input.database).await?),
        Output::DataFile(_) | Output::DryRun { .. } => None,
    };
    let mut termination = Termination::listen();
    try_println!("Migrating from DuckDB file '{}'", input.duckdb_path.display());
    let progress = Progress::new();
    let heartbeat = progress.keep_reporting();
    let cancellation = Arc::new(Cancellation::default());
    let migration = prepare(args, input, output.spill_dir(), &cancellation, &mut termination).await?;
    let import_command = match &output {
        Output::DryRun { .. } => None,
        Output::DataFile(data_file) => {
            Some(console_import_command(args, &input.database, &input.schema_path, &data_file.path))
        }
    };
    let (checksums, elapsed) =
        convert(migration, input, &output, import_command.as_deref(), &progress, &cancellation, &mut termination)
            .await?;
    drop(heartbeat);
    print_conversion_counts(&checksums, elapsed);

    let (Output::DataFile(data_file), Some(import_command)) = (output, import_command) else {
        try_println!("Nothing was written or loaded (--dry-run).");
        return Ok(());
    };
    let Some(settings) = server else {
        try_println!("  Data file:      {}", data_file.path.display());
        try_println!("Import with: {import_command}");
        return Ok(());
    };
    let kept = |failure: Failure| Failure { kept_hint: Some(kept_hint(&data_file.path, &import_command)), ..failure };
    let database = &input.database;
    try_println!("Importing into '{database}' from {} (kept if the import fails)...", data_file.path.display());
    let started = Instant::now();
    let import = tokio::spawn(import(settings, database.clone(), input.schema_text.clone(), data_file.path.clone()));
    let imported = until_terminated(&mut termination, import).await;
    joined(imported.map_err(|exit_code| kept(Failure::interrupted_import(exit_code)))?).map_err(kept)?;
    remove_file_if_exists(&data_file.path);
    try_println!("Imported in {}.", format_duration(started.elapsed()));
    Ok(())
}

struct MigrationInput {
    duckdb_path: PathBuf,
    schema_path: String,
    schema_text: String,
    database: String,
    original_database: String,
}

impl MigrationInput {
    fn read(args: &Args) -> MigrationInput {
        let duckdb_path = PathBuf::from(args.duckdb.as_deref().expect("dispatched on --duckdb"));
        check_source_file(&duckdb_path);
        let Some(schema_path) = args.schema_file.clone() else {
            fatal_with(ExitCode::UserInputError, "--schema-file is required with --duckdb");
        };
        let schema_text = fs::read_to_string(&schema_path).unwrap_or_else(|err| {
            fatal_with(ExitCode::UserInputError, format!("failed to read schema file '{schema_path}': {err}"))
        });
        let database = database_name(args, &duckdb_path);
        let original_database = format!("duckdb:{}", duckdb_path.file_name().unwrap_or_default().to_string_lossy());
        MigrationInput { duckdb_path, schema_path, schema_text, database, original_database }
    }
}

fn check_source_file(duckdb_path: &Path) {
    if !duckdb_path.is_file() {
        let problem = if duckdb_path.exists() { "is not a file" } else { "does not exist" };
        fatal_with(ExitCode::UserInputError, format!("DuckDB file '{}' {problem}", duckdb_path.display()));
    }
}

fn database_name(args: &Args, duckdb_path: &Path) -> String {
    let name = args.database.clone().unwrap_or_else(|| {
        let stem = duckdb_path.file_stem().expect("the source was checked to be a file, which has a name");
        stem.to_string_lossy().into_owned()
    });
    if !args.dry_run && !typeql::common::identifier::is_valid_label(&name) {
        let origin = if args.database.is_some() { "" } else { " (taken from the file name; pass --database)" };
        fatal_with(ExitCode::UserInputError, format!("'{name}' is not a valid TypeDB database name{origin}"));
    }
    name
}

enum Output {
    DryRun { spill_dir: PathBuf },
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
            remove_spill_left_behind(&spill_dir)?;
            return Ok(Output::DryRun { spill_dir });
        }
        let output_dir = match args.output_dir.as_deref() {
            Some(dir) => PathBuf::from(if dir.is_empty() { CURRENT_DIRECTORY } else { dir }),
            None => default_output_dir(&input.duckdb_path),
        };
        fs::create_dir_all(&output_dir).map_err(|err| {
            let message = format!("failed to create output directory '{}': {err}", output_dir.display());
            Failure::new(ExitCode::MigrationError, message)
        })?;
        let lock = OutputLock::take(&output_dir, &input.database)?;
        let path = output_data_path(&output_dir, &input.database, &input.duckdb_path, args.overwrite)?;
        let spill_dir = output_dir.join(format!(".{}.tmp", input.database));
        remove_spill_left_behind(&spill_dir)?;
        remove_file_if_exists(&partial_path(&path));
        Ok(Output::DataFile(DataFileOutput { path, spill_dir, _lock: lock }))
    }

    fn spill_dir(&self) -> &Path {
        match self {
            Output::DryRun { spill_dir } | Output::DataFile(DataFileOutput { spill_dir, .. }) => spill_dir,
        }
    }
}

fn default_output_dir(duckdb_path: &Path) -> PathBuf {
    match duckdb_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from(CURRENT_DIRECTORY),
    }
}

// One run per database and directory owns the data file, partial file and spill directory; a dead run's are stale.
// The lock file stays, as removing it would let a run that opened it just before take a lock nobody else sees.
struct OutputLock {
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
        let file = fs::OpenOptions::new().create(true).truncate(false).write(true).open(&path).map_err(failed)?;
        match file.try_lock() {
            Ok(()) => Ok(OutputLock { _file: file }),
            Err(fs::TryLockError::WouldBlock) => {
                let message = format!("another loader is preparing or importing '{database}' in '{}'", dir.display());
                Err(Failure::new(ExitCode::UserInputError, message))
            }
            Err(fs::TryLockError::Error(err)) => Err(failed(err)),
        }
    }
}

fn output_data_path(
    dir: &Path,
    database: &str,
    duckdb_path: &Path,
    is_overwrite_allowed: bool,
) -> Result<PathBuf, Failure> {
    let path = dir.join(format!("{database}.data.typedb"));
    let input_error = |problem: &str| Failure::new(ExitCode::UserInputError, format!("'{}' {problem}", path.display()));
    let source = fs::canonicalize(duckdb_path).ok();
    let is_source = |output: &Path| fs::canonicalize(output).is_ok_and(|output| Some(output) == source);
    if is_source(&path) || is_source(&partial_path(&path)) {
        return Err(input_error(
            "would overwrite the DuckDB file being converted; choose another --database or --output-dir",
        ));
    }
    if path.is_dir() {
        return Err(input_error("is a directory, which the data file cannot replace"));
    }
    // A link counts as existing even when what it points to does not.
    if !is_overwrite_allowed && fs::symlink_metadata(&path).is_ok() {
        return Err(input_error("already exists; pass --overwrite to replace it"));
    }
    Ok(path)
}

fn remove_spill_left_behind(spill_dir: &Path) -> Result<(), Failure> {
    let Ok(metadata) = fs::symlink_metadata(spill_dir) else { return Ok(()) };
    if metadata.is_dir()
        && let Ok(entries) = fs::read_dir(spill_dir)
    {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(DUCKDB_SPILL_FILE_PREFIX) {
                remove_file_if_exists(&entry.path());
            }
        }
    }
    fs::remove_dir(spill_dir).map_err(|err| {
        let message = format!("'{}' is left from an earlier run and cannot be removed: {err}", spill_dir.display());
        Failure::new(ExitCode::UserInputError, message)
    })
}

fn remove_file_if_exists(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            println_warning!("failed to remove '{}': {err}", path.display());
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

async fn connect_to_new_database(args: &Args, database: &str) -> Result<ConnectionSettings, Failure> {
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
    let settings = ConnectionSettings {
        addresses,
        credentials: Credentials::new(username, &password),
        options: DriverOptions::new(tls_config),
    };
    let timed_out = |_| {
        let message = format!("TypeDB did not answer within {} s", CONNECT_TIMEOUT.as_secs());
        Failure::new(ExitCode::ConnectionError, message)
    };
    let is_new =
        tokio::time::timeout(CONNECT_TIMEOUT, is_new_database(&settings, database)).await.map_err(timed_out)?;
    match is_new? {
        true => Ok(settings),
        false => Err(input_error(format!(
            "database '{database}' already exists; the import creates it, so choose another --database"
        ))),
    }
}

async fn is_new_database(settings: &ConnectionSettings, database: &str) -> Result<bool, Failure> {
    let driver = settings
        .connect()
        .await
        .map_err(|err| Failure::new(ExitCode::ConnectionError, format!("failed to connect to TypeDB: {err}")))?;
    let contains = driver.databases().contains(database).await;
    contains
        .map(|is_present| !is_present)
        .map_err(|err| Failure::new(ExitCode::ConnectionError, format!("failed to list databases: {err}")))
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

    fn interrupted(exit_code: i32) -> Failure {
        Failure { exit_code, message: "interrupted; nothing was loaded".to_owned(), kept_hint: None }
    }

    fn interrupted_after_conversion(exit_code: i32, data_path: &Path, import_command: &str) -> Failure {
        let message = "interrupted after the conversion; nothing was loaded".to_owned();
        Failure { exit_code, message, kept_hint: Some(kept_hint(data_path, import_command)) }
    }

    fn interrupted_import(exit_code: i32) -> Failure {
        let message = "interrupted before the import completed; the server discards a partial import".to_owned();
        Failure { exit_code, message, kept_hint: None }
    }

    fn report(self) -> i32 {
        println_error!("{}", self.message);
        if let Some(kept_hint) = self.kept_hint {
            try_println!("{kept_hint}");
        }
        self.exit_code
    }
}

async fn until_terminated<T>(termination: &mut Termination, work: impl Future<Output = T>) -> Result<T, i32> {
    tokio::select! {
        result = work => Ok(result),
        exit_code = termination.next_exit_code() => Err(exit_code),
    }
}

struct Interrupted<T> {
    exit_code: i32,
    outcome: T,
}

async fn cancel_on_termination<T: Send + 'static>(
    termination: &mut Termination,
    cancellation: &Cancellation,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Interrupted<T>> {
    let mut task = tokio::task::spawn_blocking(work);
    let exit_code = tokio::select! {
        outcome = &mut task => return Ok(joined(outcome)),
        exit_code = termination.next_exit_code() => exit_code,
    };
    let mut repeat = tokio::time::interval(INTERRUPT_REPEAT);
    loop {
        tokio::select! {
            outcome = &mut task => return Err(Interrupted { exit_code, outcome: joined(outcome) }),
            _ = repeat.tick() => cancellation.cancel(),
            second_exit_code = termination.next_exit_code() => {
                eprintln!("{FORCE_EXIT_MESSAGE}");
                process::exit(second_exit_code)
            }
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
    cancellation: &Arc<Cancellation>,
    termination: &mut Termination,
) -> Result<Migration, Failure> {
    let (source, schema_text) = (input.duckdb_path.clone(), input.schema_text.clone());
    let (spill_dir, memory_limit, task_cancellation) =
        (spill_dir.to_path_buf(), args.memory_limit.clone(), cancellation.clone());
    let started = Instant::now();
    let prepared = cancel_on_termination(termination, cancellation, move || {
        Migration::prepare(&source, &schema_text, &spill_dir, memory_limit.as_deref(), task_cancellation)
    })
    .await
    .map_err(|interrupted| Failure::interrupted(interrupted.exit_code))?;
    match prepared {
        Ok(migration) => {
            print_mapping(&migration.describe_mapping());
            print_warnings(migration.warnings());
            try_println!(
                "Checked in {} (source DB memory limit {}).",
                format_duration(started.elapsed()),
                migration.memory_limit()
            );
            Ok(migration)
        }
        Err(PrepareError::Setup(message)) => Err(Failure::new(ExitCode::UserInputError, message)),
        Err(PrepareError::Invalid { errors, warnings, mapping_description }) => {
            print_mapping(&mapping_description);
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
    import_command: Option<&str>,
    progress: &Progress,
    cancellation: &Cancellation,
    termination: &mut Termination,
) -> Result<(Checksums, Duration), Failure> {
    let data_path = match output {
        Output::DryRun { .. } => None,
        Output::DataFile(data_file) => Some(data_file.path.clone()),
    };
    let (original_database, progress) = (input.original_database.clone(), progress.clone());
    let data_file = data_path.clone();
    let converted = cancel_on_termination(termination, cancellation, move || {
        let started = Instant::now();
        let checksums = match &data_file {
            None => migration.run(io::sink(), VERSION.trim(), &original_database, &progress),
            Some(path) => write_data_file(&migration, path, &original_database, &progress),
        };
        drop(migration);
        checksums.map(|checksums| (checksums, started.elapsed()))
    })
    .await;
    match (converted, data_path.zip(import_command)) {
        (Ok(converted), _) => converted.map_err(|message| Failure::new(ExitCode::MigrationError, message)),
        (Err(Interrupted { exit_code, outcome: Ok(_) }), Some((path, import_command))) => {
            Err(Failure::interrupted_after_conversion(exit_code, &path, import_command))
        }
        (Err(Interrupted { exit_code, .. }), _) => Err(Failure::interrupted(exit_code)),
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
        remove_file_if_exists(&self.0);
    }
}

fn partial_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().expect("data paths are built with a file name").to_string_lossy();
    path.with_file_name(format!(".{}.partial", file_name.trim_start_matches('.')))
}

// Checks and sorts report nothing for up to hours: each interval says what the latest scan reached, or "working".
#[derive(Clone)]
struct Progress {
    started: Instant,
    latest_scan: Arc<Mutex<Option<String>>>,
}

impl Progress {
    const INTERVAL: Duration = Duration::from_secs(30);

    fn new() -> Progress {
        Progress { started: Instant::now(), latest_scan: Arc::new(Mutex::new(None)) }
    }

    fn keep_reporting(&self) -> Heartbeat {
        let progress = self.clone();
        Heartbeat(tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at((Instant::now() + Progress::INTERVAL).into(), Progress::INTERVAL);
            loop {
                ticks.tick().await;
                let latest = progress.latest_scan().take().unwrap_or_else(|| "working".to_owned());
                try_println!("  {latest} ({} elapsed)", format_duration(progress.started.elapsed()));
            }
        }))
    }

    fn latest_scan(&self) -> MutexGuard<'_, Option<String>> {
        self.latest_scan.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ProgressListener for Progress {
    fn report(&self, progress: &ScanProgress<'_>) {
        *self.latest_scan() = Some(progress.to_string());
    }
}

struct Heartbeat(tokio::task::JoinHandle<()>);

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn import(
    settings: ConnectionSettings,
    database: String,
    schema: String,
    data_path: PathBuf,
) -> Result<(), Failure> {
    let imported = match settings.connect().await {
        Ok(driver) => driver.databases().import_from_file(&database, &schema, &data_path).await,
        Err(err) => Err(err),
    };
    imported.map_err(|err| {
        let code = match err {
            typedb_driver::Error::Connection(_) => ExitCode::ConnectionError,
            _ => ExitCode::GeneralError,
        };
        let cause = match err.code().as_str() {
            IS_BEING_IMPORTED => format!("another import of '{database}' is in progress"),
            ALREADY_EXISTS => format!("'{database}' was created by another import meanwhile"),
            _ => err.to_string(),
        };
        Failure::new(code, format!("import into '{database}' failed: {cause}"))
    })
}

fn kept_hint(data_path: &Path, import_command: &str) -> String {
    format!(
        "The prepared data file is kept: {}\nImport it without converting again with:\n  {import_command}",
        data_path.display()
    )
}

// Paths are made absolute so that the command can be run from anywhere.
fn console_import_command(args: &Args, database: &str, schema_path: &str, data_path: &Path) -> String {
    let mut command = String::from(CONSOLE_COMMAND);
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
    try_println!("Mapping:");
    for line in lines {
        try_println!("  {line}");
    }
}

fn print_warnings(warnings: &[String]) {
    for warning in warnings {
        println_warning!("{warning}");
    }
}

fn print_conversion_counts(checksums: &Checksums, elapsed: Duration) {
    try_println!("Converted in {}.", format_duration(elapsed));
    try_println!("  Attributes:     {:>8}", checksums.attribute_count);
    try_println!("  Entities:       {:>8}", checksums.entity_count);
    try_println!("  Relations:      {:>8}", checksums.relation_count);
    try_println!("  Ownerships:     {:>8}", checksums.ownership_count);
    try_println!("  Role players:   {:>8}", checksums.role_count);
}
