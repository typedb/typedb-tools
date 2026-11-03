/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Child,
    sync::OnceLock,
    thread::sleep,
    time::Duration,
};

use typedb_binary_runner::runner::TypeDBBinaryRunner;

const TYPEDB_SERVER_ARCHIVE_VAR: &str = "TYPEDB_SERVER_ARCHIVE";
const TYPEDB_LOADER_ARCHIVE_VAR: &str = "TYPEDB_LOADER_ARCHIVE";

const TYPEDB_SERVER_SUBCOMMAND: &str = "server";
const TYPEDB_LOADER_SUBCOMMAND: &str = "loader";

const SCHEMA_PATH_VAR: &str = "SCHEMA_PATH";
const QUERY_PATH_VAR: &str = "QUERY_PATH";
const DATA_PATH_VAR: &str = "DATA_PATH";
const DATA_WITH_REJECTS_PATH_VAR: &str = "DATA_WITH_REJECTS_PATH";
const SCHEMA_WITH_FRIENDSHIPS_PATH_VAR: &str = "SCHEMA_WITH_FRIENDSHIPS_PATH";
const QUERY_FRIENDSHIP_PATH_VAR: &str = "QUERY_FRIENDSHIP_PATH";
const DATA_FRIENDSHIPS_PATH_VAR: &str = "DATA_FRIENDSHIPS_PATH";

const SERVER_ADDRESS: &str = "localhost:1729";
const ADMIN_USERNAME: &str = "admin";
const ADMIN_PASSWORD: &str = "password";

#[test]
fn loader_loads_clean_csv() -> Result<(), Box<dyn Error>> {
    /*
    NOTE: subprocesses make the logging and debugging from this test difficult.
          The simplest way to bisect issues is to run TypeDB server externally,
          and remove it from here.
    */

    ensure_typedb_server();

    let database = "loader-test-clean-db";
    let schema_path = env::var(SCHEMA_PATH_VAR)?;
    let query_path = env::var(QUERY_PATH_VAR)?;
    let data_path_string = env::var(DATA_PATH_VAR)?;
    let data_path = stage_data_file(&data_path_string)?;
    let data_path_str = data_path.to_string_lossy().into_owned();

    let loader_runner = TypeDBBinaryRunner::new(TYPEDB_LOADER_ARCHIVE_VAR, TYPEDB_LOADER_SUBCOMMAND)
        .expect("Failed to create loader binary runner");
    let args = [
        "--address",
        SERVER_ADDRESS,
        "--database",
        database,
        "--create-db",
        "--schema-file",
        &schema_path,
        "--query",
        &query_path,
        "--data",
        &data_path_str,
        "--header",
        "--username",
        ADMIN_USERNAME,
        "--password",
        ADMIN_PASSWORD,
        "--tls-disabled",
    ];
    let mut loader_process: Child = loader_runner.run(&args).expect("Failed to spawn loader process.");
    let status = loader_process.wait().expect("Error waiting for loader to finish").code().unwrap_or(-1);

    let rejects_csv = output_file(&data_path, "rejects.csv");
    let rejects_log = output_file(&data_path, "rejects.log");

    if status != 0 {
        panic!("Loader returned non-zero exit status: {}", status);
    }
    if rejects_csv.exists() {
        panic!("Unexpected rejects CSV created on clean run: {}", rejects_csv.display());
    }
    if rejects_log.exists() {
        panic!("Unexpected rejects log created on clean run: {}", rejects_log.display());
    }
    Ok(())
}

#[test]
fn loader_writes_rejects_for_bad_rows() -> Result<(), Box<dyn Error>> {
    ensure_typedb_server();

    let database = "loader-test-rejects-db";
    let schema_path = env::var(SCHEMA_PATH_VAR)?;
    let query_path = env::var(QUERY_PATH_VAR)?;
    let data_path_string = env::var(DATA_WITH_REJECTS_PATH_VAR)?;
    let data_path = stage_data_file(&data_path_string)?;
    let data_path_str = data_path.to_string_lossy().into_owned();

    let loader_runner = TypeDBBinaryRunner::new(TYPEDB_LOADER_ARCHIVE_VAR, TYPEDB_LOADER_SUBCOMMAND)
        .expect("Failed to create loader binary runner");
    let args = [
        "--address",
        SERVER_ADDRESS,
        "--database",
        database,
        "--create-db",
        "--schema-file",
        &schema_path,
        "--query",
        &query_path,
        "--data",
        &data_path_str,
        "--header",
        "--username",
        ADMIN_USERNAME,
        "--password",
        ADMIN_PASSWORD,
        "--tls-disabled",
    ];
    let mut loader_process: Child = loader_runner.run(&args).expect("Failed to spawn loader process.");
    let status = loader_process.wait().expect("Error waiting for loader to finish").code().unwrap_or(-1);

    let rejects_csv = output_file(&data_path, "rejects.csv");
    let rejects_log = output_file(&data_path, "rejects.log");
    let csv_contents = fs::read_to_string(&rejects_csv).ok();
    let log_contents = fs::read_to_string(&rejects_log).ok();

    if status != 0 {
        panic!("Loader returned non-zero exit status on tolerant run: {}", status);
    }
    let csv = csv_contents.unwrap_or_else(|| panic!("Expected rejects CSV at {}", rejects_csv.display()));
    let log = log_contents.unwrap_or_else(|| panic!("Expected rejects log at {}", rejects_log.display()));
    assert!(csv.contains("bob"), "rejects CSV missing 'bob' row: {csv}");
    assert!(csv.contains("dan"), "rejects CSV missing 'dan' row: {csv}");
    assert!(log.contains("row 2"), "rejects log missing 'row 2' entry: {log}");
    assert!(log.contains("row 4"), "rejects log missing 'row 4' entry: {log}");
    Ok(())
}

#[test]
fn loader_loads_relation_match_insert() -> Result<(), Box<dyn Error>> {
    ensure_typedb_server();

    let database = "loader-test-relations-db";
    let schema_path = env::var(SCHEMA_WITH_FRIENDSHIPS_PATH_VAR)?;
    let people_query_path = env::var(QUERY_PATH_VAR)?;
    let friendship_query_path = env::var(QUERY_FRIENDSHIP_PATH_VAR)?;
    let people_data = stage_data_file(&env::var(DATA_PATH_VAR)?)?;
    let people_data_str = people_data.to_string_lossy().into_owned();
    let friendships_data = stage_data_file(&env::var(DATA_FRIENDSHIPS_PATH_VAR)?)?;
    let friendships_data_str = friendships_data.to_string_lossy().into_owned();

    let loader_runner = TypeDBBinaryRunner::new(TYPEDB_LOADER_ARCHIVE_VAR, TYPEDB_LOADER_SUBCOMMAND)
        .expect("Failed to create loader binary runner");

    // First pass: create the database, apply the schema, and load people as entities.
    let people_args = [
        "--address",
        SERVER_ADDRESS,
        "--database",
        database,
        "--create-db",
        "--schema-file",
        &schema_path,
        "--query",
        &people_query_path,
        "--data",
        &people_data_str,
        "--header",
        "--username",
        ADMIN_USERNAME,
        "--password",
        ADMIN_PASSWORD,
        "--tls-disabled",
        "--no-checkpoint",
    ];
    let mut people_process: Child = loader_runner.run(&people_args).expect("Failed to spawn loader (people pass).");
    let people_status = people_process.wait().expect("Error waiting for loader (people pass)").code().unwrap_or(-1);

    // Second pass: insert friendships using a match-insert query against the now-loaded persons.
    let friendship_args = [
        "--address",
        SERVER_ADDRESS,
        "--database",
        database,
        "--query",
        &friendship_query_path,
        "--data",
        &friendships_data_str,
        "--header",
        "--username",
        ADMIN_USERNAME,
        "--password",
        ADMIN_PASSWORD,
        "--tls-disabled",
    ];
    let mut friendship_process: Child =
        loader_runner.run(&friendship_args).expect("Failed to spawn loader (friendship pass).");
    let friendship_status =
        friendship_process.wait().expect("Error waiting for loader (friendship pass)").code().unwrap_or(-1);

    let friendship_rejects_csv = output_file(&friendships_data, "rejects.csv");
    let friendship_rejects_log = output_file(&friendships_data, "rejects.log");

    if people_status != 0 {
        panic!("Loader (people pass) returned non-zero exit status: {}", people_status);
    }
    if friendship_status != 0 {
        panic!("Loader (friendship pass) returned non-zero exit status: {}", friendship_status);
    }
    if friendship_rejects_csv.exists() {
        panic!("Unexpected rejects CSV on friendship pass: {}", friendship_rejects_csv.display());
    }
    if friendship_rejects_log.exists() {
        panic!("Unexpected rejects log on friendship pass: {}", friendship_rejects_log.display());
    }
    Ok(())
}

#[test]
fn loader_imports_a_duckdb_database() -> Result<(), Box<dyn Error>> {
    ensure_typedb_server();

    let database = "loader-test-duckdb-db";
    let dir = env::temp_dir().join(format!("typedb-loader-duckdb-test-{}", std::process::id()));
    fs::create_dir_all(&dir)?;
    let duckdb_path = dir.join("people.duckdb");
    duckdb::Connection::open(&duckdb_path)?.execute_batch(
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL, nickname VARCHAR[]);
         CREATE TABLE friendship (_iid UBIGINT, a UBIGINT, b UBIGINT);
         COMMENT ON TABLE friendship IS '@typedb relation friendship (friend = a, friend = b)';
         COMMENT ON COLUMN friendship.a IS '@typedb references person';
         COMMENT ON COLUMN friendship.b IS '@typedb references person';
         INSERT INTO person SELECT i, 'person ' || i, CASE WHEN i % 3 = 0 THEN ['p' || i, 'q'] END FROM range(1000) t(i);
         INSERT INTO friendship SELECT i, i, (i + 1) % 1000 FROM range(1000) t(i);",
    )?;
    let schema_path = dir.join("schema.tql");
    fs::write(
        &schema_path,
        "define attribute name, value string; attribute nickname, value string;
         entity person, owns name @card(1), owns nickname @card(0..), plays friendship:friend;
         relation friendship, relates friend @card(2);",
    )?;
    let (duckdb_path, schema_path) = (duckdb_path.to_string_lossy(), schema_path.to_string_lossy());

    let loader_runner = TypeDBBinaryRunner::new(TYPEDB_LOADER_ARCHIVE_VAR, TYPEDB_LOADER_SUBCOMMAND)
        .expect("Failed to create loader binary runner");
    let args = [
        "--duckdb",
        &duckdb_path,
        "--schema-file",
        &schema_path,
        "--database",
        database,
        "--address",
        SERVER_ADDRESS,
        "--username",
        ADMIN_USERNAME,
        "--password",
        ADMIN_PASSWORD,
        "--tls-disabled",
    ];
    // The import verifies the file's checksums against what it imported.
    let status = loader_runner.run(&args)?.wait()?.code().unwrap_or(-1);
    assert_eq!(status, 0, "the DuckDB load failed");
    assert!(!dir.join(format!("{database}.data.typedb")).exists(), "the data file outlived its import");
    // The import created the database, so a second load is refused.
    let status = loader_runner.run(&args)?.wait()?.code().unwrap_or(-1);
    assert_eq!(status, 2, "a load into an existing database must fail");
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

// A single TypeDB server is shared across every test in this binary: each test uses a distinct
// database name, and starting one server per test would race on the hardcoded port 1729.
// The runner (which owns the unpacked distribution's temp dir) and the child are leaked so they
// outlive every test; the OS reaps the server process when the test binary exits.
static SERVER_READY: OnceLock<()> = OnceLock::new();

fn ensure_typedb_server() {
    SERVER_READY.get_or_init(|| {
        let runner = TypeDBBinaryRunner::new(TYPEDB_SERVER_ARCHIVE_VAR, TYPEDB_SERVER_SUBCOMMAND)
            .expect("Failed to create server binary runner");
        // note: run in development mode to avoid polluting analytics data when using tagged releases
        let args = ["--server.address", "0.0.0.0:1729", "--development-mode.enabled", "true"];
        let child: Child = runner.run(&args).expect("Failed to spawn child server process.");
        sleep(Duration::from_secs(5));
        Box::leak(Box::new((runner, child)));
    });
}

// Copies the read-only Bazel runfile to a temp dir so the rejects sibling files can be written
// next to it without polluting the source tree.
fn stage_data_file(source: &str) -> Result<PathBuf, Box<dyn Error>> {
    let source_path = Path::new(source);
    let file_name = source_path.file_name().ok_or_else(|| format!("source data path has no file name: {source}"))?;
    let staged_dir = env::temp_dir().join(format!("typedb-loader-test-{}", std::process::id()));
    fs::create_dir_all(&staged_dir)?;
    let staged = staged_dir.join(file_name);
    fs::copy(source_path, &staged)?;
    Ok(staged)
}

fn output_file(data_path: &Path, filename: &str) -> PathBuf {
    let stem = data_path.file_stem().and_then(|s| s.to_str()).unwrap_or("data");
    let dirname = format!("loader_{stem}_progress");
    match data_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(dirname).join(filename),
        _ => PathBuf::from(dirname).join(filename),
    }
}
