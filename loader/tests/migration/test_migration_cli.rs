/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use duckdb::Connection;
use prost::Message;
use typedb_protocol::migration::Item;

const SCHEMA: &str = r#"define
  attribute name value string;
  entity person, owns name @card(1..1);
"#;

// Cargo builds the binary alongside the test; Bazel passes it in as a data dependency.
fn loader_binary() -> PathBuf {
    std::env::var_os("TYPEDB_LOADER_BIN")
        .map(PathBuf::from)
        .or_else(|| option_env!("CARGO_BIN_EXE_typedb-loader").map(PathBuf::from))
        .expect("the loader binary is provided by Cargo or by Bazel's TYPEDB_LOADER_BIN")
}

fn setup(test_name: &str) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let dir = std::env::temp_dir().join(format!("typedb-migration-cli-{test_name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch(
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL);
         INSERT INTO person VALUES (1, 'Alice'), (2, 'Bob');",
    )?;
    fs::write(dir.join("schema.tql"), SCHEMA)?;
    Ok(dir)
}

fn loader_command(dir: &Path, extra: &[&str]) -> Command {
    let mut command = Command::new(loader_binary());
    command
        .arg("--duckdb")
        .arg(dir.join("fixture.duckdb"))
        .arg("--schema-file")
        .arg(dir.join("schema.tql"))
        .args(["--database", "fixture"])
        .args(extra);
    command
}

fn run_loader(dir: &Path, extra: &[&str]) -> std::process::Output {
    loader_command(dir, extra).output().expect("failed to run the loader binary")
}

#[test]
fn files_only_writes_the_data_file() -> Result<(), Box<dyn Error>> {
    let dir = setup("clean")?;
    let out = dir.join("out").to_string_lossy().into_owned();
    let output = run_loader(&dir, &["--files-only", &out]);
    assert!(
        output.status.success(),
        "loader failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let schema_path = dir.join("schema.tql").to_string_lossy().into_owned();
    assert!(
        stdout.contains(&format!("database import fixture {schema_path} ")),
        "no import hint referencing the original schema file: {stdout}"
    );
    assert!(!dir.join("out/fixture.schema.typeql").exists(), "the schema file is referenced, not copied");
    let data = fs::read(dir.join("out/fixture.data.typedb"))?;
    let mut buf: &[u8] = &data;
    let mut count = 0;
    while !buf.is_empty() {
        let len = prost::decode_length_delimiter(&mut buf)?;
        let (message, rest) = buf.split_at(len);
        buf = rest;
        Item::decode(message)?;
        count += 1;
    }
    assert_eq!(count, 1 + 2 + 2 + 1); // header, 2 names, 2 persons, checksums

    let rerun = run_loader(&dir, &["--files-only", &out]);
    assert!(!rerun.status.success());
    let rerun = run_loader(&dir, &["--files-only", &out, "--overwrite"]);
    assert!(rerun.status.success());

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn validation_errors_exit_nonzero_with_no_output() -> Result<(), Box<dyn Error>> {
    let dir = setup("invalid")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch("ALTER TABLE person ADD COLUMN mystery VARCHAR;")?;
    drop(conn);

    let out = dir.join("out").to_string_lossy().into_owned();
    let output = run_loader(&dir, &["--files-only", &out]);
    assert_eq!(output.status.code(), Some(2), "user-input error exit code expected");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("mystery"), "error does not name the column: {stderr}");
    assert!(!dir.join("out/fixture.data.typedb").exists());

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn dry_run_writes_nothing() -> Result<(), Box<dyn Error>> {
    let dir = setup("dry")?;
    let output = run_loader(&dir, &["--dry-run"]);
    assert!(output.status.success());
    assert!(!dir.join("out").exists(), "--dry-run created the output directory");
    let output = run_loader(&dir, &["--dry-run", "--files-only", "out"]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be used with"), "clap conflict expected");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn csv_options_conflict_with_duckdb() -> Result<(), Box<dyn Error>> {
    let dir = setup("conflict")?;
    let output = run_loader(&dir, &["--data", "rows.csv"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be used with"), "clap conflict expected: {stderr}");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn unsupported_extensions_point_to_the_right_path() -> Result<(), Box<dyn Error>> {
    let dir = setup("dispatch")?;
    fs::write(dir.join("rows.csv"), "a,b\n")?;
    let csv = dir.join("rows.csv").to_string_lossy().into_owned();
    let output = Command::new(loader_binary())
        .args(["--duckdb", &csv, "--schema-file", &dir.join("schema.tql").to_string_lossy()])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--data"), "CSV hint expected");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn invalid_database_names_fail_before_converting() -> Result<(), Box<dyn Error>> {
    let dir = setup("name")?;
    let out = dir.join("out").to_string_lossy().into_owned();
    let output = Command::new(loader_binary())
        .args(["--duckdb", &dir.join("fixture.duckdb").to_string_lossy(), "--schema-file"])
        .arg(dir.join("schema.tql"))
        .args(["--database", "my data", "--files-only", &out])
        .output()?;
    assert_eq!(output.status.code(), Some(2), "user-input error exit code expected");
    assert!(String::from_utf8_lossy(&output.stderr).contains("'my data' is not a valid TypeDB database name"));
    assert!(!dir.join("out").exists(), "nothing is written for an invalid name");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn a_staged_data_file_is_refused_before_connecting() -> Result<(), Box<dyn Error>> {
    let dir = setup("staged")?;
    let staged = dir.join("fixture.data.typedb");
    fs::write(&staged, b"left by an earlier run whose import failed")?;
    let connection = ["--address", "127.0.0.1:1", "--username", "admin", "--password", "x", "--tls-disabled"];
    let output = run_loader(&dir, &connection);
    assert_eq!(output.status.code(), Some(2), "user-input error exit code expected");
    assert!(String::from_utf8_lossy(&output.stderr).contains("pass --overwrite"));
    let output = run_loader(&dir, &[&connection[..], &["--overwrite"]].concat());
    assert_eq!(output.status.code(), Some(3), "with --overwrite the run proceeds to connect");
    assert!(String::from_utf8_lossy(&output.stderr).contains("failed to connect"));
    assert_eq!(fs::read(&staged)?, b"left by an earlier run whose import failed", "untouched until converted");
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[cfg(unix)]
#[test]
fn a_termination_signal_removes_the_partial_file() -> Result<(), Box<dyn Error>> {
    let dir = setup("sigterm")?;
    Connection::open(dir.join("fixture.duckdb"))?
        .execute_batch("INSERT INTO person SELECT 10 + i, 'name' || i FROM range(2000000) t(i);")?;
    let out = dir.join("out");
    let mut loader = loader_command(&dir, &["--files-only", out.to_str().unwrap()]).spawn()?;
    let partial = out.join(".fixture.data.typedb.partial");
    let deadline = Instant::now() + Duration::from_secs(60);
    while !partial.exists() {
        assert!(loader.try_wait()?.is_none(), "the loader finished before the signal");
        assert!(Instant::now() < deadline, "the conversion did not start in time");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(Command::new("kill").args(["-TERM", &loader.id().to_string()]).status()?.success());
    assert_eq!(loader.wait()?.code(), Some(143));
    assert!(!partial.exists(), "the partial file was left behind");
    assert!(!out.join("fixture.data.typedb").exists());
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}
