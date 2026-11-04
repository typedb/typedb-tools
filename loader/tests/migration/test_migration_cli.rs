/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    process::Command,
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
    let connection = Connection::open(dir.join("fixture.duckdb"))?;
    connection.execute_batch(
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
    let output = run_loader(&dir, &["--files-only", "--output-dir", &out]);
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
    assert_eq!(
        file_names(&dir.join("out"))?,
        LEFT_BEHIND,
        "only the data file is left: the schema is referenced, not copied"
    );
    let data = zstd::stream::decode_all(fs::File::open(dir.join("out/fixture.data.typedb"))?)?;
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

    let rerun = run_loader(&dir, &["--files-only", "--output-dir", &out]);
    assert_eq!(rerun.status.code(), Some(2), "user-input error exit code expected");
    assert!(String::from_utf8_lossy(&rerun.stderr).contains("pass --overwrite"));
    assert!(!String::from_utf8_lossy(&rerun.stdout).contains("Checked in"), "refused before the checks");
    let rerun = run_loader(&dir, &["--files-only", "--output-dir", &out, "--overwrite"]);
    assert!(rerun.status.success());

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn validation_errors_exit_nonzero_with_no_output() -> Result<(), Box<dyn Error>> {
    let dir = setup("invalid")?;
    let connection = Connection::open(dir.join("fixture.duckdb"))?;
    connection.execute_batch("ALTER TABLE person ADD COLUMN mystery VARCHAR;")?;
    drop(connection);

    let out = dir.join("out").to_string_lossy().into_owned();
    let output = run_loader(&dir, &["--files-only", "--output-dir", &out]);
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
    assert_eq!(file_names(&dir)?, ["fixture.duckdb", "schema.tql"], "--dry-run wrote next to the DuckDB file");
    let output = run_loader(&dir, &["--dry-run", "--files-only"]);
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
fn files_of_other_formats_point_to_staging_them() -> Result<(), Box<dyn Error>> {
    let dir = setup("dispatch")?;
    fs::write(dir.join("rows.csv"), "a,b\n")?;
    let csv = dir.join("rows.csv").to_string_lossy().into_owned();
    let output = Command::new(loader_binary())
        .args(["--duckdb", &csv, "--schema-file", &dir.join("schema.tql").to_string_lossy(), "--dry-run"])
        .output()?;
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("staging other formats"), "staging hint expected");

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
        .args(["--database", "my data", "--files-only", "--output-dir", &out])
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

#[test]
fn the_memory_limit_is_reported_and_validated() -> Result<(), Box<dyn Error>> {
    let dir = setup("memory")?;
    let output = run_loader(&dir, &["--dry-run", "--memory-limit", "256MiB"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}\n{}", String::from_utf8_lossy(&output.stderr));
    assert!(stdout.contains("(source DB memory limit 256.0 MiB)"), "{stdout}");

    let output = run_loader(&dir, &["--dry-run"]);
    assert!(String::from_utf8_lossy(&output.stdout).contains("(source DB memory limit "), "a default limit is set");

    let output = run_loader(&dir, &["--dry-run", "--memory-limit", "lots"]);
    assert_eq!(output.status.code(), Some(2), "an invalid limit is a user-input error");
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid memory limit 'lots'"));
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn checks_spill_into_an_output_directory_that_does_not_exist_yet() -> Result<(), Box<dyn Error>> {
    let dir = setup("spill")?;
    // Shuffled ids take the GROUP BY check, which spills about 250 MB at this size.
    Connection::open(dir.join("fixture.duckdb"))?.execute_batch(
        "DROP TABLE person;
         CREATE TABLE person AS SELECT (i * 7919 % 20000003)::UBIGINT AS _iid FROM range(20000000) t(i);",
    )?;
    fs::write(dir.join("schema.tql"), "define\n  entity person;\n")?;
    let out = dir.join("out/nested").to_string_lossy().into_owned();
    let output = run_loader(&dir, &["--files-only", "--output-dir", &out, "--memory-limit", "256MiB"]);
    assert!(
        output.status.success(),
        "loader failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(file_names(&dir.join("out/nested"))?, LEFT_BEHIND, "the spill directory is removed");
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn what_a_dead_run_left_is_swept() -> Result<(), Box<dyn Error>> {
    let dir = setup("stale")?;
    let out = dir.join("out");
    fs::create_dir_all(out.join(".fixture.tmp"))?;
    fs::write(out.join(".fixture.tmp/duckdb_temp_storage-0.tmp"), b"spilled by a run that was killed")?;
    fs::write(out.join(".fixture.data.typedb.partial"), b"half a file")?;
    fs::write(out.join(".fixture.lock"), b"")?;
    let output = run_loader(&dir, &["--files-only", "--output-dir", out.to_str().unwrap()]);
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(file_names(&out)?, LEFT_BEHIND);

    fs::create_dir(out.join(".fixture.tmp"))?;
    fs::write(out.join(".fixture.tmp/notes.txt"), b"not DuckDB's")?;
    let output = run_loader(&dir, &["--files-only", "--overwrite", "--output-dir", out.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(2), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(fs::read(out.join(".fixture.tmp/notes.txt"))?, b"not DuckDB's", "only DuckDB's spill files are removed");
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[cfg(unix)]
mod signals_and_concurrency {
    use std::{
        io::{BufRead, BufReader, Read},
        process::Stdio,
        time::{Duration, Instant},
    };

    use super::*;

    #[test]
    fn a_termination_signal_removes_the_partial_file() -> Result<(), Box<dyn Error>> {
        let dir = setup("sigterm")?;
        Connection::open(dir.join("fixture.duckdb"))?
            .execute_batch("INSERT INTO person SELECT 10 + i, 'name' || i FROM range(2000000) t(i);")?;
        let out = dir.join("out");
        let mut loader = loader_command(&dir, &["--files-only", "--output-dir", out.to_str().unwrap()]).spawn()?;
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
        assert_eq!(file_names(&out)?, Vec::<String>::new(), "nothing is left behind");
        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn a_termination_signal_stops_the_checks_and_leaves_nothing() -> Result<(), Box<dyn Error>> {
        let dir = setup("sigterm-checks")?;
        Connection::open(dir.join("fixture.duckdb"))?.execute_batch(
            "DROP TABLE person;
             CREATE TABLE person AS SELECT (i * 7919 % 20000003)::UBIGINT AS _iid, 'name' || i AS name
             FROM range(20000000) t(i);",
        )?;
        let out = dir.join("out");
        let mut loader = loader_command(&dir, &["--files-only", "--output-dir", out.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut stdout = BufReader::new(loader.stdout.take().expect("piped"));
        let mut line = String::new();
        while !line.starts_with("Migrating from") {
            line.clear();
            assert!(stdout.read_line(&mut line)? > 0, "the loader ended before the checks began");
        }
        std::thread::sleep(Duration::from_millis(500));
        assert!(Command::new("kill").args(["-TERM", &loader.id().to_string()]).status()?.success());
        let mut rest = String::new();
        stdout.read_to_string(&mut rest)?;
        let output = loader.wait_with_output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(143), "{stderr}");
        assert!(stderr.contains("interrupted; nothing was loaded"), "{stderr}");
        assert!(!rest.contains("Checked in"), "the signal came after the checks: {rest}");
        assert_eq!(file_names(&out)?, Vec::<String>::new(), "nothing is left behind");
        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn a_second_loader_for_the_same_database_and_directory_fails_fast() -> Result<(), Box<dyn Error>> {
        let dir = setup("concurrent")?;
        Connection::open(dir.join("fixture.duckdb"))?
            .execute_batch("INSERT INTO person SELECT 10 + i, 'name' || i FROM range(2000000) t(i);")?;
        let out = dir.join("out");
        let mut first = loader_command(&dir, &["--files-only", "--output-dir", out.to_str().unwrap()]).spawn()?;
        let deadline = Instant::now() + Duration::from_secs(60);
        while !out.join(".fixture.lock").exists() {
            assert!(first.try_wait()?.is_none(), "the first loader finished before the second started");
            assert!(Instant::now() < deadline, "the first loader did not start in time");
            std::thread::sleep(Duration::from_millis(10));
        }
        let second = run_loader(&dir, &["--files-only", "--output-dir", out.to_str().unwrap()]);
        assert_eq!(second.status.code(), Some(2), "{}", String::from_utf8_lossy(&second.stderr));
        assert!(String::from_utf8_lossy(&second.stderr).contains("another loader is preparing or importing 'fixture'"));
        assert!(first.wait()?.success(), "the first loader must not be disturbed");
        assert!(out.join("fixture.data.typedb").exists());
        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }
}

// Elsewhere than on Unix, the lock file stays.
const LEFT_BEHIND: &[&str] =
    if cfg!(unix) { &["fixture.data.typedb"] } else { &[".fixture.lock", "fixture.data.typedb"] };

fn file_names(dir: &Path) -> io::Result<Vec<String>> {
    let mut names: Vec<String> = fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    names.sort();
    Ok(names)
}
