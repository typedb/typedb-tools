/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::{ArgGroup, Parser};

pub const USERNAME_VALUE_NAME: &str = "username";

const CSV_OPTIONS: [&str; 14] = [
    "query",
    "data",
    "header",
    "no_header",
    "null_values",
    "max_rows",
    "batch_rows",
    "parallel_batches",
    "stop_on_error",
    "no_stop_on_error",
    "max_rejects",
    "create_db",
    "no_checkpoint",
    "resume",
];

#[derive(Parser, Debug)]
#[command(author, about, group = ArgGroup::new("migration_options").multiple(true).conflicts_with_all(CSV_OPTIONS))]
pub struct Args {
    /// Migrate a DuckDB database instead of loading a CSV: its tables are mapped onto `--schema-file`,
    /// converted and imported into a new `--database`.
    #[arg(long, value_name = "path to DuckDB file", help_heading = "Migration", group = "migration_options")]
    pub duckdb: Option<String>,

    /// With --duckdb: write `<database>.data.typedb` into `--output-dir` without importing it.
    #[arg(
        long = "files-only",
        default_value = "false",
        help_heading = "Migration",
        requires = "duckdb",
        group = "migration_options"
    )]
    pub files_only: bool,

    /// With --duckdb: map and convert the database, printing the mapping and the counts, without
    /// writing or loading anything.
    #[arg(
        long = "dry-run",
        default_value = "false",
        help_heading = "Migration",
        requires = "duckdb",
        conflicts_with_all = ["files_only", "output_dir"],
        group = "migration_options"
    )]
    pub dry_run: bool,

    /// With --duckdb: the memory DuckDB may use before it spills to disk, such as `8GB` or `512MiB`.
    /// Default: half of the memory DuckDB detects (the machine's, or a container's limit).
    #[arg(
        long = "memory-limit",
        value_name = "size",
        help_heading = "Migration",
        requires = "duckdb",
        group = "migration_options"
    )]
    pub memory_limit: Option<String>,

    /// With --duckdb: replace an existing data file instead of refusing.
    #[arg(long, default_value = "false", help_heading = "Migration", requires = "duckdb", group = "migration_options")]
    pub overwrite: bool,

    /// Path to a TypeQL query file used as the loading template.
    /// File path can be absolute or relative to the current directory.
    #[arg(long, value_name = "path to query file (.tql)")]
    pub query: Option<String>,

    /// Name of the database to load data into.
    #[arg(long, visible_alias = "db", value_name = "database")]
    pub database: Option<String>,

    /// Path to the data file to load.
    /// File path can be absolute or relative to the current directory.
    #[arg(long, value_name = "path to data file (.csv)")]
    pub data: Option<String>,

    /// Whether the data file contains a header row. Default: false.
    /// Pass `--no-header` to explicitly disable when overriding a checkpoint that had it set.
    #[arg(long = "header", num_args = 0..=1, default_missing_value = "true", conflicts_with = "no_header")]
    pub header: Option<bool>,

    /// Force header row off, overriding any value carried by the checkpoint.
    #[arg(long = "no-header")]
    pub no_header: bool,

    /// Strings in the data file to treat as null. May be repeated.
    ///
    /// Default behaviour (flag not provided): only empty cells are treated as null.
    ///
    /// When provided, this list REPLACES the default — empty cells are no longer treated as
    /// null unless you include `""` (an empty string) explicitly. Include `""` in the list if
    /// you want both empty cells and your custom tokens to count as null.
    #[arg(long = "null-values", value_name = "value")]
    pub null_values: Option<Vec<String>>,

    /// Process at most this many data rows from the CSV. If not provided, all rows are processed.
    /// Pass `--max-rows 0` to explicitly clear a cap inherited from a checkpoint (e.g. on resume
    /// after the original run was bounded by `--max-rows`).
    #[arg(long = "max-rows", value_name = "n")]
    pub max_rows: Option<usize>,

    /// Number of rows submitted in each `query_with_inputs` invocation. Each batch is committed
    /// in its own write transaction. Default: 1000.
    /// On resume, this must match the value stored in the checkpoint.
    #[arg(long = "batch-rows", value_name = "n")]
    pub batch_rows: Option<usize>,

    /// Maximum number of batches submitted concurrently to the server. Default: 1 (strictly
    /// sequential).
    #[arg(long = "parallel-batches", value_name = "n")]
    pub parallel_batches: Option<usize>,

    /// Abort on the first row or batch error instead of skipping and continuing.
    /// The offending row(s) are still written to the rejects file before exit. Default: false.
    /// Pass `--no-stop-on-error` to explicitly disable when overriding a checkpoint that had it set.
    #[arg(long = "stop-on-error", num_args = 0..=1, default_missing_value = "true",
        conflicts_with = "no_stop_on_error")]
    pub stop_on_error: Option<bool>,

    /// Force stop-on-error off, overriding any value carried by the checkpoint.
    #[arg(long = "no-stop-on-error")]
    pub no_stop_on_error: bool,

    /// Abort once the total number of rejected rows exceeds this threshold.
    /// Applies independently of --stop-on-error.
    #[arg(long = "max-rejects", value_name = "n")]
    pub max_rejects: Option<usize>,

    /// Path to a TypeQL schema file to run in a schema transaction before data loading.
    /// Ignored (with a warning) when resuming.
    #[arg(long = "schema-file", value_name = "path to schema file (.tql)")]
    pub schema_file: Option<String>,

    /// Create the database if it does not already exist. Ignored (with a warning) when resuming.
    /// Default: false.
    #[arg(long = "create-db", num_args = 0..=1, default_missing_value = "true")]
    pub create_db: Option<bool>,

    /// TypeDB address(es) to connect to.
    /// Accepts either `--address host:port` or `--addresses host1:port1,host2:port2,host3:port3`
    #[arg(long = "address", alias = "addresses", value_name = "host:port[,host:port]")]
    pub addresses: Option<String>,

    /// Username for authentication.
    #[arg(long, value_name = USERNAME_VALUE_NAME)]
    pub username: Option<String>,

    /// Password for authentication. Will be requested safely by default.
    #[arg(long, value_name = "password")]
    pub password: Option<String>,

    /// Disable TLS encryption for the connection to TypeDB. Default: false (TLS enabled).
    /// Disable with caution: credentials and queries are sent in plaintext.
    #[arg(long = "tls-disabled", num_args = 0..=1, default_missing_value = "true")]
    pub tls_disabled: Option<bool>,

    /// Path to the TLS encryption root CA file.
    #[arg(long = "tls-root-ca", value_name = "path")]
    pub tls_root_ca: Option<String>,

    /// Directory to write the loader's output into, created if it does not already exist.
    /// From a CSV: `rejects.csv`, `rejects.log` and `checkpoint.json`, by default in
    /// `loader_<data-file-stem>_progress` next to the data file.
    /// With --duckdb: `<database>.data.typedb`, by default next to the DuckDB file.
    #[arg(long = "output-dir", value_name = "path to output directory")]
    pub output_dir: Option<String>,

    /// Disable checkpointing entirely. The loader will not write or maintain a checkpoint file.
    #[arg(long = "no-checkpoint", default_value = "false")]
    pub no_checkpoint: bool,

    /// Resume a previous run from the given output directory. The directory must contain a
    /// `checkpoint.json` from a prior run. Parameters from the checkpoint are used unless
    /// overridden on the command line.
    #[arg(long = "resume", value_name = "path to previous output directory")]
    pub resume: Option<String>,
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, error::ErrorKind};

    use super::{Args, CSV_OPTIONS};

    #[test]
    fn every_csv_option_conflicts_with_migration_options() {
        let shared = [
            "database",
            "schema_file",
            "addresses",
            "username",
            "password",
            "tls_disabled",
            "tls_root_ca",
            "output_dir",
        ];
        let mut command = Args::command();
        command.build(); // the group learns its members here
        let group = command.get_groups().find(|group| group.get_id() == "migration_options").unwrap();
        let migration: Vec<&str> = group.get_args().map(|id| id.as_str()).collect();
        for arg in command.get_arguments().filter(|arg| arg.get_long() != Some("help")) {
            let id = arg.get_id().as_str();
            if shared.contains(&id) || migration.contains(&id) {
                continue;
            }
            assert!(CSV_OPTIONS.contains(&id), "'{id}' is in neither list; add it to CSV_OPTIONS or to this test");
            let option = format!("--{}", arg.get_long().unwrap());
            let mut argv = vec!["typedb-loader", "--duckdb", "x.duckdb", &option];
            if arg.get_action().takes_values() && arg.get_num_args().is_none_or(|range| range.min_values() > 0) {
                argv.push("1");
            }
            let err = Args::command().try_get_matches_from(&argv).expect_err(&format!("{option} was accepted"));
            assert_eq!(err.kind(), ErrorKind::ArgumentConflict, "{option}: {err}");
        }
    }
}
