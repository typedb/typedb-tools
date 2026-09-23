/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Parser;

pub const USERNAME_VALUE_NAME: &str = "username";

/// The two invocation modes: a fresh load (connection, database, query, and data are required),
/// and a resume (everything is carried by the checkpoint). Clap can't derive this because
/// required-ness is only resolved after merging CLI args with the checkpoint.
const USAGE: &str = "\
typedb-loader --address <host:port> --username <user> --database <db> --query <query.tql> --data <data.csv> [OPTIONS]
       typedb-loader --resume <output-dir> [OPTIONS]";

#[derive(Parser, Debug)]
#[command(author, about, override_usage = USAGE)]
pub struct Args {
    /// Path to a TypeQL query file used as the loading template.
    /// File path can be absolute or relative to the current directory.
    #[arg(long, value_name = "path to query file (.tql)")]
    pub query: Option<String>,

    /// Name of the database to load data into.
    #[arg(long, value_name = "database")]
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

    /// Directory to write the loader's output into: `rejects.csv`, `rejects.log`, and
    /// `checkpoint.json`. Created if it does not already exist.
    /// Defaults to `loader_<data-file-stem>_progress` next to the data file.
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
