/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    ExitCode,
    checkpoint::{Checkpoint, CheckpointParams, Hashes},
    cli::{Args, USERNAME_VALUE_NAME},
    fatal_with,
};

/// The settings the loader actually uses, after merging CLI args with values carried by a
/// checkpoint on resume. Fields here are always concrete (no `Option` for defaults) — the
/// merging happens in [`resolve_params`].
pub(crate) struct Params {
    pub query: String,
    pub database: String,
    pub data: String,
    pub header: bool,
    pub null_values: Vec<String>,
    pub max_rows: Option<usize>,
    pub batch_rows: usize,
    pub parallel_batches: usize,
    pub stop_on_error: bool,
    pub max_rejects: Option<usize>,
    pub schema_file: Option<String>,
    pub create_db: bool,
    pub addresses: String,
    pub username: String,
    pub tls_disabled: bool,
    pub tls_root_ca: Option<String>,
}

impl Params {
    pub(crate) fn to_checkpoint_params(&self) -> CheckpointParams {
        CheckpointParams {
            query: self.query.clone(),
            database: self.database.clone(),
            data: self.data.clone(),
            header: self.header,
            null_values: self.null_values.clone(),
            max_rows: self.max_rows,
            batch_rows: self.batch_rows,
            parallel_batches: self.parallel_batches,
            stop_on_error: self.stop_on_error,
            max_rejects: self.max_rejects,
            schema_file: self.schema_file.clone(),
            create_db: self.create_db,
            addresses: self.addresses.clone(),
            username: self.username.clone(),
            tls_disabled: self.tls_disabled,
            tls_root_ca: self.tls_root_ca.clone(),
        }
    }
}

/// Folds a `--no-X` opt-out flag into the value supplied by its companion `--X`. clap's
/// `conflicts_with` ensures both can't be set at once, so the `no_x = true` case wins iff `flag`
/// is `None`.
fn merge_no_flag(flag: Option<bool>, no_flag: bool) -> Option<bool> {
    flag.or(if no_flag { Some(false) } else { None })
}

pub(crate) fn resolve_params(args: &Args, checkpoint: Option<&CheckpointParams>) -> Result<Params, String> {
    // Hard-error if --batch-rows is provided on resume with a different value.
    if let (Some(cli_batch), Some(prior)) = (args.batch_rows, checkpoint) {
        if cli_batch != prior.batch_rows {
            return Err(format!(
                "--batch-rows ({cli_batch}) differs from the checkpoint ({}); it cannot change on resume.\nPossible solutions:\n  * omit --batch-rows to reuse the checkpoint's value\n  * start a fresh run (without '--resume') to change the batch size",
                prior.batch_rows
            ));
        }
    }

    let pick_string = |cli: &Option<String>, prior: Option<&String>, name: &str| -> Result<String, String> {
        match cli.as_ref().or(prior) {
            Some(v) => Ok(v.clone()),
            None => Err(format!("--{name} is required")),
        }
    };
    let pick_optional_string = |cli: &Option<String>, prior: Option<&Option<String>>| -> Option<String> {
        cli.clone().or_else(|| prior.cloned().flatten())
    };
    let pick_bool =
        |cli: Option<bool>, prior: Option<bool>, default: bool| -> bool { cli.or(prior).unwrap_or(default) };
    let pick_usize =
        |cli: Option<usize>, prior: Option<usize>, default: usize| -> usize { cli.or(prior).unwrap_or(default) };
    let pick_opt_usize =
        |cli: Option<usize>, prior: Option<Option<usize>>| -> Option<usize> { cli.or_else(|| prior.flatten()) };
    let pick_vec = |cli: &Option<Vec<String>>, prior: Option<&Vec<String>>| -> Vec<String> {
        cli.clone().or_else(|| prior.cloned()).unwrap_or_default()
    };

    let params = Params {
        query: pick_string(&args.query, checkpoint.map(|c| &c.query), "query")?,
        database: pick_string(&args.database, checkpoint.map(|c| &c.database), "database")?,
        data: pick_string(&args.data, checkpoint.map(|c| &c.data), "data")?,
        header: pick_bool(merge_no_flag(args.header, args.no_header), checkpoint.map(|c| c.header), false),
        null_values: pick_vec(&args.null_values, checkpoint.map(|c| &c.null_values)),
        // --max-rows 0 explicitly unsets the cap (e.g. when resuming with a different ceiling).
        // Without this escape hatch the value resumed from the checkpoint would be sticky.
        max_rows: match args.max_rows {
            Some(0) => None,
            Some(n) => Some(n),
            None => checkpoint.and_then(|c| c.max_rows),
        },
        batch_rows: pick_usize(args.batch_rows, checkpoint.map(|c| c.batch_rows), 1000),
        parallel_batches: pick_usize(args.parallel_batches, checkpoint.map(|c| c.parallel_batches), 1),
        stop_on_error: pick_bool(
            merge_no_flag(args.stop_on_error, args.no_stop_on_error),
            checkpoint.map(|c| c.stop_on_error),
            false,
        ),
        max_rejects: pick_opt_usize(args.max_rejects, checkpoint.map(|c| c.max_rejects)),
        schema_file: pick_optional_string(&args.schema_file, checkpoint.map(|c| &c.schema_file)),
        create_db: pick_bool(args.create_db, checkpoint.map(|c| c.create_db), false),
        addresses: pick_string(&args.addresses, checkpoint.map(|c| &c.addresses), "address")?,
        username: pick_string(&args.username, checkpoint.map(|c| &c.username), USERNAME_VALUE_NAME)?,
        tls_disabled: pick_bool(args.tls_disabled, checkpoint.map(|c| c.tls_disabled), false),
        tls_root_ca: pick_optional_string(&args.tls_root_ca, checkpoint.map(|c| &c.tls_root_ca)),
    };
    Ok(params)
}

/// Resolves params from CLI + checkpoint, enforces validity rules, and emits warnings for
/// args that are silently ignored on resume. Exits on any validation failure.
pub(crate) fn resolve_and_validate_params(args: &Args, resume_checkpoint: Option<&Checkpoint>) -> Params {
    let params = resolve_params(args, resume_checkpoint.map(|c| &c.params))
        .unwrap_or_else(|err| fatal_with(ExitCode::UserInputError, err));
    if params.batch_rows == 0 {
        fatal_with(ExitCode::UserInputError, "--batch-rows must be greater than 0");
    }
    if params.parallel_batches == 0 {
        fatal_with(ExitCode::UserInputError, "--parallel-batches must be greater than 0");
    }
    if resume_checkpoint.is_some() {
        if args.schema_file.is_some() {
            eprintln!("warning: --schema-file is ignored when resuming; the original schema query will not be re-run");
        }
        if args.create_db.unwrap_or(false) {
            eprintln!("warning: --create-db is ignored when resuming; the database is assumed to exist");
        }
    }
    params
}

/// Collects all the ways the current run's params or hashes diverge from the checkpoint. Pure;
/// the caller decides how to present them.
pub(crate) fn resume_warnings(params: &Params, prior: &Checkpoint, hashes: &Hashes) -> Vec<String> {
    let mut warnings: Vec<String> = Vec::new();

    if params.header != prior.params.header {
        warnings.push(format!(
            "--header changed since checkpoint ({} -> {}); CSV column interpretation may differ",
            prior.params.header, params.header
        ));
    }
    if params.null_values != prior.params.null_values {
        warnings.push(format!(
            "--null-values changed since checkpoint ({:?} -> {:?}); cell interpretation may differ",
            prior.params.null_values, params.null_values
        ));
    }
    if params.data != prior.params.data {
        warnings.push(format!("--data path changed since checkpoint ('{}' -> '{}')", prior.params.data, params.data));
    }
    if hashes.data != prior.hashes.data {
        warnings.push(format!(
            "data file hash mismatch: checkpoint expected {}, actual {}",
            prior.hashes.data, hashes.data
        ));
    }
    if hashes.schema != prior.hashes.schema {
        warnings.push(format!(
            "live TypeDB schema hash mismatch: checkpoint expected {}, actual {}",
            prior.hashes.schema, hashes.schema
        ));
    }
    if params.query != prior.params.query {
        warnings
            .push(format!("--query path changed since checkpoint ('{}' -> '{}')", prior.params.query, params.query));
    }
    if hashes.query != prior.hashes.query {
        warnings.push(format!(
            "query file content hash mismatch: checkpoint expected {}, actual {}",
            prior.hashes.query, hashes.query
        ));
    }
    if params.database != prior.params.database {
        warnings.push(format!(
            "--database changed since checkpoint ('{}' -> '{}')",
            prior.params.database, params.database
        ));
    }

    warnings
}
