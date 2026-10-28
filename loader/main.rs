/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::process::exit;

use clap::Parser;

use crate::{
    checkpoint::{CheckpointInit, compute_hashes, initialize_checkpoint},
    cli::Args,
    load::run_load,
    output::OutputConfiguration,
    params::resolve_and_validate_params,
    query::load_query,
    setup::connect_and_initialize,
    startup::{install_shutdown_handler, load_resume_checkpoint, prompt_password_if_missing},
};

mod checkpoint;
mod cli;
mod csv_reader;
mod load;
mod migration;
mod output;
mod params;
mod progress;
mod prompts;
mod query;
mod rejects;
mod setup;
mod startup;

pub(crate) const VERSION: &str = include_str!("../VERSION");

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if args.duckdb.is_some() {
        migration::run_migration(&args).await;
    } else {
        run_load_command(args).await;
    }
}

async fn run_load_command(args: Args) {
    let shutdown = install_shutdown_handler();

    let resume_checkpoint = load_resume_checkpoint(&args);
    let resuming = resume_checkpoint.is_some();
    if resuming && args.no_checkpoint {
        fatal_with(ExitCode::UserInputError, "--no-checkpoint cannot be combined with --resume");
    }

    let params = resolve_and_validate_params(&args, resume_checkpoint.as_ref());
    let password = prompt_password_if_missing(&args, &params.username);
    let (query_text, inputs) = load_query(&params.query);

    let driver = connect_and_initialize(&params, &password, resuming).await;
    let output_configuration = OutputConfiguration::create_for_load(&args, &params);
    output_configuration.prepare_output_dir();

    // Hashes are computed iff checkpointing is enabled; resume implies checkpointing, so any
    // resume path can rely on these being present.
    let hashes = if output_configuration.checkpoint_path.is_some() {
        Some(compute_hashes(&driver, &params.database, &params.data, &query_text).await)
    } else {
        None
    };

    let CheckpointInit { checkpoint, restarted } = initialize_checkpoint(&params, resume_checkpoint, hashes);
    if restarted {
        output_configuration.clear_for_restart();
    }
    let resuming = resuming && !restarted;

    if let Err(reason) =
        run_load(params, inputs, query_text, driver, checkpoint, output_configuration, resuming, shutdown).await
    {
        eprintln!("error: {reason}");
        exit(1);
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum ExitCode {
    GeneralError = 1,
    UserInputError = 2,
    ConnectionError = 3,
    MigrationError = 4,
}

pub(crate) fn fatal(message: impl AsRef<str>) -> ! {
    fatal_with(ExitCode::GeneralError, message)
}

pub(crate) fn fatal_with(code: ExitCode, message: impl AsRef<str>) -> ! {
    typedb_cli_common::println_error!("{}", message.as_ref());
    exit(code as i32);
}
