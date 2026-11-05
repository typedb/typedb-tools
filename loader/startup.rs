/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{ExitCode, checkpoint::Checkpoint, cli::Args, fatal, fatal_with, output::CHECKPOINT_FILENAME};

// What a shell reports for a process ended by the signal: 128 and the signal's number.
const SIGINT_EXIT_CODE: i32 = 130;
#[cfg(unix)]
const SIGTERM_EXIT_CODE: i32 = 143;

/// Installs the interrupt handler. The first interrupt flips the returned flag so the main loop can
/// drain in-flight batches and write a final checkpoint; a second interrupt force-exits.
pub(crate) fn install_shutdown_handler() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut termination = Termination::listen();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        termination.next_exit_code().await;
        shutdown_signal.store(true, Ordering::SeqCst);
        eprintln!("\nInterrupt received; finishing in-flight batches. Interrupt again to force exit.");
        let exit_code = termination.next_exit_code().await;
        eprintln!("Force-exiting.");
        std::process::exit(exit_code);
    });
    shutdown
}

// SIGINT and SIGTERM alike, as the exit code a shell reports for each. Once listening, none is missed.
#[cfg(unix)]
pub(crate) struct Termination {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Termination {
    pub(crate) fn listen() -> Self {
        use tokio::signal::unix::{SignalKind, signal};
        Self {
            interrupt: signal(SignalKind::interrupt()).expect("failed to install the SIGINT handler"),
            terminate: signal(SignalKind::terminate()).expect("failed to install the SIGTERM handler"),
        }
    }

    pub(crate) async fn next_exit_code(&mut self) -> i32 {
        tokio::select! {
            Some(()) = self.interrupt.recv() => SIGINT_EXIT_CODE,
            Some(()) = self.terminate.recv() => SIGTERM_EXIT_CODE,
            else => std::future::pending().await,
        }
    }
}

#[cfg(not(unix))]
pub(crate) struct Termination;

#[cfg(not(unix))]
impl Termination {
    pub(crate) fn listen() -> Self {
        Self
    }

    pub(crate) async fn next_exit_code(&mut self) -> i32 {
        tokio::signal::ctrl_c().await.expect("failed to listen for Ctrl+C");
        SIGINT_EXIT_CODE
    }
}

/// Loads the prior checkpoint from --resume. Exits on conflict with --output-dir
/// (the resume directory IS the output directory) or on unreadable checkpoint.
pub(crate) fn load_resume_checkpoint(args: &Args) -> Option<Checkpoint> {
    if args.resume.is_some() && args.output_dir.is_some() {
        fatal_with(
            ExitCode::UserInputError,
            "--output-dir cannot be combined with --resume; the resume directory is the output directory",
        );
    }
    args.resume.as_deref().map(|dir| {
        let path = Path::new(dir).join(CHECKPOINT_FILENAME);
        Checkpoint::load(&path).unwrap_or_else(|err| fatal(err))
    })
}

/// Returns the password supplied via --password, falling back to an interactive prompt that
/// echoes the username so the user knows which credential is being requested.
pub(crate) fn prompt_password_if_missing(args: &Args, username: &str) -> String {
    args.password.clone().unwrap_or_else(|| rpassword::prompt_password(format!("password for '{username}': ")).unwrap())
}
