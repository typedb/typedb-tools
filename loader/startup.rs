/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use typedb_cli_common::println_warning;

use crate::{ExitCode, checkpoint::Checkpoint, cli::Args, fatal, fatal_with, output::CHECKPOINT_FILENAME};

/// Installs the interrupt handler. The first interrupt flips the returned flag so the main loop can
/// drain in-flight batches and write a final checkpoint; a second interrupt force-exits.
pub(crate) fn install_shutdown_handler() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut termination = Termination::listen();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        termination.next().await;
        shutdown_signal.store(true, Ordering::SeqCst);
        eprintln!("\nInterrupt received; finishing in-flight batches. Interrupt again to force exit.");
        let exit_code = termination.next().await;
        eprintln!("Force-exiting.");
        std::process::exit(exit_code);
    });
    shutdown
}

// SIGINT and SIGTERM alike, as the exit code a shell reports for each. Never one when they cannot be handled.
pub(crate) struct Termination(Option<Signals>);

impl Termination {
    pub(crate) fn listen() -> Self {
        match Signals::register() {
            Ok(signals) => Termination(Some(signals)),
            Err(err) => {
                println_warning!("interrupts will not be handled: {err}");
                Termination(None)
            }
        }
    }

    pub(crate) async fn next(&mut self) -> i32 {
        match &mut self.0 {
            Some(signals) => signals.next().await,
            None => std::future::pending().await,
        }
    }
}

#[cfg(unix)]
struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn register() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self { interrupt: signal(SignalKind::interrupt())?, terminate: signal(SignalKind::terminate())? })
    }

    async fn next(&mut self) -> i32 {
        tokio::select! {
            Some(()) = self.interrupt.recv() => 130,
            Some(()) = self.terminate.recv() => 143,
            else => std::future::pending().await,
        }
    }
}

#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn register() -> io::Result<Self> {
        Ok(Self)
    }

    async fn next(&mut self) -> i32 {
        match tokio::signal::ctrl_c().await {
            Ok(()) => 130,
            Err(_) => std::future::pending().await,
        }
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
