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

use crate::{ExitCode, checkpoint::Checkpoint, cli::Args, fatal, fatal_with, output::CHECKPOINT_FILENAME};

/// Installs the interrupt handler. The first interrupt flips the returned flag so the main loop can
/// drain in-flight batches and write a final checkpoint; a second interrupt force-exits.
pub(crate) fn install_shutdown_handler() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut signals = match Termination::listen() {
        Ok(signals) => signals,
        Err(err) => {
            eprintln!("warning: interrupts will not be handled: {err}");
            return shutdown;
        }
    };
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let Some(_) = signals.recv().await else { return };
        shutdown_signal.store(true, Ordering::SeqCst);
        eprintln!("\nInterrupt received; finishing in-flight batches. Interrupt again to force exit.");
        let Some(exit_code) = signals.recv().await else { return };
        eprintln!("Force-exiting.");
        std::process::exit(exit_code);
    });
    shutdown
}

// SIGINT (Ctrl+C) and SIGTERM (`kill`, systemd, Kubernetes, `timeout`) are treated alike, as command-line tools
// commonly do. The handlers are registered at once, not when a task first polls them; `recv` resolves to the
// exit code a shell reports for the signal.
#[cfg(unix)]
pub(crate) struct Termination {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Termination {
    pub(crate) fn listen() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self { interrupt: signal(SignalKind::interrupt())?, terminate: signal(SignalKind::terminate())? })
    }

    pub(crate) async fn recv(&mut self) -> Option<i32> {
        tokio::select! {
            received = self.interrupt.recv() => received.map(|()| 130),
            received = self.terminate.recv() => received.map(|()| 143),
        }
    }
}

#[cfg(not(unix))]
pub(crate) struct Termination;

#[cfg(not(unix))]
impl Termination {
    pub(crate) fn listen() -> io::Result<Self> {
        Ok(Self)
    }

    pub(crate) async fn recv(&mut self) -> Option<i32> {
        tokio::signal::ctrl_c().await.ok().map(|()| 130)
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
