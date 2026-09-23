/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    io::{self, BufRead, Write},
};

use crate::checkpoint::InFlightBatch;

enum InFlightMode {
    ReprocessAll,
    SkipAll,
    DecideEach,
    Restart,
    Abort,
}

/// The user's choice when resuming a run that has in-flight batches recorded.
pub(crate) enum InFlightDecision {
    /// Resume the prior checkpoint, treating the listed batch indices as already committed.
    Resume(HashSet<usize>),
    /// Discard the prior checkpoint and start over from the beginning.
    Restart,
    /// Exit the loader without running.
    Abort,
}

/// Asks the user how to handle batches the previous run dispatched but never confirmed as
/// committed.
pub(crate) fn resolve_in_flight_skips(in_flight: &[InFlightBatch]) -> InFlightDecision {
    if in_flight.is_empty() {
        return InFlightDecision::Resume(HashSet::new());
    }
    eprintln!("\nThe checkpoint records {} in-flight batch(es) from the previous run.", in_flight.len());
    eprintln!(
        "These batches were dispatched but never confirmed as committed. Verify them manually against the database before deciding."
    );
    for batch in in_flight {
        eprintln!("  - batch {} (first row: {})", batch.batch_index, format_first_row(&batch.first_row));
    }
    eprintln!("\nOptions:");
    eprintln!("  * [a]ll     = reprocess all (default)");
    eprintln!("  * [s]kip    = treat all as already committed");
    eprintln!("  * [d]ecide  = decide each batch individually");
    eprintln!("  * [r]estart = discard the checkpoint and start over");
    eprintln!("  * [q]uit    = abort the loader");
    let mode = loop {
        let choice = prompt("Choose action").trim().to_ascii_lowercase();
        match choice.as_str() {
            "" | "a" | "all" => break InFlightMode::ReprocessAll,
            "s" | "skip" | "skip all" => break InFlightMode::SkipAll,
            "d" | "each" | "decide" => break InFlightMode::DecideEach,
            "r" | "restart" => break InFlightMode::Restart,
            "q" | "quit" | "abort" => break InFlightMode::Abort,
            other => eprintln!("Unknown choice '{other}'. Please enter 'a', 's', 'd', 'r', or 'q'."),
        }
    };
    match mode {
        InFlightMode::ReprocessAll => InFlightDecision::Resume(HashSet::new()),
        InFlightMode::SkipAll => InFlightDecision::Resume(in_flight.iter().map(|b| b.batch_index).collect()),
        InFlightMode::DecideEach => InFlightDecision::Resume(
            in_flight
                .iter()
                .filter(|batch| {
                    let q = format!(
                        "Reprocess batch {} (first row: {})?",
                        batch.batch_index,
                        format_first_row(&batch.first_row)
                    );
                    !confirm(&q)
                })
                .map(|batch| batch.batch_index)
                .collect(),
        ),
        InFlightMode::Restart => InFlightDecision::Restart,
        InFlightMode::Abort => InFlightDecision::Abort,
    }
}

fn format_first_row(row: &[String]) -> String {
    if row.is_empty() { "<empty>".to_owned() } else { row.join(",") }
}

pub(crate) fn confirm(question: &str) -> bool {
    loop {
        let answer = prompt(&format!("{question} [y/N]")).trim().to_ascii_lowercase();
        match answer.as_str() {
            "y" | "yes" => return true,
            "" | "n" | "no" => return false,
            other => eprintln!("Please answer 'y' or 'n' (got '{other}')."),
        }
    }
}

fn prompt(message: &str) -> String {
    eprint!("{message}: ");
    let _ = io::stderr().flush();
    let mut line = String::new();
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    let _ = handle.read_line(&mut line);
    line
}
