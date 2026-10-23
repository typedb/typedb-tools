/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use csv::StringRecord;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::task::JoinHandle;
use typedb_driver::{
    TransactionType, TypeDBDriver,
    answer::QueryAnswer,
    given::{GivenRowEntry, GivenRows, GivenRowsHeader},
};

use crate::{
    checkpoint::{Checkpoint, CheckpointWriter, InFlightBatch},
    csv_reader::{Batch, CsvReader, RowRejection},
    fatal,
    output::OutputConfiguration,
    params::Params,
    progress::{LoadStats, print_progress, print_summary},
    query::GivenSpec,
    rejects::RejectsWriter,
};

/// Executes the load: drives the CSV through parallel commits, persists checkpoints between
/// batches, and prints a final summary. Returns `Err(reason)` iff a stop condition was tripped
/// (Ctrl+C, --stop-on-error, --max-rejects); `Ok(())` is a clean finish.
pub(crate) async fn run_load(
    params: Params,
    inputs: Vec<GivenSpec>,
    query_text: String,
    driver: TypeDBDriver,
    checkpoint: Checkpoint,
    output_configuration: OutputConfiguration,
    resuming: bool,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    let OutputConfiguration { rejects_csv, rejects_log, checkpoint_path, .. } = output_configuration;

    let mut next_batch_index = checkpoint.watermark + 1;
    // Batches the prior run already completed (out-of-order) -- read past them but don't dispatch.
    let completed_above_watermark: HashSet<usize> =
        checkpoint.completed_above_watermark.iter().map(|c| c.batch_index).collect();

    let given_header = Arc::new(GivenRowsHeader::new(inputs.iter().map(|input| input.name.clone()).collect()));
    let mut csv_reader = CsvReader::open_for_load(&params, inputs, &checkpoint);
    let checkpoint_writer = CheckpointWriter::open_for_load(resuming, checkpoint_path);
    let rejects_writer =
        RejectsWriter::open_for_load(rejects_csv, rejects_log, csv_reader.headers().cloned(), resuming);
    let total_bytes = csv_reader.file_size();

    let driver = Arc::new(driver);
    let database: Arc<str> = Arc::from(params.database.as_str());
    let query_text: Arc<str> = Arc::from(query_text);

    let mut state = LoadState::new(checkpoint, rejects_writer, checkpoint_writer);
    // Persist the initial state so a fresh run leaves a checkpoint file even before the first
    // batch finishes.
    state.persist();

    let started = Instant::now();
    let mut producing = true;
    let mut in_flight: FuturesUnordered<JoinHandle<BatchResult>> = FuturesUnordered::new();

    loop {
        state.check_shutdown(&shutdown);
        while producing && !state.stop_requested() && in_flight.len() < params.parallel_batches {
            let batch = match csv_reader.next_batch(params.batch_rows) {
                Some(b) => b,
                None => {
                    producing = false;
                    break;
                }
            };
            let batch_index = next_batch_index;
            next_batch_index += 1;
            // Already-completed batches from a prior run: read past them to keep the cursor aligned.
            // (User-skipped in-flight batches were merged into completed_above_watermark above.)
            if completed_above_watermark.contains(&batch_index) {
                continue;
            }

            let first_row = batch.first_row.clone().unwrap_or_default();
            let byte_end = batch.byte_end;
            state.record_dispatch(InFlightBatch { batch_index, byte_end, first_row });

            let driver = driver.clone();
            let database = database.clone();
            let query_text = query_text.clone();
            let given_header = given_header.clone();
            in_flight.push(tokio::spawn(async move {
                process_batch(driver, database, query_text, given_header, batch_index, batch).await
            }));
        }

        let Some(joined) = in_flight.next().await else { break };
        let result = joined.unwrap_or_else(|err| fatal(format!("batch task panicked: {err}")));
        state.apply_batch_result(&params, result);

        print_progress(state.stats(), started, csv_reader.bytes_position(), total_bytes);
    }

    state.finalize(started)
}

/// Mutable load-time state that accumulates across batches: stats, rejects, the in-flight
/// checkpoint, the one-shot stop request, and the writer that persists the checkpoint after
/// each transition.
struct LoadState {
    stats: LoadStats,
    rejects: RejectsWriter,
    stop: Stop,
    checkpoint: Checkpoint,
    checkpoint_writer: Option<CheckpointWriter>,
}

impl LoadState {
    fn new(checkpoint: Checkpoint, rejects: RejectsWriter, checkpoint_writer: Option<CheckpointWriter>) -> Self {
        Self { stats: LoadStats::default(), rejects, stop: Stop::default(), checkpoint, checkpoint_writer }
    }

    fn stats(&self) -> &LoadStats {
        &self.stats
    }

    fn stop_requested(&self) -> bool {
        self.stop.requested()
    }

    fn check_shutdown(&mut self, shutdown: &AtomicBool) {
        if !self.stop.requested() && shutdown.load(Ordering::SeqCst) {
            self.stop.request("aborted: interrupted by user");
        }
    }

    fn record_dispatch(&mut self, batch: InFlightBatch) {
        self.checkpoint.record_dispatch(batch);
        self.persist();
    }

    /// Folds one completed batch into the accumulating state: logs and records rejections,
    /// updates stats, records a commit failure, advances the checkpoint, and trips stop
    /// conditions (--stop-on-error, --max-rejects).
    fn apply_batch_result(&mut self, params: &Params, result: BatchResult) {
        self.stats.rows_attempted += result.rows_attempted;
        for rejection in &result.parse_rejected {
            eprintln!("row {}: {}", rejection.row_number, rejection.message);
            self.rejects
                .record_row(rejection.row_number, rejection.record.as_ref(), &rejection.message)
                .unwrap_or_else(|err| fatal(err));
        }
        self.stats.rows_rejected += result.parse_rejected.len();
        if params.stop_on_error && !result.parse_rejected.is_empty() {
            self.stop.request("aborted due to --stop-on-error");
        }

        if result.parsed_count > 0 {
            match result.commit_result {
                Ok(()) => self.stats.rows_committed += result.parsed_count,
                Err(err) => {
                    eprintln!("batch {}: {} rows rejected by commit: {err}", result.batch_index, result.parsed_count);
                    self.rejects
                        .record_batch_failure(
                            &result.parsed_row_numbers,
                            &result.parsed_records,
                            result.batch_index,
                            &err,
                        )
                        .unwrap_or_else(|err| fatal(err));
                    self.stats.rows_rejected += result.parsed_count;
                    if params.stop_on_error {
                        self.stop.request("aborted due to --stop-on-error");
                    }
                }
            }
        }

        // Record the finish regardless of success/failure -- the row data has been recorded
        // one way or another (committed, or written to the rejects file).
        self.checkpoint.record_finish(result.batch_index);
        self.persist();

        if let Some(limit) = params.max_rejects {
            if self.stats.rows_rejected > limit {
                self.stop.request(&format!(
                    "aborted: rejected rows ({}) exceeded --max-rejects {}",
                    self.stats.rows_rejected, limit
                ));
            }
        }
    }

    fn persist(&self) {
        if let Some(writer) = &self.checkpoint_writer {
            writer.write(&self.checkpoint).unwrap_or_else(|err| fatal(err));
        }
    }

    /// Consumes self, flushes rejects, prints the final summary and artifact paths, and turns
    /// any pending stop reason into the outcome's Result.
    fn finalize(mut self, started: Instant) -> Result<(), String> {
        self.rejects.flush().unwrap_or_else(|err| fatal(err));
        print_summary(&self.stats, started);
        if self.rejects.was_written() {
            println!("  Rejects CSV:    {}", self.rejects.csv_path().display());
            println!("  Rejects log:    {}", self.rejects.log_path().display());
        }
        if let Some(writer) = &self.checkpoint_writer {
            println!("  Checkpoint:     {}", writer.path().display());
        }
        match self.stop.into_reason() {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }
}

/// Tracks a one-shot stop request: the first reason wins, so messages produced by later
/// stop conditions don't overwrite the trigger that started the drain.
#[derive(Default)]
struct Stop {
    reason: Option<String>,
}

impl Stop {
    fn requested(&self) -> bool {
        self.reason.is_some()
    }

    fn request(&mut self, reason: &str) {
        if self.reason.is_none() {
            self.reason = Some(reason.to_owned());
        }
    }

    fn into_reason(self) -> Option<String> {
        self.reason
    }
}

struct BatchResult {
    batch_index: usize,
    rows_attempted: usize,
    parse_rejected: Vec<RowRejection>,
    parsed_row_numbers: Vec<usize>,
    parsed_records: Vec<StringRecord>,
    parsed_count: usize,
    commit_result: Result<(), String>,
}

async fn process_batch(
    driver: Arc<TypeDBDriver>,
    database: Arc<str>,
    query_text: Arc<str>,
    given_header: Arc<GivenRowsHeader>,
    batch_index: usize,
    batch: Batch,
) -> BatchResult {
    let parsed_count = batch.rows.len();
    let commit_result = if parsed_count > 0 {
        commit_batch(&driver, &database, &query_text, given_header, batch_index, batch.rows).await
    } else {
        Ok(())
    };
    BatchResult {
        batch_index,
        rows_attempted: batch.rows_attempted,
        parse_rejected: batch.rejected,
        parsed_row_numbers: batch.row_numbers,
        parsed_records: batch.records,
        parsed_count,
        commit_result,
    }
}

async fn commit_batch(
    driver: &TypeDBDriver,
    database: &str,
    query: &str,
    given_header: Arc<GivenRowsHeader>,
    batch_index: usize,
    rows: Vec<Vec<GivenRowEntry>>,
) -> Result<(), String> {
    let mut given_rows = GivenRows::new_with_headers(given_header, rows.len());
    for row in rows {
        given_rows.push_row(row).map_err(|err| format!("building given rows: {err}"))?;
    }
    let transaction = driver
        .transaction(database.to_owned(), TransactionType::Write)
        .await
        .map_err(|err| format!("opening write transaction on '{database}': {err}"))?;
    let answer = transaction.query_with_rows(query, given_rows).await.map_err(|err| format!("query failed: {err}"))?;
    let has_any = match answer {
        QueryAnswer::Ok(_) => false,
        QueryAnswer::ConceptRowStream(_, mut stream) => stream.next().await.is_some(),
        QueryAnswer::ConceptDocumentStream(_, mut stream) => stream.next().await.is_some(),
    };
    if !has_any {
        typedb_cli_common::println_warning!("batch {batch_index} inserted zero rows");
    }
    transaction.commit().await.map_err(|err| format!("commit failed: {err}"))?;
    Ok(())
}
