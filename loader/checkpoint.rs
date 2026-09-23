/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use typedb_driver::TypeDBDriver;

use crate::{
    ExitCode, fatal, fatal_with,
    params::{Params, resume_warnings},
    prompts::{InFlightDecision, confirm, resolve_in_flight_skips},
};

pub(crate) const CHECKPOINT_VERSION: u32 = 1;

/// Hashes captured at the start of a run for integrity-checking on resume. All three are taken
/// together: a run either has all hashes or none (when checkpointing is disabled).
///
/// Fields are flattened into the parent struct on the wire so the JSON layout is unchanged from
/// when these lived directly on `Checkpoint`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Hashes {
    #[serde(rename = "query_hash")]
    pub query: String,
    #[serde(rename = "data_hash")]
    pub data: String,
    #[serde(rename = "schema_hash")]
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CheckpointParams {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InFlightBatch {
    pub batch_index: usize,
    pub byte_end: u64,
    pub first_row: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CompletedBatch {
    pub batch_index: usize,
    pub byte_end: u64,
}

/// Tracks committed and in-flight batches and maintains a watermark = the highest batch index
/// such that every batch <= it has finished. Non-consecutive completions are parked in
/// `completed_above_watermark` until the watermark catches up.
///
/// Sole owner is the main task; mutated and serialised from one thread, so the vectors stay
/// small (bounded by `parallel_batches`) and linear scans are cheap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub version: u32,
    pub params: CheckpointParams,
    #[serde(flatten)]
    pub hashes: Hashes,
    pub watermark: usize,
    pub watermark_bytes: u64,
    pub completed_above_watermark: Vec<CompletedBatch>,
    pub in_flight: Vec<InFlightBatch>,
}

impl Checkpoint {
    pub(crate) fn new(params: CheckpointParams, hashes: Hashes) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            params,
            hashes,
            watermark: 0,
            watermark_bytes: 0,
            completed_above_watermark: Vec::new(),
            in_flight: Vec::new(),
        }
    }

    pub(crate) fn load(path: &Path) -> Result<Self, String> {
        let file = File::open(path).map_err(|err| format!("opening checkpoint '{}': {err}", path.display()))?;
        let checkpoint: Checkpoint = serde_json::from_reader(BufReader::new(file))
            .map_err(|err| format!("parsing checkpoint '{}': {err}", path.display()))?;
        if checkpoint.version != CHECKPOINT_VERSION {
            return Err(format!(
                "checkpoint '{}' is version {} but loader expects version {}",
                path.display(),
                checkpoint.version,
                CHECKPOINT_VERSION
            ));
        }
        Ok(checkpoint)
    }

    pub(crate) fn set_hashes(&mut self, hashes: Hashes) {
        self.hashes = hashes;
    }

    pub(crate) fn record_dispatch(&mut self, batch: InFlightBatch) {
        self.in_flight.push(batch);
    }

    /// Records that a batch has finished (either committed or rejected). The `byte_end` is taken
    /// from the prior dispatch record. If the finished batch is contiguous with the watermark,
    /// the watermark slides forward, also absorbing any previously-parked completions.
    pub(crate) fn record_finish(&mut self, batch_index: usize) {
        let Some(byte_end) = take_in_flight(&mut self.in_flight, batch_index) else {
            return;
        };
        self.absorb_finished_batch(batch_index, byte_end);
    }

    /// Drops an in-flight entry without dispatching it. Used on resume when the user chooses to
    /// treat an in-flight batch as already committed. Advances the watermark just like a
    /// normal completion would.
    pub(crate) fn mark_in_flight_as_skipped(&mut self, batch_index: usize) {
        if let Some(byte_end) = take_in_flight(&mut self.in_flight, batch_index) {
            self.absorb_finished_batch(batch_index, byte_end);
        }
    }

    /// Applies the user's resume-time decisions about in-flight batches: those in `skipped` are
    /// treated as already committed (watermark advances), and any remaining in-flight entries
    /// are dropped so the upcoming re-dispatch doesn't leave the prior records as ghosts that
    /// would later be mistaken for the new dispatches (same batch_index, stale byte_end).
    pub(crate) fn apply_in_flight_decisions(&mut self, skipped: &HashSet<usize>) {
        for &index in skipped {
            self.mark_in_flight_as_skipped(index);
        }
        self.in_flight.clear();
    }

    fn absorb_finished_batch(&mut self, batch_index: usize, byte_end: u64) {
        if batch_index == self.watermark + 1 {
            self.watermark = batch_index;
            self.watermark_bytes = byte_end;
            while let Some(byte_end) = take_completed(&mut self.completed_above_watermark, self.watermark + 1) {
                self.watermark += 1;
                self.watermark_bytes = byte_end;
            }
        } else {
            self.completed_above_watermark.push(CompletedBatch { batch_index, byte_end });
        }
    }
}

fn take_in_flight(in_flight: &mut Vec<InFlightBatch>, batch_index: usize) -> Option<u64> {
    let pos = in_flight.iter().position(|b| b.batch_index == batch_index)?;
    Some(in_flight.swap_remove(pos).byte_end)
}

fn take_completed(completed: &mut Vec<CompletedBatch>, batch_index: usize) -> Option<u64> {
    let pos = completed.iter().position(|c| c.batch_index == batch_index)?;
    Some(completed.swap_remove(pos).byte_end)
}

pub(crate) struct CheckpointWriter {
    path: PathBuf,
}

impl CheckpointWriter {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn open_for_load(resuming: bool, checkpoint_path: Option<PathBuf>) -> Option<Self> {
        checkpoint_path.map(|checkpoint_path| {
            if !resuming && checkpoint_path.exists() {
                fatal_with(
                    ExitCode::UserInputError,
                    format!(
                        "checkpoint already exists at '{}'.\nPossible solutions:\n  * pass '--resume {}' to continue from it\n  * pass '--output-dir <path>' to write elsewhere\n  * pass '--no-checkpoint' to disable checkpointing",
                        checkpoint_path.display(),
                        checkpoint_path.parent().unwrap().display()
                    ),
                );
            }
            CheckpointWriter::new(checkpoint_path)
        })
    }

    /// Writes the checkpoint atomically: serialise to a sibling temp file, fsync, rename over the
    /// target. A crash mid-write leaves the previous good checkpoint in place.
    pub(crate) fn write(&self, checkpoint: &Checkpoint) -> Result<(), String> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let temp_name = format!(
            ".{}.tmp.{}",
            self.path.file_name().and_then(|s| s.to_str()).unwrap_or("checkpoint"),
            std::process::id()
        );
        let temp_path = parent.join(temp_name);
        let payload = serde_json::to_vec_pretty(checkpoint).map_err(|err| format!("serialising checkpoint: {err}"))?;
        {
            let mut file = File::create(&temp_path)
                .map_err(|err| format!("creating checkpoint temp file '{}': {err}", temp_path.display()))?;
            file.write_all(&payload)
                .map_err(|err| format!("writing checkpoint temp file '{}': {err}", temp_path.display()))?;
            file.sync_all().map_err(|err| format!("fsync checkpoint temp file '{}': {err}", temp_path.display()))?;
        }
        fs::rename(&temp_path, &self.path).map_err(|err| {
            let _ = fs::remove_file(&temp_path);
            format!("renaming checkpoint into place at '{}': {err}", self.path.display())
        })?;
        Ok(())
    }
}

/// How many bytes of the data file to hash for the checkpoint integrity check. Hashing the whole
/// file would dominate startup time on multi-GB inputs; the prefix plus the file's total length
/// is enough to catch any realistic accidental change (truncation, prepend, header edit, swap).
pub(crate) const HASH_PREFIX_BYTES: u64 = 64 * 1024 * 1024;

/// Hashes the first `HASH_PREFIX_BYTES` of the file and combines the result with the file's total
/// length so that two files sharing a prefix but differing in size produce different hashes.
pub(crate) fn hash_file(path: &Path) -> Result<String, String> {
    let file = File::open(path).map_err(|err| format!("opening '{}' for hashing: {err}", path.display()))?;
    let total_size = file.metadata().map_err(|err| format!("reading '{}' metadata: {err}", path.display()))?.len();
    let mut reader = BufReader::new(file).take(HASH_PREFIX_BYTES);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).map_err(|err| format!("reading '{}' for hashing: {err}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("sha256:{total_size}:{:x}", hasher.finalize()))
}

pub(crate) fn hash_string(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

/// Prepares the checkpoint for the run: either resumes from a prior checkpoint (with
/// freshened hashes and any user-chosen in-flight skips applied), or builds a fresh checkpoint.
/// Interactive: shows resume warnings and prompts for confirmation when params have drifted,
/// and asks per-in-flight whether to skip or retry.
pub(crate) struct CheckpointInit {
    pub checkpoint: Checkpoint,
    /// True iff the user chose Restart at the in-flight prompt: caller must treat the run as
    /// fresh (no resume), and clear any prior output files in the output dir.
    pub restarted: bool,
}

pub(crate) fn initialize_checkpoint(
    params: &Params,
    resume_checkpoint: Option<Checkpoint>,
    hashes: Option<Hashes>,
) -> CheckpointInit {
    let decision = if let Some(prior) = resume_checkpoint.as_ref() {
        let hashes = hashes.as_ref().expect("resume requires checkpointing, which always produces hashes");
        let warnings = resume_warnings(params, prior, hashes);
        if !warnings.is_empty() {
            eprintln!("\nResume warnings:");
            for w in &warnings {
                eprintln!("  - {w}");
            }
            if !confirm("Continue anyway?") {
                fatal_with(ExitCode::UserInputError, "aborted: resume cancelled by user");
            }
        }
        resolve_in_flight_skips(&prior.in_flight)
    } else {
        InFlightDecision::Resume(HashSet::new())
    };

    let skipped_in_flight = match decision {
        InFlightDecision::Resume(skipped) => skipped,
        InFlightDecision::Restart => {
            return CheckpointInit {
                checkpoint: Checkpoint::new(params.to_checkpoint_params(), hashes.unwrap_or_default()),
                restarted: true,
            };
        }
        InFlightDecision::Abort => fatal_with(ExitCode::UserInputError, "aborted: resume cancelled by user"),
    };

    let checkpoint = match resume_checkpoint {
        Some(mut prior) => {
            if let Some(hashes) = hashes.as_ref() {
                prior.set_hashes(hashes.clone());
            }
            prior.apply_in_flight_decisions(&skipped_in_flight);
            prior
        }
        None => Checkpoint::new(params.to_checkpoint_params(), hashes.unwrap_or_default()),
    };
    CheckpointInit { checkpoint, restarted: false }
}

/// Computes the three integrity hashes for the current run by combining the static data/query
/// hashes with the live schema fetched from the driver. Exits on any I/O failure.
pub(crate) async fn compute_hashes(driver: &TypeDBDriver, database: &str, data_path: &str, query_text: &str) -> Hashes {
    println!("Hashing data file (first 64 MB)...");
    let data = hash_file(Path::new(data_path)).unwrap_or_else(|err| fatal(err));
    println!("Fetching live schema for hashing...");
    let db = driver
        .databases()
        .get(database.to_owned())
        .await
        .unwrap_or_else(|err| fatal(format!("failed to look up database '{database}': {err}")));
    let schema_text = db.schema().await.unwrap_or_else(|err| fatal(format!("failed to fetch live schema: {err}")));
    Hashes { query: hash_string(query_text), data, schema: hash_string(&schema_text) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stub_params() -> CheckpointParams {
        CheckpointParams {
            query: String::new(),
            database: String::new(),
            data: String::new(),
            header: false,
            null_values: Vec::new(),
            max_rows: None,
            batch_rows: 0,
            parallel_batches: 0,
            stop_on_error: false,
            max_rejects: None,
            schema_file: None,
            create_db: false,
            addresses: String::new(),
            username: String::new(),
            tls_disabled: false,
            tls_root_ca: None,
        }
    }

    fn in_flight(batch_index: usize, byte_end: u64) -> InFlightBatch {
        InFlightBatch { batch_index, byte_end, first_row: Vec::new() }
    }

    /// Builds a checkpoint that looks like the previous run dispatched some batches past the
    /// current watermark and crashed before any of them finished.
    fn checkpoint_with_in_flight(in_flights: Vec<InFlightBatch>, watermark: usize, watermark_bytes: u64) -> Checkpoint {
        let mut checkpoint = Checkpoint::new(stub_params(), Hashes::default());
        checkpoint.watermark = watermark;
        checkpoint.watermark_bytes = watermark_bytes;
        checkpoint.in_flight = in_flights;
        checkpoint
    }

    #[test]
    fn reprocess_all_clears_in_flight_without_touching_watermark() {
        // Reprocess-all => skipped set is empty; every in-flight entry must be dropped so the
        // re-dispatch doesn't create duplicate entries with the same batch_index.
        let mut checkpoint = checkpoint_with_in_flight(vec![in_flight(5, 100), in_flight(6, 200)], 4, 80);
        checkpoint.apply_in_flight_decisions(&HashSet::new());
        assert!(checkpoint.in_flight.is_empty(), "stale dispatch records must be cleared");
        assert_eq!(checkpoint.watermark, 4, "watermark must not advance when nothing is skipped");
        assert_eq!(checkpoint.watermark_bytes, 80);
        assert!(checkpoint.completed_above_watermark.is_empty());
    }

    #[test]
    fn skip_all_advances_watermark_through_contiguous_batches() {
        // Skip-all over a contiguous range starting at watermark+1: watermark slides through
        // each one, watermark_bytes ends at the final batch's byte_end.
        let mut checkpoint = checkpoint_with_in_flight(vec![in_flight(5, 100), in_flight(6, 200)], 4, 80);
        let skipped: HashSet<usize> = [5, 6].into_iter().collect();
        checkpoint.apply_in_flight_decisions(&skipped);
        assert!(checkpoint.in_flight.is_empty());
        assert_eq!(checkpoint.watermark, 6);
        assert_eq!(checkpoint.watermark_bytes, 200);
        assert!(checkpoint.completed_above_watermark.is_empty());
    }

    #[test]
    fn decide_each_advances_skipped_and_drops_the_rest() {
        // Decide-each where only batch 5 (the next-after-watermark) is skipped: watermark
        // advances to 5; batches 6 and 7 were chosen for reprocess, so their stale records
        // must be dropped — they'll re-appear in `in_flight` once the re-dispatch reaches them.
        let mut checkpoint =
            checkpoint_with_in_flight(vec![in_flight(5, 100), in_flight(6, 200), in_flight(7, 300)], 4, 80);
        let skipped: HashSet<usize> = [5].into_iter().collect();
        checkpoint.apply_in_flight_decisions(&skipped);
        assert!(checkpoint.in_flight.is_empty(), "reprocess-chosen batches must not remain as ghosts");
        assert_eq!(checkpoint.watermark, 5);
        assert_eq!(checkpoint.watermark_bytes, 100);
        assert!(checkpoint.completed_above_watermark.is_empty());
    }

    #[test]
    fn decide_each_skipping_non_contiguous_batch_parks_in_completed_above_watermark() {
        // Skip batch 6 but reprocess batch 5: the watermark can't advance past 4 yet (since 5
        // isn't done), so 6 lands in completed_above_watermark to be absorbed when 5 finishes.
        let mut checkpoint = checkpoint_with_in_flight(vec![in_flight(5, 100), in_flight(6, 200)], 4, 80);
        let skipped: HashSet<usize> = [6].into_iter().collect();
        checkpoint.apply_in_flight_decisions(&skipped);
        assert!(checkpoint.in_flight.is_empty());
        assert_eq!(checkpoint.watermark, 4, "5 still pending => watermark stays at 4");
        assert_eq!(checkpoint.watermark_bytes, 80);
        assert_eq!(checkpoint.completed_above_watermark.len(), 1);
        assert_eq!(checkpoint.completed_above_watermark[0].batch_index, 6);
        assert_eq!(checkpoint.completed_above_watermark[0].byte_end, 200);
    }

    #[test]
    fn record_dispatch_after_reprocess_decision_does_not_collide_with_prior_entry() {
        // End-to-end shape of the bug: simulate the bug scenario as it would unfold in
        // run_load. The prior run left batch 5 in-flight; user picks reprocess-all; the new
        // run dispatches batch 5 again and then records its finish. Without the fix, finish
        // would pop the STALE entry and advance watermark using the wrong byte_end.
        let mut checkpoint = checkpoint_with_in_flight(vec![in_flight(5, 100)], 4, 80);
        checkpoint.apply_in_flight_decisions(&HashSet::new());
        // Re-dispatch with the new byte_end.
        checkpoint.record_dispatch(in_flight(5, 999));
        checkpoint.record_finish(5);
        assert_eq!(checkpoint.watermark, 5);
        assert_eq!(checkpoint.watermark_bytes, 999, "finish must use the NEW dispatch's byte_end, not the stale one");
        assert!(checkpoint.in_flight.is_empty());
    }
}
