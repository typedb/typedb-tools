/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::BTreeMap,
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Mutex, mpsc},
    thread,
};

use duckdb::{Statement, arrow::record_batch::RecordBatch};

use crate::Cancellation;

// A chunk is cut by rows or by bytes, whichever comes first, so that wide rows do not inflate what is in flight.
const CHUNK_ROWS: usize = 32_768;
const CHUNK_BYTES: usize = 32 << 20;
const CHUNKS_IN_FLIGHT_PER_WORKER: usize = 4;

// Streamed, as a whole result would exceed the memory limit: this thread reads, `encode` runs on every worker, and
// `write` receives the chunks in the order they were read. Each chunk read takes a permit, returned once written,
// which bounds the chunks waiting for an earlier one.
pub(crate) fn encode_in_order<Encoded: Send>(
    chunks: ChunkReader<'_>,
    encode: &(impl Fn(&[RecordBatch]) -> Result<Encoded, String> + Sync),
    workers: usize,
    mut write: impl FnMut(Encoded) -> Result<(), String> + Send,
) -> Result<(), String> {
    let (work_sender, work_receiver) = mpsc::sync_channel::<(u64, Vec<RecordBatch>)>(workers);
    let (encoded_sender, encoded_receiver) = mpsc::sync_channel::<(u64, Result<Encoded, String>)>(workers);
    let (permit_sender, permits) = mpsc::sync_channel::<()>(CHUNKS_IN_FLIGHT_PER_WORKER * workers);
    for _ in 0..CHUNKS_IN_FLIGHT_PER_WORKER * workers {
        permit_sender.send(()).expect("the permit channel has room for every permit");
    }
    thread::scope(|scope| {
        // Only the workers hold the receiver, so sending fails rather than blocks once they have all left.
        let work_receiver = Arc::new(Mutex::new(work_receiver));
        for _ in 0..workers {
            let (work_receiver, encoded_sender) = (work_receiver.clone(), encoded_sender.clone());
            scope.spawn(move || encode_received(&work_receiver, &encoded_sender, encode));
        }
        drop((work_receiver, encoded_sender));
        let writer = scope.spawn(move || -> Result<(), String> {
            let (mut pending, mut next_sequence) = (BTreeMap::new(), 0);
            for (sequence, chunk) in encoded_receiver {
                pending.insert(sequence, chunk?);
                while let Some(chunk) = pending.remove(&next_sequence) {
                    write(chunk)?;
                    next_sequence += 1;
                    let _ = permit_sender.send(());
                }
            }
            Ok(())
        });
        let mut read_error = None;
        for (sequence, chunk) in (0..).zip(chunks) {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    read_error = Some(err);
                    break;
                }
            };
            let is_sent = permits.recv().is_ok() && work_sender.send((sequence, chunk)).is_ok();
            if !is_sent {
                break;
            }
        }
        drop(work_sender);
        let written = writer.join().unwrap_or_else(|_| Err("the output writer panicked".to_owned()));
        match read_error {
            Some(err) => Err(err),
            None => written,
        }
    })
}

pub(crate) struct ChunkReader<'connection> {
    statement: Statement<'connection>,
    cancellation: &'connection Cancellation,
}

impl<'connection> ChunkReader<'connection> {
    pub(crate) fn start(
        mut statement: Statement<'connection>,
        cancellation: &'connection Cancellation,
    ) -> Result<Self, String> {
        if cancellation.is_cancelled() {
            return Err("cancelled".to_owned());
        }
        // Stepping the statement, rather than iterating its stream, reports a fetch error on every duckdb-rs version.
        let _ = statement.stream_arrow([]).map_err(|err| err.to_string())?;
        Ok(ChunkReader { statement, cancellation })
    }
}

impl Iterator for ChunkReader<'_> {
    type Item = Result<Vec<RecordBatch>, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::new();
        let (mut rows, mut bytes) = (0, 0);
        while rows < CHUNK_ROWS && bytes < CHUNK_BYTES {
            if self.cancellation.is_cancelled() {
                return Some(Err("cancelled".to_owned()));
            }
            match self.statement.step() {
                Ok(Some(array)) => {
                    let batch = RecordBatch::from(&array);
                    rows += batch.num_rows();
                    bytes += batch.get_array_memory_size();
                    chunk.push(batch);
                }
                Ok(None) => break,
                Err(err) => return Some(Err(err.to_string())),
            }
        }
        (!chunk.is_empty()).then_some(Ok(chunk))
    }
}

// A chunk lost to a panic would hold its permit forever; it fails the scan instead.
fn encode_received<Encoded>(
    work: &Mutex<mpsc::Receiver<(u64, Vec<RecordBatch>)>>,
    encoded: &mpsc::SyncSender<(u64, Result<Encoded, String>)>,
    encode: &(impl Fn(&[RecordBatch]) -> Result<Encoded, String> + Sync),
) {
    loop {
        let received = work.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).recv();
        let Ok((sequence, batches)) = received else { return };
        let chunk = panic::catch_unwind(AssertUnwindSafe(|| encode(&batches)))
            .unwrap_or_else(|_| Err("the encoder panicked".to_owned()));
        if encoded.send((sequence, chunk)).is_err() {
            return;
        }
    }
}
