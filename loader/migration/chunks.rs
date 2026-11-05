/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use duckdb::{Statement, arrow::record_batch::RecordBatch};
use typedb_cli_common::constants::MB;

use crate::Cancellation;

// A chunk is cut by rows or by bytes, whichever comes first, so that wide rows do not inflate what is in flight.
const CHUNK_ROWS: usize = 32_768;
const CHUNK_BYTES: usize = 32 * MB as usize;
const CHUNKS_IN_FLIGHT_PER_WORKER: usize = 2;
// About a GiB of chunks, whatever the core count.
const MAX_CHUNKS_IN_FLIGHT: usize = 32;
const CANCELLED: &str = "cancelled";

// Each chunk's result channel is queued as the chunk is read, so `write` takes the results in read order.
pub(crate) fn encode_in_order<Encoded: Send>(
    chunks: ChunkReader<'_>,
    encode: &(impl Fn(&[RecordBatch]) -> Result<Encoded, String> + Sync),
    workers: usize,
    mut write: impl FnMut(Encoded) -> Result<(), String> + Send,
) -> Result<(), String> {
    let (work_sender, work) =
        mpsc::sync_channel::<(Vec<RecordBatch>, mpsc::SyncSender<Result<Encoded, String>>)>(workers);
    // Only the workers hold the work queue, so that once they have all stopped, sending to it fails.
    let work = Arc::new(Mutex::new(work));
    let (results_sender, results) =
        mpsc::sync_channel::<mpsc::Receiver<Result<Encoded, String>>>(chunks_in_flight(workers));
    thread::scope(|scope| {
        for _ in 0..workers {
            let work = work.clone();
            scope.spawn(move || {
                loop {
                    let received = work.lock().expect("no worker panics while it takes work").recv();
                    let Ok((batches, result_sender)) = received else { return };
                    let _ = result_sender.send(encode(&batches));
                }
            });
        }
        drop(work);
        let writer = scope.spawn(move || -> Result<(), String> {
            for result_receiver in results {
                write(result_receiver.recv().map_err(|_| "an encoder panicked".to_owned())??)?;
            }
            Ok(())
        });
        let mut read_error = None;
        for chunk in chunks {
            let batches = match chunk {
                Ok(batches) => batches,
                Err(err) => {
                    read_error = Some(err);
                    break;
                }
            };
            let (result_sender, result_receiver) = mpsc::sync_channel(1);
            if results_sender.send(result_receiver).is_err() || work_sender.send((batches, result_sender)).is_err() {
                break;
            }
        }
        drop((work_sender, results_sender));
        let written = writer.join().unwrap_or_else(|_| Err("the output writer panicked".to_owned()));
        read_error.map_or(written, Err)
    })
}

fn chunks_in_flight(workers: usize) -> usize {
    (CHUNKS_IN_FLIGHT_PER_WORKER * workers).min(MAX_CHUNKS_IN_FLIGHT)
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
            return Err(CANCELLED.to_owned());
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
                return Some(Err(CANCELLED.to_owned()));
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

#[cfg(test)]
mod test {
    use std::panic;

    use duckdb::Connection;

    use super::*;

    #[test]
    fn an_encoder_panic_propagates_rather_than_hangs() {
        let connection = Connection::open_in_memory().unwrap();
        let cancellation = Cancellation::default();
        for workers in [1, 2].into_iter().cycle().take(20) {
            let statement = connection.prepare("SELECT range FROM range(200000)").unwrap();
            let chunks = ChunkReader::start(statement, &cancellation).unwrap();
            let encode = |_: &[RecordBatch]| -> Result<(), String> { panic!("encoder bug") };
            let outcome =
                panic::catch_unwind(panic::AssertUnwindSafe(|| encode_in_order(chunks, &encode, workers, |()| Ok(()))));
            assert!(outcome.is_err(), "the panic did not propagate");
        }
    }
}
