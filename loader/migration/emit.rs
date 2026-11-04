/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashSet},
    fmt::Write as _,
    io::{BufWriter, Write},
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use duckdb::{
    Connection, Statement,
    arrow::{
        array::{Array, UInt64Array},
        record_batch::RecordBatch,
    },
};
use prost::Message;
use typedb_protocol::migration::{
    Item,
    item::{self, Checksums},
};

use crate::{
    Cancellation, ProgressListener, ScanProgress,
    mapping::{MappingPlan, OwnershipColumn, TableKind, TableMapping},
    schema::ValueType,
    sql::{self, IID_COLUMN},
    value::{ProjectedColumn, TypeDBValue},
};

pub(crate) fn emit(
    conn: &Connection,
    plan: &MappingPlan,
    typedb_version: &str,
    original_database: &str,
    output: impl Write + Send,
    progress: &dyn ProgressListener,
    cancellation: &Cancellation,
) -> Result<Checksums, String> {
    let mut file =
        DataFile { output: BufWriter::with_capacity(OUTPUT_BUFFER, output), checksums: Checksums::default() };
    file.write_item(item::Item::Header(header(typedb_version, original_database)))?;
    let scanner = Scanner { conn, progress, cancellation };
    for attribute in AttributeSource::of(plan) {
        file.write_attribute(&scanner, &attribute)?;
    }
    for table in &plan.tables {
        file.write_objects(&scanner, plan, table)?;
    }
    file.finish()
}

// zstd frames, one per chunk and each with a checksum, so a damaged or truncated file fails to decompress.
const COMPRESSION_LEVEL: i32 = 1;
const OUTPUT_BUFFER: usize = 8 << 20;

// A chunk is cut by rows or by bytes, whichever comes first, so that wide rows do not inflate what is in flight.
const CHUNK_ROWS: usize = 32_768;
const CHUNK_BYTES: usize = 32 << 20;
const CHUNKS_IN_FLIGHT_PER_WORKER: usize = 4;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

fn header(typedb_version: &str, original_database: &str) -> item::Header {
    item::Header {
        typedb_version: typedb_version.to_owned(),
        original_database: original_database.to_owned(),
        guarantees: Some(item::header::Guarantees {
            ordering: item::header::guarantees::Ordering::AttributesEntitiesRelations.into(),
            attributes_unique: true,
            players_by_label_scoped_ids: true,
            owned_attributes_by_value: true,
        }),
    }
}

struct DataFile<W: Write> {
    output: BufWriter<W>,
    checksums: Checksums,
}

impl<W: Write + Send> DataFile<W> {
    fn write_item(&mut self, item: item::Item) -> Result<(), String> {
        let bytes = compress(&Item { item: Some(item) }.encode_length_delimited_to_vec())?;
        self.write_bytes(&bytes)
    }

    fn write_chunk(&mut self, chunk: &EncodedChunk) -> Result<(), String> {
        self.write_bytes(&chunk.bytes)?;
        add_checksums(&mut self.checksums, &chunk.checksums);
        Ok(())
    }

    // Sorted values import faster; a streamed result stays sorted only while insertion order is preserved.
    fn write_attribute(&mut self, scanner: &Scanner<'_>, attribute: &AttributeSource<'_>) -> Result<(), String> {
        let with_context =
            |err: String| format!("attribute '{}' (from {}): {err}", attribute.label, attribute.columns.join(", "));
        let encoder = AttributeEncoder { label: attribute.label, value_type: attribute.value_type };
        let query = sql::scan::distinct_values_in_order(&attribute.values);
        let preserve_order =
            |is_preserved| scanner.conn.execute_batch(&sql::setting::set_preserve_insertion_order(is_preserved));
        preserve_order(true).map_err(|err| with_context(err.to_string()))?;
        let scanned = scanner.scan(&query, &encoder, &format!("attribute '{}'", attribute.label), None, self);
        let restored = preserve_order(false);
        scanned.map_err(with_context)?;
        restored.map_err(|err| with_context(err.to_string()))
    }

    fn write_objects(&mut self, scanner: &Scanner<'_>, plan: &MappingPlan, table: &TableMapping) -> Result<(), String> {
        let rows: u64 = scanner
            .conn
            .query_row(&sql::scan::row_count(&table.name), [], |row| row.get(0))
            .map_err(|err| format!("failed to count the rows of '{}': {err}", table.name))?;
        let encoder = ObjectEncoder::new(plan, table);
        let scanned = scanner
            .scan(&encoder.query(), &encoder, &format!("table '{}'", table.name), Some(rows), self)
            .map_err(|err| format!("scan of '{}' failed: {err}", table.name))?;
        if scanned != rows {
            return Err(format!("the scan of '{}' returned {scanned} of its {rows} rows", table.name));
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Checksums, String> {
        let checksums = self.checksums;
        self.write_item(item::Item::Checksums(checksums))?;
        self.output.flush().map_err(|err| format!("failed to flush the output file: {err}"))?;
        Ok(checksums)
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.output.write_all(bytes).map_err(|err| format!("failed to write the output file: {err}"))
    }
}

struct Scanner<'a> {
    conn: &'a Connection,
    progress: &'a dyn ProgressListener,
    cancellation: &'a Cancellation,
}

impl Scanner<'_> {
    // Streamed, as a whole result would exceed the memory limit: one reader, encoders on every core, an ordered writer.
    fn scan(
        &self,
        query: &str,
        encoder: &(impl ChunkEncoder + Sync),
        name: &str,
        total_rows: Option<u64>,
        file: &mut DataFile<impl Write + Send>,
    ) -> Result<u64, String> {
        let workers = self.encoding_workers()?;
        let statement = self.conn.prepare(query).map_err(|err| err.to_string())?;
        let chunks = ChunkReader::start(statement, self.cancellation)?;
        let writer = OrderedWriter::new(file, self.progress, name, total_rows);
        encode_in_parallel(chunks, encoder, workers, writer)
    }

    // Each worker holds a chunk in and a chunk out; DuckDB's threads, which the memory limit bounds, bound them too.
    fn encoding_workers(&self) -> Result<usize, String> {
        let duckdb_threads: i64 = self
            .conn
            .query_row(&sql::setting::current("threads"), [], |row| row.get(0))
            .map_err(|err| err.to_string())?;
        let cores = thread::available_parallelism().map(|cores| cores.get()).unwrap_or(2);
        Ok(cores.min(usize::try_from(duckdb_threads).unwrap_or(1).max(1)))
    }
}

struct ChunkReader<'conn> {
    statement: Statement<'conn>,
    cancellation: &'conn Cancellation,
}

impl<'conn> ChunkReader<'conn> {
    fn start(mut statement: Statement<'conn>, cancellation: &'conn Cancellation) -> Result<Self, String> {
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

trait ChunkEncoder {
    fn encode(&self, batches: &[RecordBatch]) -> Result<EncodedChunk, String>;
}

#[derive(Default)]
struct EncodedChunk {
    bytes: Vec<u8>,
    checksums: Checksums,
    rows: u64,
}

impl EncodedChunk {
    fn write(&mut self, item: &Item) {
        item.encode_length_delimited(&mut self.bytes).expect("a Vec grows to fit");
    }
}

struct OrderedWriter<'a, W: Write> {
    file: &'a mut DataFile<W>,
    progress: &'a dyn ProgressListener,
    scan: &'a str,
    total_rows: Option<u64>,
    pending: BTreeMap<u64, EncodedChunk>,
    next_sequence: u64,
    rows: u64,
    bytes: u64,
    reported: Instant,
}

impl<'a, W: Write + Send> OrderedWriter<'a, W> {
    fn new(
        file: &'a mut DataFile<W>,
        progress: &'a dyn ProgressListener,
        scan: &'a str,
        total_rows: Option<u64>,
    ) -> Self {
        OrderedWriter {
            file,
            progress,
            scan,
            total_rows,
            pending: BTreeMap::new(),
            next_sequence: 0,
            rows: 0,
            bytes: 0,
            reported: Instant::now(),
        }
    }

    // The writes that the chunk makes possible, one permit returned for each.
    fn accept(&mut self, sequence: u64, chunk: EncodedChunk, permits: &mpsc::SyncSender<()>) -> Result<(), String> {
        self.pending.insert(sequence, chunk);
        while let Some(chunk) = self.pending.remove(&self.next_sequence) {
            self.file.write_chunk(&chunk)?;
            self.rows += chunk.rows;
            self.bytes += chunk.bytes.len() as u64;
            self.next_sequence += 1;
            let _ = permits.send(());
        }
        if self.reported.elapsed() >= PROGRESS_INTERVAL {
            let progress = ScanProgress {
                scan: self.scan,
                rows: self.rows,
                total_rows: self.total_rows,
                encoded_bytes: self.bytes,
            };
            self.progress.report(&progress);
            self.reported = Instant::now();
        }
        Ok(())
    }
}

// Each chunk read takes a permit, returned once written, which bounds the chunks waiting for an earlier one.
fn encode_in_parallel<W: Write + Send>(
    chunks: ChunkReader<'_>,
    encoder: &(impl ChunkEncoder + Sync),
    workers: usize,
    mut writer: OrderedWriter<'_, W>,
) -> Result<u64, String> {
    let (work_sender, work_receiver) = mpsc::sync_channel::<(u64, Vec<RecordBatch>)>(workers);
    let (encoded_sender, encoded_receiver) = mpsc::sync_channel::<(u64, Result<EncodedChunk, String>)>(workers);
    let (permit_sender, permits) = mpsc::sync_channel::<()>(CHUNKS_IN_FLIGHT_PER_WORKER * workers);
    for _ in 0..CHUNKS_IN_FLIGHT_PER_WORKER * workers {
        permit_sender.send(()).expect("the permit channel has room for every permit");
    }
    thread::scope(|scope| {
        // Only the workers hold the receiver, so sending fails rather than blocks once they have all left.
        let work_receiver = Arc::new(Mutex::new(work_receiver));
        for _ in 0..workers {
            let (work_receiver, encoded_sender) = (work_receiver.clone(), encoded_sender.clone());
            scope.spawn(move || encode_received(&work_receiver, &encoded_sender, encoder));
        }
        drop((work_receiver, encoded_sender));
        let writer = scope.spawn(move || -> Result<u64, String> {
            for (sequence, chunk) in encoded_receiver {
                writer.accept(sequence, chunk?, &permit_sender)?;
            }
            Ok(writer.rows)
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
            let is_sent =
                permits.recv().is_ok() && !writer.is_finished() && work_sender.send((sequence, chunk)).is_ok();
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

// A chunk lost to a panic would hold its permit forever; it fails the scan instead.
fn encode_received(
    work: &Mutex<mpsc::Receiver<(u64, Vec<RecordBatch>)>>,
    encoded: &mpsc::SyncSender<(u64, Result<EncodedChunk, String>)>,
    encoder: &(impl ChunkEncoder + Sync),
) {
    loop {
        let received = work.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).recv();
        let Ok((sequence, batches)) = received else { return };
        let chunk = panic::catch_unwind(AssertUnwindSafe(|| {
            encoder.encode(&batches).and_then(|mut chunk| {
                chunk.bytes = compress(&chunk.bytes)?;
                Ok(chunk)
            })
        }))
        .unwrap_or_else(|_| Err("the encoder panicked".to_owned()));
        if encoded.send((sequence, chunk)).is_err() {
            return;
        }
    }
}

// All of an attribute's columns in one aggregate, which spills where one aggregate per column fails.
struct AttributeSource<'a> {
    label: &'a str,
    value_type: ValueType,
    values: Vec<String>,
    columns: Vec<String>,
}

impl<'a> AttributeSource<'a> {
    fn of(plan: &'a MappingPlan) -> Vec<AttributeSource<'a>> {
        let mut sources: Vec<AttributeSource<'a>> = Vec::new();
        for table in &plan.tables {
            for ownership in &table.ownerships {
                let values = sql::scan::values(&table.name, &ownership.attribute_values());
                let column = format!("{}.{}", table.name, ownership.column);
                match sources.iter_mut().find(|source| source.label == ownership.attribute) {
                    Some(source) => {
                        source.values.push(values);
                        source.columns.push(column);
                    }
                    None => sources.push(AttributeSource {
                        label: &ownership.attribute,
                        value_type: ownership.value_type,
                        values: vec![values],
                        columns: vec![column],
                    }),
                }
            }
        }
        sources
    }
}

struct AttributeEncoder<'a> {
    label: &'a str,
    value_type: ValueType,
}

impl ChunkEncoder for AttributeEncoder<'_> {
    fn encode(&self, batches: &[RecordBatch]) -> Result<EncodedChunk, String> {
        let mut chunk = EncodedChunk::default();
        let mut message = Item {
            item: Some(item::Item::Attribute(item::Attribute { label: self.label.to_owned(), ..Default::default() })),
        };
        for batch in batches {
            chunk.rows += batch.num_rows() as u64;
            for value in ProjectedColumn::of(batch.column(0).as_ref())?.values(self.value_type) {
                let Some(value) = value? else { continue };
                let Some(item::Item::Attribute(attribute)) = &mut message.item else { unreachable!() };
                write_attribute_id(&mut attribute.id, self.label, &value);
                attribute.value = Some(value.into_proto());
                chunk.write(&message);
                chunk.checksums.attribute_count += 1;
            }
        }
        Ok(chunk)
    }
}

struct ObjectEncoder<'a> {
    table: &'a TableMapping,
    players: Vec<Player<'a>>,
    has_repeatable_ownerships: bool,
}

struct Player<'a> {
    label: &'a str,
    scoped_role: String,
}

impl<'a> ObjectEncoder<'a> {
    fn new(plan: &'a MappingPlan, table: &'a TableMapping) -> Self {
        let players = table
            .roles()
            .iter()
            .map(|role| Player {
                label: &plan.tables[role.player_table_index].type_label,
                scoped_role: role.relates.scoped_label(),
            })
            .collect();
        let ownerships = &table.ownerships;
        let has_repeatable_ownerships = ownerships.iter().enumerate().any(|(index, ownership)| {
            ownership.is_list || ownerships[..index].iter().any(|other| other.attribute == ownership.attribute)
        });
        ObjectEncoder { table, players, has_repeatable_ownerships }
    }

    fn query(&self) -> String {
        let projections: Vec<String> = std::iter::once(sql::column_of_scanned_table(IID_COLUMN))
            .chain(self.table.ownerships.iter().map(OwnershipColumn::scanned))
            .chain(self.table.roles().iter().map(|role| sql::column_of_scanned_table(&role.column)))
            .collect();
        sql::scan::rows(&self.table.name, &projections)
    }

    fn encode_row(&self, row: &ScannedRow<'_>, item: &mut ObjectItem) -> Result<Checksums, String> {
        item.start(&self.table.type_label, row.iid());
        row.for_each_owned_value(|ownership, value| {
            item.own(&ownership.attribute, value, self.has_repeatable_ownerships)
        })?;
        let mut checksums = Checksums { ownership_count: item.attributes().len() as i64, ..Default::default() };
        if self.table.is_relation() {
            for (player, id) in self.players.iter().zip(row.player_ids()) {
                if let Some(id) = id
                    && item.play(&player.scoped_role, player.label, id)
                {
                    checksums.role_count += 1;
                }
            }
            if item.roles().is_empty() {
                return Err("no role is filled; a relation cannot exist without players".to_owned());
            }
            checksums.relation_count = 1;
        } else {
            checksums.entity_count = 1;
        }
        Ok(checksums)
    }
}

impl ChunkEncoder for ObjectEncoder<'_> {
    fn encode(&self, batches: &[RecordBatch]) -> Result<EncodedChunk, String> {
        let mut chunk = EncodedChunk::default();
        let mut item = ObjectItem::new(self.table);
        for batch in batches {
            let batch = ScannedBatch::of(batch, self.table)?;
            for row in batch.rows() {
                chunk.rows += 1;
                let checksums =
                    self.encode_row(&row, &mut item).map_err(|err| format!("{IID_COLUMN} {}: {err}", row.iid()))?;
                add_checksums(&mut chunk.checksums, &checksums);
                chunk.write(&item.message);
            }
        }
        Ok(chunk)
    }
}

struct ScannedBatch<'a> {
    ids: &'a UInt64Array,
    ownerships: Vec<ScannedOwnership<'a>>,
    player_ids: Vec<&'a UInt64Array>,
}

struct ScannedOwnership<'a> {
    column: ProjectedColumn<'a>,
    ownership: &'a OwnershipColumn,
}

impl<'a> ScannedBatch<'a> {
    fn of(batch: &'a RecordBatch, table: &'a TableMapping) -> Result<Self, String> {
        let columns: Vec<ProjectedColumn<'a>> =
            batch.columns().iter().map(|column| ProjectedColumn::of(column.as_ref())).collect::<Result<_, _>>()?;
        let mut columns = columns.into_iter();
        let ids = columns.next().and_then(|column| column.instance_ids());
        let ids = ids.ok_or_else(|| format!("{IID_COLUMN} was not read as UBIGINT"))?;
        let ownerships = columns
            .by_ref()
            .take(table.ownerships.len())
            .zip(&table.ownerships)
            .map(|(column, ownership)| ScannedOwnership { column, ownership })
            .collect();
        let player_ids = columns
            .zip(table.roles())
            .map(|(column, role)| {
                column.instance_ids().ok_or_else(|| format!("column '{}' was not read as UBIGINT", role.column))
            })
            .collect::<Result<_, _>>()?;
        Ok(ScannedBatch { ids, ownerships, player_ids })
    }

    fn rows(&self) -> impl Iterator<Item = ScannedRow<'_>> {
        (0..self.ids.len()).map(|index| ScannedRow { batch: self, index })
    }
}

struct ScannedRow<'a> {
    batch: &'a ScannedBatch<'a>,
    index: usize,
}

impl ScannedRow<'_> {
    fn iid(&self) -> u64 {
        self.batch.ids.value(self.index)
    }

    // A list column's non-NULL elements, or the column's value unless NULL.
    fn for_each_owned_value(&self, mut action: impl FnMut(&OwnershipColumn, TypeDBValue)) -> Result<(), String> {
        for ScannedOwnership { column, ownership } in &self.batch.ownerships {
            if ownership.is_list {
                for value in column.list_values(self.index, ownership.value_type)? {
                    if let Some(value) = value? {
                        action(ownership, value);
                    }
                }
            } else if let Some(value) = column.value(self.index, ownership.value_type)? {
                action(ownership, value);
            }
        }
        Ok(())
    }

    fn player_ids(&self) -> impl Iterator<Item = Option<u64>> {
        self.batch.player_ids.iter().map(|ids| (!ids.is_null(self.index)).then(|| ids.value(self.index)))
    }
}

// An entity or relation item refilled row by row; the strings and lists of earlier rows' items are reused.
struct ObjectItem {
    message: Item,
    parts_pool: ItemPartsPool,
}

impl ObjectItem {
    fn new(table: &TableMapping) -> Self {
        let label = table.type_label.clone();
        let object = match &table.kind {
            TableKind::Entity => item::Item::Entity(item::Entity { label, ..Default::default() }),
            TableKind::Relation { .. } => item::Item::Relation(item::Relation { label, ..Default::default() }),
        };
        ObjectItem { message: Item { item: Some(object) }, parts_pool: ItemPartsPool::default() }
    }

    fn start(&mut self, label: &str, iid: u64) {
        let (id, label_scoped_id, attributes, roles) = object_parts(&mut self.message);
        write_object_id(id, label, iid);
        *label_scoped_id = Some(iid);
        self.parts_pool.recycle(attributes, roles);
    }

    fn own(&mut self, attribute: &str, value: TypeDBValue, is_repeatable: bool) {
        if let Some(owned) = self.parts_pool.owned_attribute(attribute, value, is_repeatable) {
            object_parts(&mut self.message).2.push(owned);
        }
    }

    // Returns whether the player is new to the role: one player bound by several of the role's columns is listed once.
    fn play(&mut self, role: &str, label: &str, id: u64) -> bool {
        let Some(item::Item::Relation(relation)) = &mut self.message.item else { return false };
        self.parts_pool.add_player(&mut relation.roles, role, label, id)
    }

    fn attributes(&self) -> &[item::OwnedAttribute] {
        match &self.message.item {
            Some(item::Item::Entity(entity)) => &entity.attributes,
            Some(item::Item::Relation(relation)) => &relation.attributes,
            _ => &[],
        }
    }

    fn roles(&self) -> &[item::relation::Role] {
        match &self.message.item {
            Some(item::Item::Relation(relation)) => &relation.roles,
            _ => &[],
        }
    }
}

#[derive(Default)]
struct ItemPartsPool {
    attributes: Vec<item::OwnedAttribute>,
    roles: Vec<item::relation::Role>,
    players: Vec<item::relation::role::Player>,
    owned_ids: HashSet<String>,
}

impl ItemPartsPool {
    // Takes back the parts of the previous row's item, whose lists keep their capacity.
    fn recycle(&mut self, attributes: &mut Vec<item::OwnedAttribute>, roles: Option<&mut Vec<item::relation::Role>>) {
        self.attributes.append(attributes);
        for mut role in roles.into_iter().flat_map(|roles| roles.drain(..)) {
            self.players.append(&mut role.players);
            self.roles.push(role);
        }
        self.owned_ids.clear();
    }

    // None when the attribute is already owned.
    fn owned_attribute(
        &mut self,
        label: &str,
        value: TypeDBValue,
        is_repeatable: bool,
    ) -> Option<item::OwnedAttribute> {
        let mut owned = self.attributes.pop().unwrap_or_default();
        write_attribute_id(&mut owned.id, label, &value);
        if is_repeatable && !self.owned_ids.insert(owned.id.clone()) {
            self.attributes.push(owned);
            return None;
        }
        let by_value = owned.attribute_by_value.get_or_insert_default();
        by_value.label.clear();
        by_value.label.push_str(label);
        by_value.value = Some(value.into_proto());
        Some(owned)
    }

    fn add_player(&mut self, roles: &mut Vec<item::relation::Role>, role: &str, label: &str, id: u64) -> bool {
        let grouped = match roles.iter().position(|grouped| grouped.label == role) {
            Some(index) => &mut roles[index],
            None => {
                let mut grouped = self.roles.pop().unwrap_or_default();
                grouped.label.clear();
                grouped.label.push_str(role);
                roles.push(grouped);
                roles.last_mut().expect("just pushed")
            }
        };
        let is_known = grouped
            .players
            .iter()
            .filter_map(|known| known.player_by_label_scoped_id.as_ref())
            .any(|known| known.label_scoped_id == id && known.label == label);
        if is_known {
            return false;
        }
        let mut player = self.players.pop().unwrap_or_default();
        write_object_id(&mut player.id, label, id);
        let by_id = player.player_by_label_scoped_id.get_or_insert_default();
        by_id.label.clear();
        by_id.label.push_str(label);
        by_id.label_scoped_id = id;
        grouped.players.push(player);
        true
    }
}

fn write_object_id(id: &mut String, label: &str, label_scoped_id: u64) {
    id.clear();
    let _ = write!(id, "{label}:{label_scoped_id}");
}

fn write_attribute_id(id: &mut String, label: &str, value: &TypeDBValue) {
    id.clear();
    let _ = write!(id, "{label}:{value}");
}

fn object_parts(
    message: &mut Item,
) -> (&mut String, &mut Option<u64>, &mut Vec<item::OwnedAttribute>, Option<&mut Vec<item::relation::Role>>) {
    match &mut message.item {
        Some(item::Item::Entity(entity)) => (&mut entity.id, &mut entity.label_scoped_id, &mut entity.attributes, None),
        Some(item::Item::Relation(relation)) => {
            (&mut relation.id, &mut relation.label_scoped_id, &mut relation.attributes, Some(&mut relation.roles))
        }
        _ => unreachable!("an object item is an entity or a relation"),
    }
}

fn compress(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let compress_error = |err: std::io::Error| format!("failed to compress the output: {err}");
    let mut compressor = zstd::bulk::Compressor::new(COMPRESSION_LEVEL).map_err(compress_error)?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true)).map_err(compress_error)?;
    compressor.compress(bytes).map_err(compress_error)
}

fn add_checksums(total: &mut Checksums, chunk: &Checksums) {
    let Checksums { entity_count, attribute_count, relation_count, role_count, ownership_count } = chunk;
    total.entity_count += entity_count;
    total.attribute_count += attribute_count;
    total.relation_count += relation_count;
    total.role_count += role_count;
    total.ownership_count += ownership_count;
}
