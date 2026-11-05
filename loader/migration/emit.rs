/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    fmt::Write as _,
    io::{BufWriter, Write},
    thread,
};

use duckdb::arrow::{
    array::{Array, ArrayRef, AsArray, UInt64Array},
    datatypes::UInt64Type,
    record_batch::RecordBatch,
};
use prost::Message;
use typedb_cli_common::constants::MB;
use typedb_protocol::migration::{
    Item,
    item::{self, Checksums},
};

use crate::{
    Migration, ProgressListener, ScanProgress,
    chunks::{ChunkReader, encode_in_order},
    mapping::{MappingPlan, OwnershipColumn, TableKind, TableMapping},
    schema::ValueType,
    sql::{self, IID_COLUMN},
    value::{ProjectedColumn, TypeDBValue},
};

const COMPRESSION_LEVEL: i32 = 1;
const OUTPUT_BUFFER: usize = 8 * MB as usize;

pub(crate) fn emit(
    migration: &Migration,
    output: impl Write + Send,
    typedb_version: &str,
    original_database: &str,
    progress: &dyn ProgressListener,
) -> Result<Checksums, String> {
    let mut file = DataFile {
        output: BufWriter::with_capacity(OUTPUT_BUFFER, output),
        checksums: Checksums::default(),
        migration,
        progress,
    };
    file.write_item(item::Item::Header(header(typedb_version, original_database)))?;
    for attribute in AttributeSource::of(&migration.plan) {
        file.write_attribute(&attribute)?;
    }
    let (relations, entities): (Vec<&TableMapping>, Vec<&TableMapping>) =
        migration.plan.tables.iter().partition(|table| matches!(table.kind, TableKind::Relation { .. }));
    for table in entities.into_iter().chain(relations) {
        file.write_objects(table)?;
    }
    file.finish()
}

fn header(typedb_version: &str, original_database: &str) -> item::Header {
    item::Header {
        typedb_version: typedb_version.to_owned(),
        original_database: original_database.to_owned(),
        guarantees: Some(item::header::Guarantees {
            item_order: item::header::guarantees::ItemOrder::OrderAttributesEntitiesRelations.into(),
            attributes_unique: true,
            label_scoped_ids_present: true,
            owned_inlined_attributes_referenced_by_value: true,
        }),
    }
}

struct DataFile<'a, W: Write> {
    output: BufWriter<W>,
    checksums: Checksums,
    migration: &'a Migration,
    progress: &'a dyn ProgressListener,
}

impl<W: Write + Send> DataFile<'_, W> {
    fn write_item(&mut self, item: item::Item) -> Result<(), String> {
        let bytes = compress(&Item { item: Some(item) }.encode_length_delimited_to_vec())?;
        write_bytes(&mut self.output, &bytes)
    }

    // Sorted values import faster; a streamed result stays sorted only while insertion order is preserved.
    fn write_attribute(&mut self, attribute: &AttributeSource<'_>) -> Result<(), String> {
        let with_context = |err: String| {
            format!("attribute '{}' (from {}): {err}", attribute.label, attribute.column_names.join(", "))
        };
        let connection = &self.migration.connection;
        let preserve_order =
            |is_preserved| connection.execute_batch(&sql::setting::set_preserve_insertion_order(is_preserved));
        preserve_order(true).map_err(|err| with_context(err.to_string()))?;
        let encoder = AttributeEncoder { label: attribute.label, value_type: attribute.value_type };
        let query = sql::scan::distinct_values_in_order(&attribute.value_queries);
        let scanned = self.scan(&query, &encoder, &format!("attribute '{}'", attribute.label), None);
        let restored = preserve_order(false);
        scanned.map_err(with_context)?;
        restored.map_err(|err| with_context(err.to_string()))
    }

    fn write_objects(&mut self, table: &TableMapping) -> Result<(), String> {
        let rows: u64 = self
            .migration
            .connection
            .query_row(&sql::scan::row_count(&table.name), [], |row| row.get(0))
            .map_err(|err| format!("failed to count the rows of '{}': {err}", table.name))?;
        let encoder = ObjectEncoder::new(&self.migration.plan, table);
        self.scan(&encoder.query(), &encoder, &format!("table '{}'", table.name), Some(rows))
            .map_err(|err| format!("scan of '{}' failed: {err}", table.name))
    }

    fn finish(mut self) -> Result<Checksums, String> {
        let checksums = self.checksums;
        self.write_item(item::Item::Checksums(checksums))?;
        self.output.flush().map_err(|err| format!("failed to flush the output file: {err}"))?;
        Ok(checksums)
    }

    fn scan(
        &mut self,
        query: &str,
        encoder: &(impl ChunkEncoder + Sync),
        scan: &str,
        total_rows: Option<u64>,
    ) -> Result<(), String> {
        let migration = self.migration;
        let statement = migration.connection.prepare(query).map_err(|err| err.to_string())?;
        let chunks = ChunkReader::start(statement, &migration.cancellation)?;
        let encode = |batches: &[RecordBatch]| {
            let items = encoder.encode(batches)?;
            let rows = batches.iter().map(|batch| batch.num_rows() as u64).sum();
            Ok(CompressedChunk { bytes: compress(&items.bytes)?, checksums: items.checksums, rows })
        };
        let (output, checksums, progress) = (&mut self.output, &mut self.checksums, self.progress);
        let (mut rows, mut encoded_bytes) = (0, 0);
        encode_in_order(chunks, &encode, encoding_workers(migration), |chunk: CompressedChunk| {
            write_bytes(output, &chunk.bytes)?;
            add_checksums(checksums, &chunk.checksums);
            rows += chunk.rows;
            encoded_bytes += chunk.bytes.len() as u64;
            progress.report(&ScanProgress { scan, rows, total_rows, encoded_bytes });
            Ok(())
        })
    }
}

fn write_bytes(output: &mut impl Write, bytes: &[u8]) -> Result<(), String> {
    output.write_all(bytes).map_err(|err| format!("failed to write the output file: {err}"))
}

// Each worker holds a chunk in and a chunk out.
// DuckDB's threads, which the memory limit bounds, bound them too.
fn encoding_workers(migration: &Migration) -> usize {
    let cores = thread::available_parallelism().map(|cores| cores.get()).unwrap_or(1);
    cores.min(migration.threads)
}

trait ChunkEncoder {
    fn encode(&self, batches: &[RecordBatch]) -> Result<EncodedItems, String>;
}

#[derive(Default)]
struct EncodedItems {
    bytes: Vec<u8>,
    checksums: Checksums,
}

impl EncodedItems {
    fn write(&mut self, message: &Item) {
        message.encode_length_delimited(&mut self.bytes).expect("a Vec grows to fit");
        let checksums = &mut self.checksums;
        match &message.item {
            Some(item::Item::Attribute(_)) => checksums.attribute_count += 1,
            Some(item::Item::Entity(entity)) => {
                checksums.entity_count += 1;
                checksums.ownership_count += entity.attributes.len() as i64;
            }
            Some(item::Item::Relation(relation)) => {
                checksums.relation_count += 1;
                checksums.ownership_count += relation.attributes.len() as i64;
                checksums.role_count += relation.roles.iter().map(|role| role.players.len() as i64).sum::<i64>();
            }
            Some(item::Item::Header(_) | item::Item::Checksums(_)) | None => {}
        }
    }
}

struct CompressedChunk {
    bytes: Vec<u8>,
    checksums: Checksums,
    rows: u64,
}

// All of an attribute's columns in one aggregate, which spills where one aggregate per column fails.
struct AttributeSource<'a> {
    label: &'a str,
    value_type: ValueType,
    value_queries: Vec<String>,
    column_names: Vec<String>,
}

impl<'a> AttributeSource<'a> {
    fn of(plan: &'a MappingPlan) -> Vec<AttributeSource<'a>> {
        let mut sources: Vec<AttributeSource<'a>> = Vec::new();
        for table in &plan.tables {
            for ownership in &table.ownerships {
                let value_query = sql::scan::values(&table.name, &ownership.attribute_values());
                let column_name = format!("{}.{}", table.name, ownership.column);
                match sources.iter_mut().find(|source| source.label == ownership.attribute) {
                    Some(source) => {
                        source.value_queries.push(value_query);
                        source.column_names.push(column_name);
                    }
                    None => sources.push(AttributeSource {
                        label: &ownership.attribute,
                        value_type: ownership.value_type,
                        value_queries: vec![value_query],
                        column_names: vec![column_name],
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
    fn encode(&self, batches: &[RecordBatch]) -> Result<EncodedItems, String> {
        let mut items = EncodedItems::default();
        let mut message = Item {
            item: Some(item::Item::Attribute(item::Attribute { label: self.label.to_owned(), ..Default::default() })),
        };
        for batch in batches {
            for value in ProjectedColumn::of(batch.column(0).as_ref(), self.value_type)?.values() {
                let value = value?;
                let Some(item::Item::Attribute(attribute)) = &mut message.item else {
                    unreachable!("built as an attribute")
                };
                write_attribute_id(&mut attribute.globally_unique_id, self.label, &value);
                attribute.value = Some(value.into_proto());
                items.write(&message);
            }
        }
        Ok(items)
    }
}

struct ObjectEncoder<'a> {
    table: &'a TableMapping,
    role_players: Vec<RolePlayer<'a>>,
}

struct RolePlayer<'a> {
    player_type: &'a str,
    role_label: String,
}

impl<'a> ObjectEncoder<'a> {
    fn new(plan: &'a MappingPlan, table: &'a TableMapping) -> Self {
        let role_players = table
            .roles()
            .iter()
            .map(|role| RolePlayer {
                player_type: &plan.tables[role.player_table_index].type_label,
                role_label: role.role_type.scoped_label(),
            })
            .collect();
        ObjectEncoder { table, role_players }
    }

    fn query(&self) -> String {
        let projections: Vec<String> = std::iter::once(sql::column_of_scanned_table(IID_COLUMN))
            .chain(self.table.ownerships.iter().map(OwnershipColumn::object_scan_projection))
            .chain(self.table.roles().iter().map(|role| sql::column_of_scanned_table(&role.column)))
            .collect();
        sql::scan::rows(&self.table.name, &projections)
    }

    fn encode_row(&self, row: &ScannedRow<'_>, item: &mut ObjectItemBuffer) -> Result<(), String> {
        item.reset(row.iid());
        row.for_each_owned_value(|ownership, value| item.own(&ownership.attribute, value))?;
        for (role_player, player_iid) in self.role_players.iter().zip(row.player_iids()) {
            if let Some(player_iid) = player_iid {
                item.play(&role_player.role_label, role_player.player_type, player_iid);
            }
        }
        Ok(())
    }
}

impl ChunkEncoder for ObjectEncoder<'_> {
    fn encode(&self, batches: &[RecordBatch]) -> Result<EncodedItems, String> {
        let mut items = EncodedItems::default();
        let mut item = ObjectItemBuffer::new(self.table);
        for batch in batches {
            let batch = ScannedBatch::of(batch, self.table)?;
            for row in batch.rows() {
                self.encode_row(&row, &mut item).map_err(|err| format!("{IID_COLUMN} {}: {err}", row.iid()))?;
                items.write(&item.message);
            }
        }
        Ok(items)
    }
}

struct ScannedBatch<'a> {
    iids: &'a UInt64Array,
    ownerships: Vec<ScannedOwnership<'a>>,
    player_iids: Vec<&'a UInt64Array>,
}

struct ScannedOwnership<'a> {
    column: ProjectedColumn<'a>,
    ownership: &'a OwnershipColumn,
}

impl<'a> ScannedBatch<'a> {
    fn of(batch: &'a RecordBatch, table: &'a TableMapping) -> Result<Self, String> {
        let instance_ids = |column: &'a ArrayRef, name: &str| {
            column.as_primitive_opt::<UInt64Type>().ok_or_else(|| format!("column '{name}' was not read as UBIGINT"))
        };
        let mut columns = batch.columns().iter();
        let iids = instance_ids(columns.next().expect("the scan projects the _iid first"), IID_COLUMN)?;
        let ownerships = columns
            .by_ref()
            .take(table.ownerships.len())
            .zip(&table.ownerships)
            .map(|(column, ownership)| {
                Ok(ScannedOwnership { column: ProjectedColumn::of(column.as_ref(), ownership.value_type)?, ownership })
            })
            .collect::<Result<_, String>>()?;
        let player_iids = columns
            .zip(table.roles())
            .map(|(column, role)| instance_ids(column, &role.column))
            .collect::<Result<_, _>>()?;
        Ok(ScannedBatch { iids, ownerships, player_iids })
    }

    fn rows(&self) -> impl Iterator<Item = ScannedRow<'_>> {
        (0..self.iids.len()).map(|index| ScannedRow { batch: self, index })
    }
}

struct ScannedRow<'a> {
    batch: &'a ScannedBatch<'a>,
    index: usize,
}

impl ScannedRow<'_> {
    fn iid(&self) -> u64 {
        self.batch.iids.value(self.index)
    }

    fn for_each_owned_value(&self, mut action: impl FnMut(&OwnershipColumn, TypeDBValue)) -> Result<(), String> {
        for ScannedOwnership { column, ownership } in &self.batch.ownerships {
            for value in column.values_at(self.index) {
                action(ownership, value?);
            }
        }
        Ok(())
    }

    fn player_iids(&self) -> impl Iterator<Item = Option<u64>> {
        self.batch.player_iids.iter().map(|iids| (!iids.is_null(self.index)).then(|| iids.value(self.index)))
    }
}

// Reused across a chunk's rows, as allocating an item's strings and lists per row costs a tenth of the conversion.
struct ObjectItemBuffer {
    message: Item,
    has_repeatable_ownerships: bool,
    owned_ids: HashSet<String>,
    spare_attributes: Vec<item::OwnedAttribute>,
    spare_roles: Vec<item::relation::Role>,
    spare_players: Vec<item::relation::role::Player>,
}

impl ObjectItemBuffer {
    fn new(table: &TableMapping) -> Self {
        let label = table.type_label.clone();
        let object = match &table.kind {
            TableKind::Entity => item::Item::Entity(item::Entity { label, ..Default::default() }),
            TableKind::Relation { .. } => item::Item::Relation(item::Relation { label, ..Default::default() }),
        };
        let ownerships = &table.ownerships;
        let has_repeatable_ownerships = ownerships.iter().enumerate().any(|(index, ownership)| {
            ownership.is_list || ownerships[..index].iter().any(|other| other.attribute == ownership.attribute)
        });
        ObjectItemBuffer {
            message: Item { item: Some(object) },
            has_repeatable_ownerships,
            owned_ids: HashSet::new(),
            spare_attributes: Vec::new(),
            spare_roles: Vec::new(),
            spare_players: Vec::new(),
        }
    }

    fn reset(&mut self, iid: u64) {
        let parts = ObjectParts::of(&mut self.message);
        write_object_id(parts.globally_unique_id, parts.label, iid);
        *parts.label_scoped_id = Some(iid);
        self.spare_attributes.append(parts.attributes);
        for mut role in parts.roles.into_iter().flat_map(|roles| roles.drain(..)) {
            self.spare_players.append(&mut role.players);
            self.spare_roles.push(role);
        }
        self.owned_ids.clear();
    }

    fn own(&mut self, attribute: &str, value: TypeDBValue) {
        let mut owned = self.spare_attributes.pop().unwrap_or_default();
        write_attribute_id(&mut owned.globally_unique_id, attribute, &value);
        if self.has_repeatable_ownerships && !self.owned_ids.insert(owned.globally_unique_id.clone()) {
            self.spare_attributes.push(owned);
            return;
        }
        let by_value = owned.attribute_by_value.get_or_insert_default();
        by_value.label.clear();
        by_value.label.push_str(attribute);
        by_value.value = Some(value.into_proto());
        ObjectParts::of(&mut self.message).attributes.push(owned);
    }

    fn play(&mut self, role: &str, player_label: &str, player_id: u64) {
        let Some(roles) = ObjectParts::of(&mut self.message).roles else {
            unreachable!("only relation tables have role columns")
        };
        let players = match roles.iter().position(|grouped| grouped.label == role) {
            Some(index) => &mut roles[index].players,
            None => {
                let mut grouped = self.spare_roles.pop().unwrap_or_default();
                grouped.label.clear();
                grouped.label.push_str(role);
                roles.push(grouped);
                &mut roles.last_mut().expect("just pushed").players
            }
        };
        let is_duplicate = players
            .iter()
            .filter_map(|listed| listed.label_scoped_id.as_ref())
            .any(|listed| listed.label_scoped_id == player_id && listed.label == player_label);
        if is_duplicate {
            return;
        }
        let mut player = self.spare_players.pop().unwrap_or_default();
        write_object_id(&mut player.globally_unique_id, player_label, player_id);
        let by_id = player.label_scoped_id.get_or_insert_default();
        by_id.label.clear();
        by_id.label.push_str(player_label);
        by_id.label_scoped_id = player_id;
        players.push(player);
    }
}

struct ObjectParts<'a> {
    label: &'a str,
    globally_unique_id: &'a mut String,
    label_scoped_id: &'a mut Option<u64>,
    attributes: &'a mut Vec<item::OwnedAttribute>,
    roles: Option<&'a mut Vec<item::relation::Role>>,
}

impl<'a> ObjectParts<'a> {
    fn of(message: &'a mut Item) -> Self {
        match &mut message.item {
            Some(item::Item::Entity(entity)) => ObjectParts {
                label: &entity.label,
                globally_unique_id: &mut entity.globally_unique_id,
                label_scoped_id: &mut entity.label_scoped_id,
                attributes: &mut entity.attributes,
                roles: None,
            },
            Some(item::Item::Relation(relation)) => ObjectParts {
                label: &relation.label,
                globally_unique_id: &mut relation.globally_unique_id,
                label_scoped_id: &mut relation.label_scoped_id,
                attributes: &mut relation.attributes,
                roles: Some(&mut relation.roles),
            },
            _ => unreachable!("an object item is an entity or a relation"),
        }
    }
}

fn write_object_id(globally_unique_id: &mut String, label: &str, label_scoped_id: u64) {
    globally_unique_id.clear();
    let _ = write!(globally_unique_id, "{label}:{label_scoped_id}");
}

fn write_attribute_id(globally_unique_id: &mut String, label: &str, value: &TypeDBValue) {
    globally_unique_id.clear();
    let _ = write!(globally_unique_id, "{label}:{value}");
}

// zstd frames, one per chunk and each with a checksum, so a damaged or truncated file fails to decompress.
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
