/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::{BufWriter, Write},
    sync::{Arc, Mutex, mpsc},
    thread,
};

use duckdb::{
    Connection,
    arrow::{array::Array, record_batch::RecordBatch},
};
use prost::Message;
use typedb_protocol::migration::{
    Item,
    item::{self, Checksums},
};

use crate::{
    SCRATCH,
    mapping::{IID_COLUMN, MappingPlan, OwnsMapping, TableKind, quoted},
    schema::ValueType,
    value::{Cells, TypeDBValue, Typed},
};

// Each scan is a pipeline: this thread streams the sorted rows out of DuckDB in record batches and groups them
// into units; every core turns units into encoded items; one thread writes the units back in order. A unit is
// cut by rows or by bytes, whichever comes first, so that wide rows do not inflate what is in flight.
const UNIT_ROWS: usize = 32_768;
const UNIT_BYTES: usize = 32 << 20;
const OUTPUT_BUFFER: usize = 8 << 20;

// The file is a sequence of zstd frames, one per unit, which zstd reads as one stream. Each frame is complete
// and carries a checksum of its content, so no stream is ever left unfinished and a file cut short or damaged
// fails to decompress rather than import less.
const COMPRESSION_LEVEL: i32 = 3;

fn compress(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let compress_error = |err: std::io::Error| format!("failed to compress the output: {err}");
    let mut compressor = zstd::bulk::Compressor::new(COMPRESSION_LEVEL).map_err(compress_error)?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true)).map_err(compress_error)?;
    compressor.compress(bytes).map_err(compress_error)
}

pub(crate) fn emit(
    conn: &Connection,
    plan: &MappingPlan,
    typedb_version: &str,
    original_database: &str,
    output: impl Write + Send,
) -> Result<Checksums, String> {
    let mut output = BufWriter::with_capacity(OUTPUT_BUFFER, output);
    let mut checksums = Checksums::default();
    write_item(
        &mut output,
        item::Item::Header(item::Header {
            typedb_version: typedb_version.to_owned(),
            original_database: original_database.to_owned(),
            guarantees: Some(item::header::Guarantees {
                ordering: item::header::guarantees::Ordering::AttributesEntitiesRelations.into(),
                attributes_unique: true,
                players_by_label_scoped_ids: true,
                owned_attributes_by_value: true,
            }),
        }),
    )?;

    emit_attributes(conn, plan, &mut output, &mut checksums)?;
    let numbering = number_instances(conn, plan)?;
    for index in 0..plan.tables.len() {
        emit_objects(conn, plan, index, &numbering, &mut output, &mut checksums)?;
    }

    write_item(&mut output, item::Item::Checksums(checksums))?;
    output.flush().map_err(|err| format!("failed to flush the output file: {err}"))?;
    Ok(checksums)
}

fn write_item(output: &mut impl Write, item: item::Item) -> Result<(), String> {
    let bytes = compress(&Item { item: Some(item) }.encode_length_delimited_to_vec())?;
    output.write_all(&bytes).map_err(|err| format!("failed to write the output file: {err}"))
}

struct TableNumbering {
    first_id: u64,
    count: u64,
    iids: Option<Iids>,
}

enum Iids {
    Consecutive { first: u64 },
    Ranked(String),
}

impl TableNumbering {
    fn id_of(&self, iid: u64, rank: Option<i64>) -> Option<u64> {
        let rank = match self.iids.as_ref().expect("iids are collected for the tables that play roles") {
            Iids::Consecutive { first } => iid.checked_sub(*first).filter(|rank| *rank < self.count),
            Iids::Ranked(_) => rank.map(|rank| rank as u64),
        };
        rank.map(|rank| self.first_id + rank)
    }
}

// Indexed like `MappingPlan::tables`.
fn number_instances(conn: &Connection, plan: &MappingPlan) -> Result<Vec<TableNumbering>, String> {
    let iid = quoted(IID_COLUMN);
    let player_tables: HashSet<usize> =
        plan.tables.iter().flat_map(|table| table.roles().iter().map(|role| role.player)).collect();
    let mut next_ids: HashMap<&str, u64> = HashMap::new();
    let mut numbering = Vec::with_capacity(plan.tables.len());
    for (index, table) in plan.tables.iter().enumerate() {
        let read_error =
            |err: duckdb::Error| format!("failed to read the {IID_COLUMN} values of '{}': {err}", table.name);
        let sql = format!("SELECT count(*)::UBIGINT, min({iid}), max({iid}) FROM {}", quoted(&table.name));
        let (count, min, max): (u64, Option<u64>, Option<u64>) =
            conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).map_err(read_error)?;
        let iids = match (min, max) {
            _ if !player_tables.contains(&index) => None,
            // `_iid` is unique, so a span as wide as the count has no gaps.
            (Some(min), Some(max)) if u128::from(max - min) + 1 == u128::from(count) => {
                Some(Iids::Consecutive { first: min })
            }
            _ => {
                // The sorted table's rowid is the rank; a `row_number()` window would not spill past the memory limit.
                let ranks = format!("{SCRATCH}.rank_{index}");
                let sql = format!(
                    "CREATE OR REPLACE TABLE {ranks} AS SELECT {iid} FROM {} ORDER BY {iid}",
                    quoted(&table.name)
                );
                // The table's rowid follows the insertion order, which this one statement must keep.
                let sql = format!("SET preserve_insertion_order = true; {sql}; SET preserve_insertion_order = false");
                conn.execute_batch(&sql).map_err(read_error)?;
                Some(Iids::Ranked(ranks))
            }
        };
        let next_id = next_ids.entry(&table.type_label).or_default();
        numbering.push(TableNumbering { first_id: *next_id, count, iids });
        *next_id += count;
    }
    Ok(numbering)
}

fn object_id(label: &str, label_scoped_id: u64) -> String {
    format!("{label}:{label_scoped_id}")
}

fn attribute_id(label: &str, value: &TypeDBValue) -> String {
    format!("{label}:{value}")
}

fn emit_attributes(
    conn: &Connection,
    plan: &MappingPlan,
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
) -> Result<(), String> {
    struct AttributeSource<'a> {
        label: &'a str,
        value_type: ValueType,
        selects: Vec<String>,
        columns: Vec<String>,
    }
    let mut sources: Vec<AttributeSource<'_>> = Vec::new();
    for table in &plan.tables {
        for owns in &table.owns {
            let select = attribute_select(&table.name, owns);
            let column = format!("{}.{}", table.name, owns.column);
            match sources.iter_mut().find(|source| source.label == owns.attribute) {
                Some(source) => {
                    source.selects.push(select);
                    source.columns.push(column);
                }
                None => sources.push(AttributeSource {
                    label: &owns.attribute,
                    value_type: owns.value_type,
                    selects: vec![select],
                    columns: vec![column],
                }),
            }
        }
    }

    for source in sources {
        let sql = format!("{} ORDER BY 1", source.selects.join(" UNION "));
        let with_context = |err: String| format!("attribute '{}' (from {:?}): {err}", source.label, source.columns);
        let encode = |batches: &[RecordBatch], _: u64| {
            let mut unit = Unit::default();
            for batch in batches {
                let cells = Cells::of(batch.column(0).as_ref()).map_err(with_context)?;
                for row in 0..batch.num_rows() {
                    unit.rows += 1;
                    let Some(value) = cells.value(row, source.value_type).map_err(with_context)? else { continue };
                    unit.write_item(item::Item::Attribute(item::Attribute {
                        id: attribute_id(source.label, &value),
                        label: source.label.to_owned(),
                        attributes: Vec::new(),
                        value: Some(value.into_proto()),
                    }));
                    unit.checksums.attribute_count += 1;
                }
            }
            Ok(unit)
        };
        scan(conn, &sql, output, checksums, &encode).map_err(with_context)?;
    }
    Ok(())
}

fn attribute_select(table: &str, owns: &OwnsMapping) -> String {
    format!(
        "SELECT DISTINCT {} AS v FROM {} WHERE {} IS NOT NULL",
        owns.read(&owns.element()),
        quoted(table),
        quoted(&owns.column)
    )
}

fn emit_objects(
    conn: &Connection,
    plan: &MappingPlan,
    table_index: usize,
    numbering: &[TableNumbering],
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
) -> Result<(), String> {
    let table = &plan.tables[table_index];
    // Loader-owned aliases keep a column named `rowid` from shadowing the scan's rowid.
    let mut selects = vec![format!("t.{} AS c0", quoted(IID_COLUMN))];
    for owns in &table.owns {
        selects.push(format!("{} AS c{}", owns.projection("t"), selects.len()));
    }
    let mut role_columns: Vec<(usize, Option<usize>)> = Vec::new();
    let mut rank_joins: Vec<(usize, &str)> = Vec::new();
    for role in table.roles() {
        let column = selects.len();
        selects.push(format!("t.{} AS c{column}", quoted(&role.column)));
        role_columns.push((column, None));
        if let Some(Iids::Ranked(ranks)) = &numbering[role.player].iids {
            rank_joins.push((role_columns.len() - 1, ranks));
        }
    }
    // Each rank is joined in a statement of its own: DuckDB spills one hash join under its memory limit, but two
    // in one statement can fail to spill.
    let base = format!("SELECT {} FROM {} t", selects.join(", "), quoted(&table.name));
    let scan_error = |err: duckdb::Error| format!("scan of '{}' failed: {err}", table.name);
    let mut source = format!("({base})");
    let mut joined_table: Option<String> = None;
    let mut width = selects.len();
    for (step, (role_index, ranks)) in rank_joins.into_iter().enumerate() {
        let (column, _) = role_columns[role_index];
        let joined = format!("{SCRATCH}.joined_{table_index}_{step}");
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {joined} AS SELECT p.*, r.rowid AS c{width} FROM {source} p \
             LEFT JOIN {ranks} r ON p.c{column} = r.{}",
            quoted(IID_COLUMN)
        ))
        .map_err(scan_error)?;
        role_columns[role_index].1 = Some(width);
        width += 1;
        source = joined.clone();
        if let Some(previous) = joined_table.replace(joined) {
            conn.execute_batch(&format!("DROP TABLE {previous}")).map_err(scan_error)?;
        }
    }
    let sql = format!("SELECT * FROM {source} ORDER BY 1");

    struct Player<'a> {
        numbering: &'a TableNumbering,
        table: &'a str,
        label: &'a str,
        scoped_role: String,
    }
    let players: Vec<Player<'_>> = table
        .roles()
        .iter()
        .map(|role| Player {
            numbering: &numbering[role.player],
            table: &plan.tables[role.player].name,
            label: &plan.tables[role.player].type_label,
            scoped_role: role.relates.scoped_label(),
        })
        .collect();
    let ownerships_may_repeat =
        table.owns.iter().enumerate().any(|(index, owns)| {
            owns.multi || table.owns[..index].iter().any(|other| other.attribute == owns.attribute)
        });
    let TableNumbering { first_id, count, .. } = numbering[table_index];

    let encode = |batches: &[RecordBatch], first_row: u64| {
        let mut unit = Unit::default();
        let mut label_scoped_id = first_id + first_row;
        for batch in batches {
            let columns: Vec<Cells<'_>> =
                batch.columns().iter().map(|column| Cells::of(column.as_ref())).collect::<Result<_, _>>()?;
            let Typed::UInt64(iids) = &columns[0].typed else {
                return Err(format!("the scan of '{}' did not return {IID_COLUMN} as UBIGINT", table.name));
            };
            for row in 0..batch.num_rows() {
                let instance_id = iids.value(row);
                if unit.last_iid.is_some_and(|previous| instance_id <= previous) {
                    return Err(format!(
                        "the scan of '{}' did not keep {IID_COLUMN} order at {instance_id}",
                        table.name
                    ));
                }
                unit.first_iid.get_or_insert(instance_id);
                unit.last_iid = Some(instance_id);
                unit.rows += 1;
                let with_context = |err: String| format!("table '{}', {IID_COLUMN} = {instance_id}: {err}", table.name);

                let mut owned = Vec::new();
                let mut owned_ids = HashSet::new();
                let mut own = |owns: &OwnsMapping, value: TypeDBValue| {
                    let id = attribute_id(&owns.attribute, &value);
                    if !ownerships_may_repeat || owned_ids.insert(id.clone()) {
                        let by_value = item::owned_attribute::AttributeByValue {
                            label: owns.attribute.clone(),
                            value: Some(value.into_proto()),
                        };
                        owned.push(item::OwnedAttribute { id, attribute_by_value: Some(by_value) });
                    }
                };
                for (index, owns) in table.owns.iter().enumerate() {
                    let cells = &columns[1 + index];
                    if owns.multi {
                        let Some((elements, range)) = cells.list_range(row).map_err(&with_context)? else { continue };
                        for element in range {
                            if let Some(value) = elements.value(element, owns.value_type).map_err(&with_context)? {
                                own(owns, value);
                            }
                        }
                    } else if let Some(value) = cells.value(row, owns.value_type).map_err(&with_context)? {
                        own(owns, value);
                    }
                }
                unit.checksums.ownership_count += owned.len() as i64;

                if let TableKind::Relation { roles: bindings } = &table.kind {
                    let mut roles: Vec<item::relation::Role> = Vec::new();
                    for (index, (role, player)) in bindings.iter().zip(&players).enumerate() {
                        let (column, rank_column) = role_columns[index];
                        let Typed::UInt64(player_iids) = &columns[column].typed else {
                            return Err(with_context(format!("column '{}' was not read as UBIGINT", role.column)));
                        };
                        if player_iids.is_null(row) {
                            continue;
                        }
                        let player_iid = player_iids.value(row);
                        let rank = match rank_column.map(|column| &columns[column].typed) {
                            Some(Typed::Int64(ranks)) if !ranks.is_null(row) => Some(ranks.value(row)),
                            _ => None,
                        };
                        let player_id = player.numbering.id_of(player_iid, rank).ok_or_else(|| {
                            with_context(format!(
                                "column '{}' holds {IID_COLUMN} {player_iid}, which table '{}' does not have",
                                role.column, player.table
                            ))
                        })?;
                        let item_player = item::relation::role::Player {
                            id: object_id(player.label, player_id),
                            player_by_label_scoped_id: Some(item::relation::role::player::PlayerByLabelScopedId {
                                label: player.label.to_owned(),
                                label_scoped_id: player_id,
                            }),
                        };
                        match roles.iter_mut().find(|grouped| grouped.label == player.scoped_role) {
                            Some(grouped) if grouped.players.iter().any(|known| known.id == item_player.id) => continue,
                            Some(grouped) => grouped.players.push(item_player),
                            None => roles.push(item::relation::Role {
                                label: player.scoped_role.clone(),
                                players: vec![item_player],
                            }),
                        }
                        unit.checksums.role_count += 1;
                    }
                    if roles.is_empty() {
                        return Err(with_context(
                            "no role is filled; a relation cannot exist without players".to_owned(),
                        ));
                    }
                    unit.write_item(item::Item::Relation(item::Relation {
                        id: object_id(&table.type_label, label_scoped_id),
                        label: table.type_label.clone(),
                        label_scoped_id: Some(label_scoped_id),
                        attributes: owned,
                        roles,
                    }));
                    unit.checksums.relation_count += 1;
                } else {
                    unit.write_item(item::Item::Entity(item::Entity {
                        id: object_id(&table.type_label, label_scoped_id),
                        label: table.type_label.clone(),
                        label_scoped_id: Some(label_scoped_id),
                        attributes: owned,
                    }));
                    unit.checksums.entity_count += 1;
                }
                label_scoped_id += 1;
            }
        }
        Ok(unit)
    };
    let scanned =
        scan(conn, &sql, output, checksums, &encode).map_err(|err| format!("scan of '{}' failed: {err}", table.name));
    if let Some(joined) = joined_table {
        conn.execute_batch(&format!("DROP TABLE {joined}")).map_err(scan_error)?;
    }
    let scanned = scanned?;
    if scanned != count {
        return Err(format!("the scan of '{}' returned {scanned} of its {count} rows", table.name));
    }
    Ok(())
}

// The encoded items of one group of batches, with what the writer needs to keep the stream in order.
#[derive(Default)]
struct Unit {
    bytes: Vec<u8>,
    checksums: Checksums,
    rows: u64,
    first_iid: Option<u64>,
    last_iid: Option<u64>,
}

impl Unit {
    fn write_item(&mut self, item: item::Item) {
        Item { item: Some(item) }.encode_length_delimited(&mut self.bytes).expect("a Vec grows to fit");
    }
}

// The result is streamed in the order of the query's ORDER BY; a result read whole would stay in memory beyond
// DuckDB's memory limit. DuckDB keeps that order in a streamed result only while insertion order is preserved
// (since 2.0 it streams a parallel sort's batches as they finish), so the setting is on for the scan alone: the
// joins and checks, which it would keep from spilling, run in statements of their own.
fn scan(
    conn: &Connection,
    sql: &str,
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
    encode: &(impl Fn(&[RecordBatch], u64) -> Result<Unit, String> + Sync),
) -> Result<u64, String> {
    let scan_error = |err: duckdb::Error| format!("scan failed: {err}");
    conn.execute_batch("SET preserve_insertion_order = true").map_err(scan_error)?;
    let scanned = (|| {
        let mut statement = conn.prepare(sql).map_err(scan_error)?;
        // The iterator `stream_arrow` returns panics on a fetch error; stepping the statement reports it instead.
        let _ = statement.stream_arrow([]).map_err(scan_error)?;
        let batches = std::iter::from_fn(|| {
            statement.step().map(|array| array.map(|array| RecordBatch::from(&array))).transpose()
        });
        pipeline(batches, output, checksums, encode)
    })();
    conn.execute_batch("SET preserve_insertion_order = false").map_err(scan_error)?;
    scanned
}

fn pipeline(
    batches: impl Iterator<Item = Result<RecordBatch, duckdb::Error>>,
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
    encode: &(impl Fn(&[RecordBatch], u64) -> Result<Unit, String> + Sync),
) -> Result<u64, String> {
    let workers = thread::available_parallelism().map(|n| n.get()).unwrap_or(2);
    let (work_sender, work_receiver) = mpsc::sync_channel::<(u64, u64, Vec<RecordBatch>)>(workers);
    let (unit_sender, unit_receiver) = mpsc::sync_channel::<(u64, Result<Unit, String>)>(workers);

    thread::scope(|scope| {
        // Only the workers hold the receiver: once the writer has failed and they have all left, sending fails
        // instead of blocking on a full channel.
        let work_receiver = Arc::new(Mutex::new(work_receiver));
        for _ in 0..workers {
            let work_receiver = work_receiver.clone();
            let unit_sender = unit_sender.clone();
            scope.spawn(move || {
                loop {
                    let received = work_receiver.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).recv();
                    let Ok((sequence, first_row, batches)) = received else { return };
                    let unit = encode(&batches, first_row).and_then(|mut unit| {
                        unit.bytes = compress(&unit.bytes)?;
                        Ok(unit)
                    });
                    if unit_sender.send((sequence, unit)).is_err() {
                        return;
                    }
                }
            });
        }
        drop(work_receiver);
        drop(unit_sender);

        let writer = scope.spawn(move || -> Result<u64, String> {
            let mut pending = BTreeMap::new();
            let mut next = 0;
            let mut rows = 0;
            let mut last_iid = None;
            for (sequence, unit) in unit_receiver {
                pending.insert(sequence, unit);
                // Units are taken in order, so the first error in the data is the one reported.
                while let Some(unit) = pending.remove(&next) {
                    let unit = unit?;
                    if let (Some(last), Some(first)) = (last_iid, unit.first_iid)
                        && first <= last
                    {
                        return Err(format!("the scan did not keep {IID_COLUMN} order at {first}"));
                    }
                    last_iid = unit.last_iid.or(last_iid);
                    output.write_all(&unit.bytes).map_err(|err| format!("failed to write the output file: {err}"))?;
                    add_checksums(checksums, &unit.checksums);
                    rows += unit.rows;
                    next += 1;
                }
            }
            Ok(rows)
        });

        let mut sequence = 0;
        let mut rows = 0;
        let mut group = Vec::new();
        let (mut group_rows, mut group_bytes) = (0, 0);
        let mut send = |group: Vec<RecordBatch>, group_rows: usize| -> bool {
            let accepted = !writer.is_finished() && work_sender.send((sequence, rows, group)).is_ok();
            sequence += 1;
            rows += group_rows as u64;
            accepted
        };
        let mut fetch_error = None;
        for batch in batches {
            let batch = match batch {
                Ok(batch) => batch,
                Err(err) => {
                    fetch_error = Some(format!("scan failed: {err}"));
                    break;
                }
            };
            group_rows += batch.num_rows();
            group_bytes += batch.get_array_memory_size();
            group.push(batch);
            if group_rows >= UNIT_ROWS || group_bytes >= UNIT_BYTES {
                if !send(std::mem::take(&mut group), group_rows) {
                    break;
                }
                (group_rows, group_bytes) = (0, 0);
            }
        }
        if !group.is_empty() {
            send(group, group_rows);
        }
        drop(work_sender);
        let written = writer.join().unwrap_or_else(|_| Err("the output writer panicked".to_owned()));
        match fetch_error {
            Some(err) => Err(err),
            None => written,
        }
    })
}

// Destructured whole, so that a field added to the protocol's checksums fails to compile here.
fn add_checksums(total: &mut Checksums, unit: &Checksums) {
    let Checksums { entity_count, attribute_count, relation_count, role_count, ownership_count } = unit;
    total.entity_count += entity_count;
    total.attribute_count += attribute_count;
    total.relation_count += relation_count;
    total.role_count += role_count;
    total.ownership_count += ownership_count;
}
