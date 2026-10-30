/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    fmt::Write as _,
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
    mapping::{IID_COLUMN, MappingPlan, OwnsMapping, TableKind, quoted},
    schema::ValueType,
    value::{Cells, TypeDBValue, Typed},
};

// Each scan is a pipeline: this thread streams the rows out of DuckDB in record batches and groups them into
// units; every core turns units into encoded items; one thread writes the units as they are done. A unit is cut
// by rows or by bytes, whichever comes first, so that wide rows do not inflate what is in flight.
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
    for index in 0..plan.tables.len() {
        emit_objects(conn, plan, index, &mut output, &mut checksums)?;
    }

    write_item(&mut output, item::Item::Checksums(checksums))?;
    output.flush().map_err(|err| format!("failed to flush the output file: {err}"))?;
    Ok(checksums)
}

fn write_item(output: &mut impl Write, item: item::Item) -> Result<(), String> {
    let bytes = compress(&Item { item: Some(item) }.encode_length_delimited_to_vec())?;
    output.write_all(&bytes).map_err(|err| format!("failed to write the output file: {err}"))
}

// Sized up front: `format!` starts empty and grows the string, which costs more than writing the id.
fn object_id(label: &str, label_scoped_id: u64) -> String {
    let mut id = String::with_capacity(label.len() + 21);
    let _ = write!(id, "{label}:{label_scoped_id}");
    id
}

fn attribute_id(label: &str, value: &TypeDBValue) -> String {
    let mut id = String::with_capacity(label.len() + 32);
    let _ = write!(id, "{label}:{value}");
    id
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
        // One aggregate over all the columns: a UNION of DISTINCTs plans one per column and one more, which fail to
        // spill under limits that a single one streams through. Values in order make the server's import of them
        // faster (6% on 10M rows of 30 attributes); a streamed result keeps its order only while insertion order
        // is preserved, which this one statement can afford.
        let sql = match source.selects.as_slice() {
            [select] => format!("{select} ORDER BY 1"),
            selects => format!("SELECT DISTINCT v FROM ({}) ORDER BY 1", selects.join(" UNION ALL ")),
        };
        let with_context = |err: String| format!("attribute '{}' (from {:?}): {err}", source.label, source.columns);
        let encode = |batches: &[RecordBatch]| {
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
        conn.execute_batch("SET preserve_insertion_order = true").map_err(|err| with_context(err.to_string()))?;
        let scanned = scan(conn, &sql, output, checksums, &encode).map_err(with_context);
        conn.execute_batch("SET preserve_insertion_order = false").map_err(|err| with_context(err.to_string()))?;
        scanned?;
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
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
) -> Result<(), String> {
    let table = &plan.tables[table_index];
    let mut selects = vec![format!("t.{} AS c0", quoted(IID_COLUMN))];
    for owns in &table.owns {
        selects.push(format!("{} AS c{}", owns.projection("t"), selects.len()));
    }
    let mut role_columns = Vec::new();
    for role in table.roles() {
        role_columns.push(selects.len());
        selects.push(format!("t.{} AS c{}", quoted(&role.column), selects.len()));
    }
    let sql = format!("SELECT {} FROM {} t", selects.join(", "), quoted(&table.name));

    // An instance's label-scoped id is its `_iid`: the checks have made it unique within its type and below 2^63.
    struct Player<'a> {
        label: &'a str,
        scoped_role: String,
    }
    let players: Vec<Player<'_>> = table
        .roles()
        .iter()
        .map(|role| Player { label: &plan.tables[role.player].type_label, scoped_role: role.relates.scoped_label() })
        .collect();
    let ownerships_may_repeat =
        table.owns.iter().enumerate().any(|(index, owns)| {
            owns.multi || table.owns[..index].iter().any(|other| other.attribute == owns.attribute)
        });
    let count_sql = format!("SELECT count(*)::UBIGINT FROM {}", quoted(&table.name));
    let count: u64 = conn
        .query_row(&count_sql, [], |row| row.get(0))
        .map_err(|err| format!("failed to count the rows of '{}': {err}", table.name))?;

    let encode = |batches: &[RecordBatch]| {
        let mut unit = Unit::default();
        for batch in batches {
            let columns: Vec<Cells<'_>> =
                batch.columns().iter().map(|column| Cells::of(column.as_ref())).collect::<Result<_, _>>()?;
            let Typed::UInt64(iids) = &columns[0].typed else {
                return Err(format!("the scan of '{}' did not return {IID_COLUMN} as UBIGINT", table.name));
            };
            for row in 0..batch.num_rows() {
                let instance_id = iids.value(row);
                unit.rows += 1;
                let with_context = |err: String| format!("table '{}', {IID_COLUMN} = {instance_id}: {err}", table.name);
                let label_scoped_id = instance_id;

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
                        let Typed::UInt64(player_iids) = &columns[role_columns[index]].typed else {
                            return Err(with_context(format!("column '{}' was not read as UBIGINT", role.column)));
                        };
                        if player_iids.is_null(row) {
                            continue;
                        }
                        let player_id = player_iids.value(row);
                        let grouped = roles.iter().position(|grouped| grouped.label == player.scoped_role);
                        let known = |grouped: &item::relation::Role| {
                            grouped
                                .players
                                .iter()
                                .filter_map(|known| known.player_by_label_scoped_id.as_ref())
                                .any(|known| known.label_scoped_id == player_id && known.label == player.label)
                        };
                        if grouped.is_some_and(|index| known(&roles[index])) {
                            continue;
                        }
                        let item_player = item::relation::role::Player {
                            id: object_id(player.label, player_id),
                            player_by_label_scoped_id: Some(item::relation::role::player::PlayerByLabelScopedId {
                                label: player.label.to_owned(),
                                label_scoped_id: player_id,
                            }),
                        };
                        match grouped {
                            Some(index) => roles[index].players.push(item_player),
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
            }
        }
        Ok(unit)
    };
    let scanned = scan(conn, &sql, output, checksums, &encode)
        .map_err(|err| format!("scan of '{}' failed: {err}", table.name))?;
    if scanned != count {
        return Err(format!("the scan of '{}' returned {scanned} of its {count} rows", table.name));
    }
    Ok(())
}

// The encoded items of one group of batches.
#[derive(Default)]
struct Unit {
    bytes: Vec<u8>,
    checksums: Checksums,
    rows: u64,
}

impl Unit {
    fn write_item(&mut self, item: item::Item) {
        Item { item: Some(item) }.encode_length_delimited(&mut self.bytes).expect("a Vec grows to fit");
    }
}

// A result read whole would stay in memory beyond DuckDB's memory limit, so it is streamed.
fn scan(
    conn: &Connection,
    sql: &str,
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
    encode: &(impl Fn(&[RecordBatch]) -> Result<Unit, String> + Sync),
) -> Result<u64, String> {
    let scan_error = |err: duckdb::Error| format!("scan failed: {err}");
    let mut statement = conn.prepare(sql).map_err(scan_error)?;
    // Stepping the statement, rather than iterating, reports a fetch error as an error on every duckdb-rs version.
    let _ = statement.stream_arrow([]).map_err(scan_error)?;
    let batches =
        std::iter::from_fn(|| statement.step().map(|array| array.map(|array| RecordBatch::from(&array))).transpose());
    pipeline(batches, output, checksums, encode)
}

fn pipeline(
    batches: impl Iterator<Item = Result<RecordBatch, duckdb::Error>>,
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
    encode: &(impl Fn(&[RecordBatch]) -> Result<Unit, String> + Sync),
) -> Result<u64, String> {
    let workers = thread::available_parallelism().map(|n| n.get()).unwrap_or(2);
    let (work_sender, work_receiver) = mpsc::sync_channel::<Vec<RecordBatch>>(workers);
    let (unit_sender, unit_receiver) = mpsc::sync_channel::<Result<Unit, String>>(workers);

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
                    let Ok(batches) = received else { return };
                    let unit = encode(&batches).and_then(|mut unit| {
                        unit.bytes = compress(&unit.bytes)?;
                        Ok(unit)
                    });
                    if unit_sender.send(unit).is_err() {
                        return;
                    }
                }
            });
        }
        drop(work_receiver);
        drop(unit_sender);

        let writer = scope.spawn(move || -> Result<u64, String> {
            let mut rows = 0;
            for unit in unit_receiver {
                let unit = unit?;
                output.write_all(&unit.bytes).map_err(|err| format!("failed to write the output file: {err}"))?;
                add_checksums(checksums, &unit.checksums);
                rows += unit.rows;
            }
            Ok(rows)
        });

        let mut group = Vec::new();
        let (mut group_rows, mut group_bytes) = (0, 0);
        let send = |group: Vec<RecordBatch>| !writer.is_finished() && work_sender.send(group).is_ok();
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
                if !send(std::mem::take(&mut group)) {
                    break;
                }
                (group_rows, group_bytes) = (0, 0);
            }
        }
        if !group.is_empty() {
            send(group);
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
