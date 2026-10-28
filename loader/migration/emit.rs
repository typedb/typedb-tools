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
    mapping::{IID_COLUMN, MappingPlan, OwnsMapping, TableKind, TableMapping, quoted},
    schema::ValueType,
    value::{Cells, TypeDBValue, Typed},
};

// Each scan is a pipeline: this thread streams the sorted rows out of DuckDB in record batches and groups them
// into units; every core turns units into encoded items; one thread writes the units back in order. A unit is
// cut by rows or by bytes, whichever comes first, so that wide rows do not inflate what is in flight.
const UNIT_ROWS: usize = 32_768;
const UNIT_BYTES: usize = 32 << 20;
const OUTPUT_BUFFER: usize = 8 << 20;

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
    for table in &plan.tables {
        emit_objects(conn, table, &numbering, &mut output, &mut checksums)?;
    }

    write_item(&mut output, item::Item::Checksums(checksums))?;
    output.flush().map_err(|err| format!("failed to flush the output file: {err}"))?;
    Ok(checksums)
}

fn write_item(output: &mut impl Write, item: item::Item) -> Result<(), String> {
    let bytes = Item { item: Some(item) }.encode_length_delimited_to_vec();
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

fn number_instances<'a>(conn: &Connection, plan: &'a MappingPlan) -> Result<HashMap<&'a str, TableNumbering>, String> {
    let iid = quoted(IID_COLUMN);
    let player_tables: HashSet<&str> =
        plan.tables.iter().flat_map(|table| table.roles().iter().map(|role| role.player_table.as_str())).collect();
    let mut next_ids: HashMap<&str, u64> = HashMap::new();
    let mut numbering = HashMap::new();
    for (index, table) in plan.tables.iter().enumerate() {
        let read_error =
            |err: duckdb::Error| format!("failed to read the {IID_COLUMN} values of '{}': {err}", table.name);
        let sql = format!("SELECT count(*)::UBIGINT, min({iid}), max({iid}) FROM {}", quoted(&table.name));
        let (count, min, max): (u64, Option<u64>, Option<u64>) =
            conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).map_err(read_error)?;
        let iids = match (min, max) {
            _ if !player_tables.contains(table.name.as_str()) => None,
            // `_iid` is unique, so a span as wide as the count has no gaps.
            (Some(min), Some(max)) if u128::from(max - min) + 1 == u128::from(count) => {
                Some(Iids::Consecutive { first: min })
            }
            _ => {
                // The sorted table's rowid is the rank; a `row_number()` window would not spill past the memory limit.
                let ranks = format!("temp.typedb_loader_rank_{index}");
                let sql = format!(
                    "CREATE OR REPLACE TEMP TABLE {ranks} AS SELECT {iid} FROM {} ORDER BY {iid}",
                    quoted(&table.name)
                );
                conn.execute_batch(&sql).map_err(read_error)?;
                Some(Iids::Ranked(ranks))
            }
        };
        let next_id = next_ids.entry(&table.type_label).or_default();
        numbering.insert(table.name.as_str(), TableNumbering { first_id: *next_id, count, iids });
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
    table: &TableMapping,
    numbering: &HashMap<&str, TableNumbering>,
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
) -> Result<(), String> {
    // Loader-owned aliases keep a column named `rowid` from shadowing the scan's rowid.
    let mut selects = vec![format!("t.{} AS c0", quoted(IID_COLUMN))];
    for owns in &table.owns {
        selects.push(format!("{} AS c{}", owns.projection("t"), selects.len()));
    }
    let mut joins = String::new();
    let mut role_columns: Vec<(usize, Option<usize>)> = Vec::new();
    for (index, role) in table.roles().iter().enumerate() {
        let column = selects.len();
        selects.push(format!("t.{} AS c{column}", quoted(&role.column)));
        let rank_column = match &numbering[role.player_table.as_str()].iids {
            Some(Iids::Ranked(ranks)) => {
                let alias = format!("r{index}");
                joins += &format!(
                    " LEFT JOIN {ranks} {alias} ON t.{} = {alias}.{}",
                    quoted(&role.column),
                    quoted(IID_COLUMN)
                );
                selects.push(format!("{alias}.rowid AS c{}", selects.len()));
                Some(selects.len() - 1)
            }
            _ => None,
        };
        role_columns.push((column, rank_column));
    }
    let sql = format!("SELECT {} FROM {} t{joins} ORDER BY 1", selects.join(", "), quoted(&table.name));
    let scan_error = |err: String| format!("scan of '{}' failed: {err}", table.name);

    let player_numbering: Vec<&TableNumbering> =
        table.roles().iter().map(|role| &numbering[role.player_table.as_str()]).collect();
    let ownerships_may_repeat =
        table.owns.iter().enumerate().any(|(index, owns)| {
            owns.multi || table.owns[..index].iter().any(|other| other.attribute == owns.attribute)
        });
    let TableNumbering { first_id, count, .. } = numbering[table.name.as_str()];

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
                    for (index, (role, players)) in bindings.iter().zip(&player_numbering).enumerate() {
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
                        let player_id = players.id_of(player_iid, rank).ok_or_else(|| {
                            with_context(format!(
                                "column '{}' holds {IID_COLUMN} {player_iid}, which table '{}' does not have",
                                role.column, role.player_table
                            ))
                        })?;
                        let player = item::relation::role::Player {
                            id: object_id(&role.player_label, player_id),
                            player_by_label_scoped_id: Some(item::relation::role::player::PlayerByLabelScopedId {
                                label: role.player_label.clone(),
                                label_scoped_id: player_id,
                            }),
                        };
                        match roles.iter_mut().find(|grouped| grouped.label == role.scoped_role) {
                            Some(grouped) if grouped.players.iter().any(|known| known.id == player.id) => continue,
                            Some(grouped) => grouped.players.push(player),
                            None => roles
                                .push(item::relation::Role { label: role.scoped_role.clone(), players: vec![player] }),
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
    let scanned = scan(conn, &sql, output, checksums, &encode).map_err(scan_error)?;
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

// A result read whole stays in memory beyond DuckDB's memory limit; a temporary table spills to disk, and
// streams back in insertion order, which `preserve_insertion_order` keeps equal to the sort order.
fn scan(
    conn: &Connection,
    sql: &str,
    output: &mut (impl Write + Send),
    checksums: &mut Checksums,
    encode: &(impl Fn(&[RecordBatch], u64) -> Result<Unit, String> + Sync),
) -> Result<u64, String> {
    const SCAN: &str = "temp.typedb_loader_scan";
    let scan_error = |err: duckdb::Error| format!("scan failed: {err}");
    conn.execute_batch(&format!("CREATE OR REPLACE TEMP TABLE {SCAN} AS {sql}")).map_err(scan_error)?;
    let rows = (|| {
        let mut statement = conn.prepare(&format!("SELECT * FROM {SCAN}")).map_err(scan_error)?;
        // The iterator `stream_arrow` returns panics on a fetch error; stepping the statement reports it instead.
        let _ = statement.stream_arrow([]).map_err(scan_error)?;
        let batches = std::iter::from_fn(|| {
            statement.step().map(|array| array.map(|array| RecordBatch::from(&array))).transpose()
        });
        pipeline(batches, output, checksums, encode)
    })();
    let dropped = conn.execute_batch(&format!("DROP TABLE {SCAN}")).map_err(scan_error);
    rows.and_then(|rows| dropped.map(|()| rows))
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
                    if unit_sender.send((sequence, encode(&batches, first_row))).is_err() {
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
                    checksums.entity_count += unit.checksums.entity_count;
                    checksums.attribute_count += unit.checksums.attribute_count;
                    checksums.relation_count += unit.checksums.relation_count;
                    checksums.role_count += unit.checksums.role_count;
                    checksums.ownership_count += unit.checksums.ownership_count;
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
