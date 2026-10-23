/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    io::{BufWriter, Write},
};

use duckdb::{Connection, Row, types::Value};
use prost::Message;
use typedb_protocol::migration::{
    Item,
    item::{self, Checksums},
};

use crate::{
    mapping::{IID_COLUMN, MappingPlan, OwnsMapping, TableMapping, quoted},
    schema::ValueType,
    value::{TypeDBValue, convert_cell, convert_list_element},
};

struct Emitter<W: Write> {
    writer: BufWriter<W>,
    buffer: Vec<u8>,
    checksums: Checksums,
}

impl<W: Write> Emitter<W> {
    fn new(output: W) -> Self {
        Self { writer: BufWriter::new(output), buffer: Vec::new(), checksums: Checksums::default() }
    }

    fn write_item(&mut self, item: item::Item) -> Result<(), String> {
        self.buffer.clear();
        Item { item: Some(item) }.encode_length_delimited(&mut self.buffer).expect("a Vec grows to fit");
        self.writer.write_all(&self.buffer).map_err(|err| format!("failed to write the output file: {err}"))
    }

    fn finish(mut self) -> Result<Checksums, String> {
        self.write_item(item::Item::Checksums(self.checksums))?;
        self.writer.flush().map_err(|err| format!("failed to flush the output file: {err}"))?;
        Ok(self.checksums)
    }
}

/// Header, attributes, entities, relations, checksums: the order the importer expects.
pub(crate) fn emit(
    conn: &Connection,
    plan: &MappingPlan,
    typedb_version: &str,
    original_database: &str,
    output: impl Write,
) -> Result<Checksums, String> {
    let mut emitter = Emitter::new(output);
    emitter.write_item(item::Item::Header(item::Header {
        typedb_version: typedb_version.to_owned(),
        original_database: original_database.to_owned(),
        guarantees: Some(item::header::Guarantees {
            ordering: item::header::guarantees::Ordering::AttributesEntitiesRelations.into(),
            attributes_unique: true,
            players_by_label_scoped_ids: true,
            owned_attributes_by_value: true,
        }),
    }))?;

    emit_attributes(conn, plan, &mut emitter)?;
    let numbering = number_instances(conn, plan)?;
    for table in &plan.tables {
        emit_objects(conn, table, &numbering, &mut emitter)?;
    }
    emitter.finish()
}

/// How a table's rows are numbered: the instances of a type get 0, 1, 2, ... in file order,
/// table after table and by `_iid` within a table.
struct TableNumbering {
    first_id: u64,
    iids: Iids,
}

/// A table's `_iid` values, kept to translate an FK value into the player's number.
enum Iids {
    /// `first..first + count`, as `row_number()` staging gives: nothing to keep.
    Consecutive { first: u64, count: u64 },
    /// Sorted, for a table that holds players; empty for any other.
    Sorted(Vec<u64>),
}

impl TableNumbering {
    fn id_of(&self, iid: u64) -> Option<u64> {
        let rank = match &self.iids {
            Iids::Consecutive { first, count } => iid.checked_sub(*first).filter(|rank| rank < count),
            Iids::Sorted(iids) => iids.binary_search(&iid).ok().map(|rank| rank as u64),
        };
        rank.map(|rank| self.first_id + rank)
    }
}

fn number_instances<'a>(conn: &Connection, plan: &'a MappingPlan) -> Result<HashMap<&'a str, TableNumbering>, String> {
    let iid = quoted(IID_COLUMN);
    let mut next_ids: HashMap<&str, u64> = HashMap::new();
    let mut numbering = HashMap::new();
    for table in &plan.tables {
        let read_error =
            |err: duckdb::Error| format!("failed to read the {IID_COLUMN} values of '{}': {err}", table.name);
        let sql = format!("SELECT count(*)::UBIGINT, min({iid}), max({iid}) FROM {}", quoted(&table.name));
        let (count, min, max): (u64, Option<u64>, Option<u64>) =
            conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).map_err(read_error)?;
        let holds_players =
            plan.tables.iter().any(|other| other.roles.iter().any(|role| role.player_table == table.name));
        let iids = match (min, max) {
            // `_iid` is unique, so a span as wide as the count has no gaps.
            (Some(min), Some(max)) if u128::from(max - min) + 1 == u128::from(count) => {
                Iids::Consecutive { first: min, count }
            }
            _ if holds_players => {
                let mut iids = Vec::with_capacity(count as usize);
                let sql = format!("SELECT {iid} FROM {}", quoted(&table.name));
                for_each_row(conn, &sql, read_error, |row| Ok(iids.push(row.get(0).map_err(read_error)?)))?;
                iids.sort_unstable();
                Iids::Sorted(iids)
            }
            _ => Iids::Sorted(Vec::new()),
        };
        let next_id = next_ids.entry(&table.type_label).or_default();
        numbering.insert(table.name.as_str(), TableNumbering { first_id: *next_id, iids });
        *next_id += count;
    }
    Ok(numbering)
}

/// The string ids, for an importer that resolves by lookup instead of by the label-scoped
/// ids and the inline values.
fn object_id(label: &str, label_scoped_id: u64) -> String {
    format!("{label}:{label_scoped_id}")
}

fn attribute_id(label: &str, value: &TypeDBValue) -> String {
    format!("{label}:{value}")
}

/// One deduplicating scan per attribute type over every column populating it.
fn emit_attributes(conn: &Connection, plan: &MappingPlan, emitter: &mut Emitter<impl Write>) -> Result<(), String> {
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
        // UNION deduplicates across the selects; ORDER BY makes the stream deterministic.
        let sql = format!("{} ORDER BY 1", source.selects.join(" UNION "));
        let scan_error = |err: duckdb::Error| format!("attribute scan for '{}' failed: {err}", source.label);
        for_each_row(conn, &sql, scan_error, |row| {
            let cell = row.get_ref(0).map_err(scan_error)?;
            let with_context = |err: String| format!("attribute '{}' (from {:?}): {err}", source.label, source.columns);
            let Some(value) = convert_cell(cell, source.value_type).map_err(with_context)? else { return Ok(()) };
            emitter.write_item(item::Item::Attribute(item::Attribute {
                id: attribute_id(source.label, &value),
                label: source.label.to_owned(),
                attributes: Vec::new(),
                value: Some(value.into_proto()),
            }))?;
            emitter.checksums.attribute_count += 1;
            Ok(())
        })?;
    }
    Ok(())
}

/// DuckDB's row group size: a page reads one row group of a scan.
const PAGE_ROWS: i64 = 122_880;

/// Passes the rows of `sql` to `on_row`, in the query's order. A result read whole stays in
/// memory until dropped; kept in a temporary table, which DuckDB spills to disk, it is read
/// a page at a time instead, by row id, which follows the order the rows were stored in.
fn for_each_row(
    conn: &Connection,
    sql: &str,
    scan_error: impl Fn(duckdb::Error) -> String,
    mut on_row: impl FnMut(&Row<'_>) -> Result<(), String>,
) -> Result<(), String> {
    const SCAN: &str = "temp.typedb_loader_scan";
    conn.execute_batch(&format!("CREATE OR REPLACE TEMP TABLE typedb_loader_scan AS {sql}")).map_err(&scan_error)?;
    let mut read_pages = || {
        let count: i64 =
            conn.query_row(&format!("SELECT count(*) FROM {SCAN}"), [], |row| row.get(0)).map_err(&scan_error)?;
        let mut page = conn
            .prepare(&format!("SELECT * FROM {SCAN} WHERE rowid >= ? AND rowid < ? ORDER BY rowid"))
            .map_err(&scan_error)?;
        for start in (0..count).step_by(PAGE_ROWS as usize) {
            let mut rows = page.query([start, start + PAGE_ROWS]).map_err(&scan_error)?;
            while let Some(row) = rows.next().map_err(&scan_error)? {
                on_row(row)?;
            }
        }
        Ok(())
    };
    let read = read_pages();
    let dropped = conn.execute_batch(&format!("DROP TABLE {SCAN}")).map_err(&scan_error);
    read.and(dropped)
}

fn attribute_select(table: &str, owns: &OwnsMapping) -> String {
    let column = quoted(&owns.column);
    let element = if owns.multi { format!("unnest({column})") } else { column.clone() };
    format!("SELECT DISTINCT {} AS v FROM {} WHERE {column} IS NOT NULL", owns.read(&element), quoted(table))
}

/// The entities or relations of one table, with their ownerships and role players.
fn emit_objects(
    conn: &Connection,
    table: &TableMapping,
    numbering: &HashMap<&str, TableNumbering>,
    emitter: &mut Emitter<impl Write>,
) -> Result<(), String> {
    let mut selects = vec![quoted(IID_COLUMN)];
    for owns in &table.owns {
        selects.push(owns.projection());
    }
    for role in &table.roles {
        selects.push(quoted(&role.column));
    }
    let sql = format!("SELECT {} FROM {} ORDER BY 1", selects.join(", "), quoted(&table.name));
    let scan_error = |err: duckdb::Error| format!("scan of '{}' failed: {err}", table.name);

    let player_numbering: Vec<&TableNumbering> =
        table.roles.iter().map(|role| &numbering[role.player_table.as_str()]).collect();
    // Only a list column, or two columns feeding one attribute, can repeat an ownership.
    let ownerships_may_repeat =
        table.owns.iter().enumerate().any(|(index, owns)| {
            owns.multi || table.owns[..index].iter().any(|other| other.attribute == owns.attribute)
        });
    let mut label_scoped_id = numbering[table.name.as_str()].first_id;
    let mut previous_iid = None;
    for_each_row(conn, &sql, scan_error, |row| {
        let instance_id: u64 = row.get(0).map_err(scan_error)?;
        // The numbering, and so every player reference, rests on reading the rows in `_iid` order.
        if previous_iid.is_some_and(|previous| instance_id <= previous) {
            return Err(format!("the scan of '{}' did not keep {IID_COLUMN} order at {instance_id}", table.name));
        }
        previous_iid = Some(instance_id);
        let with_context = |err: String| format!("table '{}', {IID_COLUMN} = {instance_id}: {err}", table.name);

        // Ownerships and role players are sets: a repeat within a row is written once.
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
            let column_index = 1 + index;
            if owns.multi {
                let Value::List(elements) =
                    row.get::<_, Value>(column_index).map_err(|err| with_context(err.to_string()))?
                else {
                    continue; // NULL list
                };
                for element in elements {
                    if let Some(value) = convert_list_element(element, owns.value_type).map_err(&with_context)? {
                        own(owns, value);
                    }
                }
            } else {
                let cell = row.get_ref(column_index).map_err(|err| with_context(err.to_string()))?;
                if let Some(value) = convert_cell(cell, owns.value_type).map_err(&with_context)? {
                    own(owns, value);
                }
            }
        }
        emitter.checksums.ownership_count += owned.len() as i64;

        if table.is_relation {
            let mut roles: Vec<item::relation::Role> = Vec::new();
            for (index, (role, players)) in table.roles.iter().zip(&player_numbering).enumerate() {
                let column_index = 1 + table.owns.len() + index;
                let player_iid: Option<u64> = row.get(column_index).map_err(|err| with_context(err.to_string()))?;
                let Some(player_iid) = player_iid else { continue };
                let player_id = players.id_of(player_iid).ok_or_else(|| {
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
                    None => roles.push(item::relation::Role { label: role.scoped_role.clone(), players: vec![player] }),
                }
                emitter.checksums.role_count += 1;
            }
            emitter.write_item(item::Item::Relation(item::Relation {
                id: object_id(&table.type_label, label_scoped_id),
                label: table.type_label.clone(),
                label_scoped_id: Some(label_scoped_id),
                attributes: owned,
                roles,
            }))?;
            emitter.checksums.relation_count += 1;
        } else {
            emitter.write_item(item::Item::Entity(item::Entity {
                id: object_id(&table.type_label, label_scoped_id),
                label: table.type_label.clone(),
                label_scoped_id: Some(label_scoped_id),
                attributes: owned,
            }))?;
            emitter.checksums.entity_count += 1;
        }
        label_scoped_id += 1;
        Ok(())
    })
}
