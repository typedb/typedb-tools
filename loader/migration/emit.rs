/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
};

use duckdb::{
    Connection,
    types::{Value, ValueRef},
};
use prost::Message;
use typedb_protocol::migration::{Item, item};

use crate::{
    iid::{StringDisambiguators, attribute_iid, entity_iid, relation_iid},
    mapping::{MappingPlan, OwnsMapping, TableMapping},
    schema::ValueType,
    validate::quoted,
    value::{TypeDBValue, convert_cell, convert_list_element},
};

#[derive(Debug, Default, Clone)]
pub struct Checksums {
    pub attribute_count: i64,
    pub entity_count: i64,
    pub relation_count: i64,
    pub ownership_count: i64,
    pub role_count: i64,
}

pub(crate) struct Emitter {
    writer: BufWriter<File>,
    pub(crate) checksums: Checksums,
    disambiguators: StringDisambiguators,
}

impl Emitter {
    pub(crate) fn new(file: File) -> Self {
        Self {
            writer: BufWriter::new(file),
            checksums: Checksums::default(),
            disambiguators: StringDisambiguators::default(),
        }
    }

    fn write_item(&mut self, item: item::Item) -> Result<(), String> {
        let message = Item { item: Some(item) };
        let mut buffer = Vec::with_capacity(message.encoded_len() + 10);
        message.encode_length_delimited(&mut buffer).expect("vec write cannot fail");
        self.writer.write_all(&buffer).map_err(|err| format!("failed to write the output file: {err}"))
    }

    pub(crate) fn finish(mut self) -> Result<Checksums, String> {
        self.write_item(item::Item::Checksums(item::Checksums {
            entity_count: self.checksums.entity_count,
            attribute_count: self.checksums.attribute_count,
            relation_count: self.checksums.relation_count,
            role_count: self.checksums.role_count,
            ownership_count: self.checksums.ownership_count,
        }))?;
        self.writer.flush().map_err(|err| format!("failed to flush the output file: {err}"))?;
        Ok(self.checksums)
    }
}

pub(crate) fn emit(
    conn: &Connection,
    plan: &MappingPlan,
    typedb_version: &str,
    original_database: &str,
    file: File,
) -> Result<Checksums, String> {
    let mut emitter = Emitter::new(file);
    emitter.write_item(item::Item::Header(item::Header {
        typedb_version: typedb_version.to_owned(),
        original_database: original_database.to_owned(),
    }))?;

    emit_attributes(conn, plan, &mut emitter)?;
    for table in plan.tables.iter().filter(|t| !t.is_relation) {
        emit_objects(conn, table, &mut emitter)?;
    }
    for table in plan.tables.iter().filter(|t| t.is_relation) {
        emit_objects(conn, table, &mut emitter)?;
    }

    emitter.finish()
}

fn emit_attributes(conn: &Connection, plan: &MappingPlan, emitter: &mut Emitter) -> Result<(), String> {
    struct AttributeSource<'a> {
        label: &'a str,
        type_id: u16,
        value_type: ValueType,
        keyed: bool,
        selects: Vec<String>,
    }
    let mut sources: Vec<AttributeSource<'_>> = Vec::new();
    for table in &plan.tables {
        for owns in &table.owns {
            let select = attribute_select(&table.table, owns);
            match sources.iter_mut().find(|source| source.label == owns.attribute) {
                Some(source) => source.selects.push(select),
                None => sources.push(AttributeSource {
                    label: &owns.attribute,
                    type_id: owns.attribute_type_id,
                    value_type: owns.value_type,
                    keyed: owns.identity_key("v").is_some(),
                    selects: vec![select],
                }),
            }
        }
    }

    for source in sources {
        // UNION deduplicates across all selects; DISTINCT inside each handles single-source
        // runs. ORDER BY pins the stream order, keeping the output file byte-deterministic.
        let order = if source.keyed { "1, 2" } else { "1" };
        let sql = format!("{} ORDER BY {order}", source.selects.join(" UNION "));
        let scan_error = |err: duckdb::Error| format!("attribute scan for '{}' failed: {err}", source.label);
        let colliding_hashes = match source.value_type {
            ValueType::String => {
                // A first pass holds one hash per long string, not the strings themselves.
                let mut stmt = conn.prepare(&sql).map_err(scan_error)?;
                let mut rows = stmt.query([]).map_err(scan_error)?;
                let mut hashes = Vec::new();
                while let Some(row) = rows.next().map_err(scan_error)? {
                    if let ValueRef::Text(bytes) = row.get_ref(0).map_err(scan_error)? {
                        hashes.extend(StringDisambiguators::hash(bytes));
                    }
                }
                StringDisambiguators::colliding_hashes(hashes)
            }
            _ => HashSet::new(),
        };
        let mut next_disambiguators = HashMap::new();

        let mut stmt = conn.prepare(&sql).map_err(scan_error)?;
        let mut rows = stmt.query([]).map_err(scan_error)?;
        while let Some(row) = rows.next().map_err(|err| err.to_string())? {
            let cell = row.get_ref(0).map_err(|err| err.to_string())?;
            let with_context = |err: String| format!("attribute '{}': {err}", source.label);
            let Some(value) = convert_cell(cell, source.value_type).map_err(with_context)? else { continue };
            if let (false, TypeDBValue::String(string)) = (colliding_hashes.is_empty(), &value) {
                emitter
                    .disambiguators
                    .assign(source.type_id, &colliding_hashes, &mut next_disambiguators, string)
                    .map_err(with_context)?;
            }
            emitter.write_item(item::Item::Attribute(item::Attribute {
                id: attribute_iid(source.type_id, &value, &emitter.disambiguators),
                label: source.label.to_owned(),
                attributes: Vec::new(),
                value: Some(value.to_proto()),
            }))?;
            emitter.checksums.attribute_count += 1;
        }
    }
    Ok(())
}

fn attribute_select(table: &str, owns: &OwnsMapping) -> String {
    let column = quoted(&owns.column);
    let element = if owns.multi { format!("unnest({column})") } else { column.clone() };
    let key = owns.identity_key(&element).map(|key| format!(", {key} AS k")).unwrap_or_default();
    format!("SELECT DISTINCT {} AS v{key} FROM {} WHERE {column} IS NOT NULL", owns.scalar(&element), quoted(table))
}

fn emit_objects(conn: &Connection, table: &TableMapping, emitter: &mut Emitter) -> Result<(), String> {
    let mut selects = vec![format!("{}::UBIGINT", quoted(&table.iid_column))];
    for owns in &table.owns {
        selects.push(owns.projection());
    }
    for role in &table.roles {
        selects.push(format!("{}::UBIGINT", quoted(&role.column)));
    }
    // Ordered by instance id so the output file is byte-deterministic across runs.
    let sql = format!("SELECT {} FROM {} ORDER BY 1", selects.join(", "), quoted(&table.table));
    let mut stmt = conn.prepare(&sql).map_err(|err| format!("scan of '{}' failed: {err}", table.table))?;
    let mut rows = stmt.query([]).map_err(|err| format!("scan of '{}' failed: {err}", table.table))?;

    while let Some(row) = rows.next().map_err(|err| err.to_string())? {
        let instance_id: u64 = row.get(0).map_err(|err| format!("table '{}': {err}", table.table))?;
        let with_context =
            |err: String| format!("table '{}', {} = {instance_id}: {err}", table.table, table.iid_column);

        // TypeDB ownerships are a set: a value repeated across a row's columns or list
        // elements is owned once.
        let mut owned = Vec::new();
        let mut owned_ids = HashSet::new();
        let mut own = |id: String| {
            if owned_ids.insert(id.clone()) {
                owned.push(item::OwnedAttribute { id });
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
                        own(attribute_iid(owns.attribute_type_id, &value, &emitter.disambiguators));
                    }
                }
            } else {
                let cell = row.get_ref(column_index).map_err(|err| with_context(err.to_string()))?;
                if let Some(value) = convert_cell(cell, owns.value_type).map_err(&with_context)? {
                    own(attribute_iid(owns.attribute_type_id, &value, &emitter.disambiguators));
                }
            }
        }
        emitter.checksums.ownership_count += owned.len() as i64;

        if table.is_relation {
            // Grouped in role-binding order so output is deterministic across runs.
            let mut roles: Vec<item::relation::Role> = Vec::new();
            for (index, role) in table.roles.iter().enumerate() {
                let column_index = 1 + table.owns.len() + index;
                let player_id: Option<u64> = row.get(column_index).map_err(|err| with_context(err.to_string()))?;
                let Some(player_id) = player_id else { continue };
                let player_iid = if role.player_is_relation {
                    relation_iid(role.player_type_id, player_id)
                } else {
                    entity_iid(role.player_type_id, player_id)
                };
                let label = role.scoped_role.clone();
                // A player named twice in one role plays it once, as with ownerships.
                match roles.iter_mut().find(|grouped| grouped.label == label) {
                    Some(grouped) if grouped.players.iter().any(|player| player.id == player_iid) => continue,
                    Some(grouped) => grouped.players.push(item::relation::role::Player { id: player_iid }),
                    None => roles.push(item::relation::Role {
                        label,
                        players: vec![item::relation::role::Player { id: player_iid }],
                    }),
                }
                emitter.checksums.role_count += 1;
            }
            emitter.write_item(item::Item::Relation(item::Relation {
                id: relation_iid(table.type_id, instance_id),
                label: table.type_label.clone(),
                attributes: owned,
                roles,
            }))?;
            emitter.checksums.relation_count += 1;
        } else {
            emitter.write_item(item::Item::Entity(item::Entity {
                id: entity_iid(table.type_id, instance_id),
                label: table.type_label.clone(),
                attributes: owned,
            }))?;
            emitter.checksums.entity_count += 1;
        }
    }
    Ok(())
}
