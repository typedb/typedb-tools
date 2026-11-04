/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    Findings,
    catalog::{Catalog, Column, ColumnType, ForeignKey, Table},
    schema::{ObjectType, Relates, Schema, ValueType},
    sql::{self, IID_COLUMN, duckdb_type},
    value::DECIMAL_FRACTIONAL_DIGITS,
};

pub(crate) fn resolve(catalog: &Catalog, schema: &Schema, findings: &mut Findings) -> MappingPlan {
    for unread in &catalog.unread_tables_and_views {
        findings.warning(format!(
            "{unread} is not loaded: only tables of the 'main' schema are read (materialise a view as a table)"
        ));
    }
    let mut targets = type_tables(catalog, schema, findings);
    targets.sort_by_key(|target| target.is_relation);
    let tables =
        targets.iter().map(|target| TableMapper::new(target, &targets, schema, findings).map()).collect::<Vec<_>>();
    let plan = MappingPlan { tables };
    // Types the errors leave unmapped would only repeat them.
    if !findings.has_errors() {
        warn_about_types_without_source(&plan, schema, findings);
    }
    plan
}

#[derive(Debug, Default)]
pub(crate) struct MappingPlan {
    pub(crate) tables: Vec<TableMapping>,
}

impl MappingPlan {
    pub(crate) fn summary(&self) -> Vec<String> {
        self.tables.iter().map(TableMapping::summary).collect()
    }
}

#[derive(Debug)]
pub(crate) struct TableMapping {
    pub(crate) name: String,
    pub(crate) type_label: String,
    pub(crate) kind: TableKind,
    pub(crate) ownerships: Vec<OwnershipColumn>,
    pub(crate) id_guarantees: DeclaredIdGuarantees,
    pub(crate) has_declared_keys: bool,
}

impl TableMapping {
    pub(crate) fn is_relation(&self) -> bool {
        matches!(self.kind, TableKind::Relation { .. })
    }

    pub(crate) fn roles(&self) -> &[RoleColumn] {
        match &self.kind {
            TableKind::Relation { roles } => roles,
            TableKind::Entity => &[],
        }
    }

    fn summary(&self) -> String {
        let checked = |declared: bool| if declared { "declared" } else { "checked by the loader" };
        let are_ids_declared = self.id_guarantees.is_unique && self.id_guarantees.is_not_null;
        let are_references_declared = self.roles().iter().all(|role| role.is_reference_declared);
        let (roles, references) = match self.roles() {
            [] => (String::new(), String::new()),
            roles => (
                format!(", {} role column(s)", roles.len()),
                format!(", references {}", checked(are_references_declared)),
            ),
        };
        format!(
            "table '{}' -> {} '{}' ({} attribute column(s){roles}; ids {}{references})",
            self.name,
            if self.is_relation() { "relation" } else { "entity" },
            self.type_label,
            self.ownerships.len(),
            checked(are_ids_declared),
        )
    }
}

#[derive(Debug)]
pub(crate) enum TableKind {
    Entity,
    Relation { roles: Vec<RoleColumn> },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeclaredIdGuarantees {
    pub(crate) is_unique: bool,
    pub(crate) is_not_null: bool,
}

#[derive(Debug)]
pub(crate) struct RoleColumn {
    pub(crate) column: String,
    pub(crate) relates: Relates,
    pub(crate) player_table_index: usize,
    pub(crate) is_reference_declared: bool,
}

#[derive(Debug)]
pub(crate) struct OwnershipColumn {
    pub(crate) column: String,
    pub(crate) attribute: String,
    pub(crate) value_type: ValueType,
    pub(crate) is_list: bool,
    pub(crate) element_type: ColumnType,
}

impl OwnershipColumn {
    // The column as an object scan reads it.
    pub(crate) fn scanned(&self) -> String {
        let column = sql::column_of_scanned_table(&self.column);
        match (self.read_type(), self.is_list) {
            (Some(read_type), true) => sql::list_cast(&column, &read_type),
            (Some(read_type), false) => sql::cast(&column, &read_type),
            (None, _) => column,
        }
    }

    // One row per value, a list's elements unnested.
    pub(crate) fn values(&self) -> String {
        let column = sql::quoted(&self.column);
        if self.is_list { sql::unnest(&column) } else { column }
    }

    // Every column of an attribute is read in the attribute's type, and strings are compared byte-wise.
    pub(crate) fn attribute_values(&self) -> String {
        let values = match self.read_type() {
            Some(read_type) => sql::cast(&self.values(), &read_type),
            None => self.values(),
        };
        if self.value_type == ValueType::String { sql::byte_wise(&values) } else { values }
    }

    fn read_type(&self) -> Option<String> {
        match (&self.element_type, self.value_type) {
            (ColumnType::ReadAsVarchar, _) => Some(duckdb_type::VARCHAR.to_owned()),
            // DuckDB compares intervals normalised (1 month = 30 days), TypeDB does not.
            (ColumnType::Interval, _) => Some(duckdb_type::VARCHAR.to_owned()),
            (ColumnType::Integer { .. }, ValueType::Integer) => Some(duckdb_type::BIGINT.to_owned()),
            (_, ValueType::Double) => Some(duckdb_type::DOUBLE.to_owned()),
            (_, ValueType::Decimal) => Some(duckdb_type::decimal(38, DECIMAL_FRACTIONAL_DIGITS)),
            (_, _) => None,
        }
    }
}

const RELATION_IS_A_TABLE: &str = "relations are tables; stage the relation as its own table, \
     then '@typedb skip' this column (see the README's modelling section)";

struct TableTarget<'a> {
    table: &'a Table,
    object: &'a ObjectType,
    is_relation: bool,
    role_bindings: Vec<RoleBinding>,
    id_guarantees: DeclaredIdGuarantees,
}

fn type_tables<'a>(catalog: &'a Catalog, schema: &'a Schema, findings: &mut Findings) -> Vec<TableTarget<'a>> {
    let mut targets = Vec::new();
    for table in &catalog.tables {
        let directive = match parse_table_directive(table) {
            Ok(Some(directive)) => directive,
            Ok(None) => match directive_implied_by_name(table, schema, findings) {
                Some(directive) => directive,
                None => continue,
            },
            Err(message) => {
                findings.error(message);
                continue;
            }
        };
        let (label, is_relation, role_bindings) = match directive {
            TableDirective::Skip => continue,
            TableDirective::Entity { label } => (label, false, Vec::new()),
            TableDirective::Relation { label, role_bindings } => (label, true, role_bindings),
        };
        let Some(object) = schema.object(&label, is_relation) else {
            findings.error(format!(
                "table '{}' is mapped to '{label}', which is not {} in the TypeDB schema",
                table.name,
                if is_relation { "a relation type" } else { "an entity type" }
            ));
            continue;
        };
        if object.is_abstract {
            findings.error(format!(
                "table '{}' is mapped to abstract type '{label}', which cannot have instances",
                table.name
            ));
            continue;
        }
        let Some(id_guarantees) = instance_id_guarantees(table, findings) else { continue };
        targets.push(TableTarget { table, object, is_relation, role_bindings, id_guarantees });
    }
    targets
}

fn directive_implied_by_name(table: &Table, schema: &Schema, findings: &mut Findings) -> Option<TableDirective> {
    let entity = find_by_name(&schema.entities, &table.name, |entity| &entity.label);
    let relation = find_by_name(&schema.relations, &table.name, |relation| &relation.label);
    match (entity, relation) {
        (Some(entity), None) => Some(TableDirective::Entity { label: entity.label.clone() }),
        (None, Some(relation)) => {
            Some(TableDirective::Relation { label: relation.label.clone(), role_bindings: Vec::new() })
        }
        (Some(_), Some(_)) => {
            findings.error(format!(
                "table '{}' matches both an entity and a relation type; disambiguate with \
                 COMMENT ON TABLE ... IS '@typedb entity <label>' or '@typedb relation <label> (...)'",
                table.name
            ));
            None
        }
        (None, None) => {
            findings.error(format!(
                "table '{}' has no matching entity or relation type in the TypeDB schema \
                 (add the type, or mark the table '@typedb skip')",
                table.name
            ));
            None
        }
    }
}

fn instance_id_guarantees(table: &Table, findings: &mut Findings) -> Option<DeclaredIdGuarantees> {
    let contract = format!(
        "add `{IID_COLUMN} UBIGINT`, unique and not null, holding the row's instance id (unsigned 64-bit; see the \
         README's modelling section)"
    );
    let Some(column) = table.column(IID_COLUMN) else {
        findings.error(format!("table '{}' has no {IID_COLUMN} column: {contract}", table.name));
        return None;
    };
    if table.foreign_key_of(IID_COLUMN).is_some() {
        findings.error(format!(
            "table '{}': {IID_COLUMN} is a FOREIGN KEY, which cannot stand for an instance of another table; \
             an instance belongs to one table (see the README's modelling section)",
            table.name
        ));
        return None;
    }
    if column.column_type != ColumnType::INSTANCE_ID {
        findings.error(format!(
            "table '{}': {IID_COLUMN} must be UBIGINT (an instance id is unsigned 64-bit), found {}: {contract}",
            table.name, column.duckdb_type
        ));
        return None;
    }
    let is_primary_key = table.primary_key.as_slice() == [IID_COLUMN];
    Some(DeclaredIdGuarantees {
        is_unique: is_primary_key || table.unique_column_sets.iter().any(|set| set.as_slice() == [IID_COLUMN]),
        is_not_null: is_primary_key || table.not_null_columns.contains(IID_COLUMN),
    })
}

#[derive(Clone, Copy)]
enum PlayerTableReference<'a> {
    ForeignKey(&'a ForeignKey),
    Directive(&'a str),
}

impl PlayerTableReference<'_> {
    fn table(&self) -> &str {
        match self {
            PlayerTableReference::ForeignKey(foreign_key) => &foreign_key.referenced_table,
            PlayerTableReference::Directive(table) => table,
        }
    }
}

struct TableMapper<'a, 'f> {
    target: &'a TableTarget<'a>,
    targets: &'a [TableTarget<'a>],
    schema: &'a Schema,
    findings: &'f mut Findings,
    ownerships: Vec<OwnershipColumn>,
    roles: Vec<RoleColumn>,
    skipped_columns: Vec<&'a str>,
}

impl<'a, 'f> TableMapper<'a, 'f> {
    fn new(
        target: &'a TableTarget<'a>,
        targets: &'a [TableTarget<'a>],
        schema: &'a Schema,
        findings: &'f mut Findings,
    ) -> Self {
        TableMapper {
            target,
            targets,
            schema,
            findings,
            ownerships: Vec::new(),
            roles: Vec::new(),
            skipped_columns: Vec::new(),
        }
    }

    fn map(mut self) -> TableMapping {
        let target = self.target;
        for column in &target.table.columns {
            self.map_column(column);
        }
        self.check_role_bindings();
        if target.is_relation {
            self.check_roles_filled();
        }
        let TableTarget { table, object, is_relation, id_guarantees, .. } = target;
        let kind = if *is_relation { TableKind::Relation { roles: self.roles } } else { TableKind::Entity };
        TableMapping {
            name: table.name.clone(),
            type_label: object.label.clone(),
            kind,
            ownerships: self.ownerships,
            id_guarantees: *id_guarantees,
            has_declared_keys: table.has_declared_keys(),
        }
    }

    fn map_column(&mut self, column: &'a Column) {
        let target = self.target;
        let table = &target.table.name;
        if column.name == IID_COLUMN {
            if column.comment.as_deref().and_then(strip_directive_prefix).is_some() {
                self.findings
                    .error(format!("table '{table}': {IID_COLUMN} holds the instances' ids and takes no directive"));
            }
            return;
        }
        let directive = match parse_column_directive(target.table, column) {
            Ok(directive) => directive,
            Err(message) => return self.findings.error(message),
        };
        if directive == Some(ColumnDirective::Skip) {
            self.skipped_columns.push(&column.name);
            return;
        }
        let reference = match player_table_reference(target.table, column, &directive) {
            Ok(reference) => reference,
            Err(message) => return self.findings.error(message),
        };
        let binding = target.role_bindings.iter().find(|binding| binding.column == column.name);
        match (&directive, binding, reference) {
            (Some(ColumnDirective::Owns(_)), Some(_), _) | (Some(ColumnDirective::Owns(_)), _, Some(_)) => {
                self.findings.error(format!(
                    "table '{table}': column '{}' holds role players (a FOREIGN KEY or a bound column), so it cannot \
                     own an attribute",
                    column.name
                ))
            }
            (_, Some(binding), reference) => self.bind_role(column, &binding.role, reference),
            (_, None, Some(reference)) if target.is_relation => self.infer_role(column, reference),
            (_, None, Some(reference)) => self.findings.error(format!(
                "table '{table}': column '{}' references table '{}', which on an entity table has no TypeDB form: \
                 {RELATION_IS_A_TABLE}",
                column.name,
                reference.table()
            )),
            (Some(ColumnDirective::Owns(attribute)), None, None) => self.bind_ownership(column, Some(attribute)),
            (_, None, None) => self.bind_ownership(column, None),
        }
    }

    fn bind_role(&mut self, column: &Column, role: &str, reference: Option<PlayerTableReference<'_>>) {
        let target = self.target;
        let (table, relation) = (&target.table.name, &target.object.label);
        let Some(relates) = target.object.relates.iter().find(|relates| relates.role == role) else {
            return self.findings.error(format!(
                "table '{table}': column '{}' is bound to role '{role}', but relation '{relation}' has no such role",
                column.name
            ));
        };
        let Some(reference) = reference else {
            return self.findings.error(format!(
                "table '{table}': column '{}' is bound to role '{role}' but names no players' table: declare it a \
                 FOREIGN KEY to that table's {IID_COLUMN}, or mark it '@typedb references <table>'",
                column.name
            ));
        };
        let Some(player_table_index) = self.resolve_player_table(column, reference) else { return };
        let player = &self.targets[player_table_index];
        if !self.roles_played_in_relation(player.object).iter().any(|played| played.role == role) {
            return self.findings.error(format!(
                "table '{table}': column '{}' is bound to role '{relation}:{role}', but its players' type '{}' \
                 (table '{}') does not play that role",
                column.name, player.object.label, player.table.name
            ));
        }
        self.roles.push(role_column(relates, column, player_table_index, reference));
    }

    // Inferred only when the players play one role: a role bound elsewhere may take several columns.
    fn infer_role(&mut self, column: &Column, reference: PlayerTableReference<'_>) {
        let Some(player_table_index) = self.resolve_player_table(column, reference) else { return };
        let target = self.target;
        let player = &self.targets[player_table_index];
        let played_roles = self.roles_played_in_relation(player.object);
        let (table, relation) = (&target.table.name, &target.object.label);
        match played_roles.as_slice() {
            [relates] => self.roles.push(role_column(relates, column, player_table_index, reference)),
            [] => self.findings.error(format!(
                "table '{table}': column '{}' references '{}', whose type '{}' plays no role of relation '{relation}'",
                column.name, player.table.name, player.object.label
            )),
            _ => self.findings.error(format!(
                "table '{table}': column '{}' is ambiguous: '{}' plays {} roles of '{relation}'; bind the columns with \
                 COMMENT ON TABLE {} IS '@typedb relation {relation} ({})'",
                column.name,
                player.object.label,
                played_roles.len(),
                sql::quoted(table),
                played_roles
                    .iter()
                    .map(|relates| format!("{} = <column>", relates.role))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }

    // The relation's roles that the player's type plays, also through the relation's supertypes.
    fn roles_played_in_relation(&self, player: &ObjectType) -> Vec<&'a Relates> {
        let relations = self.schema.relation_and_supertypes(&self.target.object.label);
        self.target
            .object
            .relates
            .iter()
            .filter(|relates| {
                player.plays.iter().any(|plays| plays.role == relates.role && relations.contains(&plays.relation))
            })
            .collect()
    }

    fn resolve_player_table(&mut self, column: &Column, reference: PlayerTableReference<'_>) -> Option<usize> {
        let target = self.target;
        let table = &target.table.name;
        let foreign_key = match reference {
            PlayerTableReference::ForeignKey(foreign_key) => foreign_key,
            PlayerTableReference::Directive(named) => {
                if column.column_type != ColumnType::INSTANCE_ID {
                    self.findings.error(format!(
                        "table '{table}': column '{}' references '{named}' and must hold its {IID_COLUMN} values, so \
                         it must be UBIGINT; found {}",
                        column.name, column.duckdb_type
                    ));
                    return None;
                }
                let player = self.target_index(named);
                if player.is_none() {
                    self.findings.error(format!(
                        "table '{table}': column '{}' references table '{named}', which is not mapped (is it skipped, \
                         invalid or misspelt?)",
                        column.name
                    ));
                }
                return player;
            }
        };
        if foreign_key.columns.len() > 1 {
            self.findings.error(format!(
                "table '{table}': column '{}' is part of the composite FOREIGN KEY {:?}, which cannot bind a role: \
                 reference the players' table by its {IID_COLUMN} column instead",
                column.name, foreign_key.columns
            ));
            return None;
        }
        let foreign_keys = target.table.foreign_keys.iter().filter(|other| other.columns.contains(&column.name));
        if foreign_keys.count() > 1 {
            self.findings.error(format!(
                "table '{table}': column '{}' is in several FOREIGN KEYs, so its players' table is ambiguous",
                column.name
            ));
            return None;
        }
        let Some(player) = self.target_index(&foreign_key.referenced_table) else {
            self.findings.error(format!(
                "table '{table}': FK column '{}' references table '{}', which is not mapped (is it skipped or \
                 invalid?)",
                column.name, foreign_key.referenced_table
            ));
            return None;
        };
        let referenced_columns = foreign_key.referenced_columns.as_slice();
        if !matches!(referenced_columns, [referenced] if referenced.eq_ignore_ascii_case(IID_COLUMN)) {
            self.findings.error(format!(
                "table '{table}': FK column '{}' must reference '{}.{IID_COLUMN}' (the instance-id column); found \
                 {referenced_columns:?}",
                column.name, foreign_key.referenced_table
            ));
            return None;
        }
        Some(player)
    }

    // DuckDB resolves identifiers case-insensitively but records references as written.
    fn target_index(&self, table: &str) -> Option<usize> {
        self.targets.iter().position(|target| target.table.name.eq_ignore_ascii_case(table))
    }

    fn bind_ownership(&mut self, column: &Column, named_attribute: Option<&str>) {
        let (target, schema) = (self.target, self.schema);
        let (table, owner) = (&target.table.name, &target.object.label);
        let attribute_label = match named_attribute {
            Some(label) => label.to_owned(),
            None => match find_by_name(&schema.attributes, &column.name, |attribute| &attribute.label) {
                Some(attribute) => attribute.label.clone(),
                None => {
                    return self.findings.error(format!(
                        "table '{table}': column '{}' has no matching attribute in the TypeDB schema (add the \
                         attribute, or mark the column '@typedb skip' / '@typedb owns <label>')",
                        column.name
                    ));
                }
            },
        };
        let Some(attribute) = schema.attribute(&attribute_label) else {
            return self.findings.error(format!(
                "table '{table}': column '{}' maps to attribute '{attribute_label}', which does not exist in the \
                 schema",
                column.name
            ));
        };
        if !target.object.owns.contains(&attribute_label) {
            return self.findings.error(format!(
                "table '{table}': column '{}' maps to attribute '{attribute_label}', but type '{owner}' does not \
                 own it",
                column.name
            ));
        }
        if attribute.is_abstract {
            return self.findings.error(format!(
                "table '{table}': column '{}' maps to attribute '{attribute_label}', which is abstract and cannot \
                 have instances; declare a concrete subtype, let '{owner}' own it, and map the column to it with \
                 '@typedb owns <label>'",
                column.name
            ));
        }
        let Some(value_type) = attribute.value_type else {
            return self.findings.error(format!(
                "table '{table}': column '{}' maps to attribute '{attribute_label}', which has no value type in the \
                 schema",
                column.name
            ));
        };
        let (element_type, is_list) = match &column.column_type {
            ColumnType::List(element) => (element.as_ref(), true),
            other => (other, false),
        };
        if let Err(problem) = element_type_problem(column, element_type, value_type, &attribute_label) {
            return self.findings.error(format!("table '{table}': column '{}' {problem}", column.name));
        }
        self.ownerships.push(OwnershipColumn {
            column: column.name.clone(),
            attribute: attribute_label,
            value_type,
            is_list,
            element_type: element_type.clone(),
        });
    }

    fn check_role_bindings(&mut self) {
        let target = self.target;
        let bindings = &target.role_bindings;
        for (index, RoleBinding { role, column }) in bindings.iter().enumerate() {
            let problem = if target.table.column(column).is_none() {
                "which the table does not have"
            } else if column == IID_COLUMN {
                "which holds the relations' own ids"
            } else if bindings[..index].iter().any(|earlier| earlier.column == *column) {
                "which the directive already binds; a column holds one role's players"
            } else if self.skipped_columns.contains(&column.as_str()) {
                "which is marked '@typedb skip'"
            } else {
                continue;
            };
            self.findings.error(format!(
                "table '{}': the relation directive binds role '{role}' to column '{column}', {problem}",
                target.table.name
            ));
        }
    }

    fn check_roles_filled(&mut self) {
        let target = self.target;
        let (table, relation) = (&target.table.name, target.object);
        if self.roles.is_empty() {
            return self.findings.error(format!(
                "relation table '{table}' fills no role, so every row would be a relation without players"
            ));
        }
        for relates in &relation.relates {
            if !self.roles.iter().any(|role| role.relates.role == relates.role) {
                self.findings.warning(format!(
                    "relation table '{table}' never fills role '{}:{}'",
                    relation.label, relates.role
                ));
            }
        }
    }
}

fn player_table_reference<'a>(
    table: &'a Table,
    column: &Column,
    directive: &'a Option<ColumnDirective>,
) -> Result<Option<PlayerTableReference<'a>>, String> {
    match (table.foreign_key_of(&column.name), directive) {
        (Some(foreign_key), Some(ColumnDirective::References(named)))
            if !foreign_key.referenced_table.eq_ignore_ascii_case(named) =>
        {
            Err(format!(
                "table '{}': column '{}' is marked '@typedb references {named}', but its FOREIGN KEY references '{}'",
                table.name, column.name, foreign_key.referenced_table
            ))
        }
        (Some(foreign_key), _) => Ok(Some(PlayerTableReference::ForeignKey(foreign_key))),
        (None, Some(ColumnDirective::References(named))) => Ok(Some(PlayerTableReference::Directive(named))),
        (None, _) => Ok(None),
    }
}

fn role_column(
    relates: &Relates,
    column: &Column,
    player_table_index: usize,
    reference: PlayerTableReference<'_>,
) -> RoleColumn {
    RoleColumn {
        column: column.name.clone(),
        relates: relates.clone(),
        player_table_index,
        is_reference_declared: matches!(reference, PlayerTableReference::ForeignKey(_)),
    }
}

// What follows "table 't': column 'c'" in the error, if the column cannot populate the attribute.
fn element_type_problem(
    column: &Column,
    element_type: &ColumnType,
    value_type: ValueType,
    attribute: &str,
) -> Result<(), String> {
    let duckdb_type = &column.duckdb_type;
    match element_type {
        ColumnType::Unsupported { hint } => Err(format!("has unsupported type {duckdb_type}: {hint}")),
        ColumnType::List(_) => Err("is a nested list, which has no TypeDB mapping".to_owned()),
        ColumnType::Decimal { scale, .. } if *scale > DECIMAL_FRACTIONAL_DIGITS => Err(format!(
            "has DECIMAL scale {scale}, beyond TypeDB's {DECIMAL_FRACTIONAL_DIGITS} fractional digits; DuckDB would \
             round the values when reading them"
        )),
        _ if !can_populate(element_type, value_type) => {
            let reason = match element_type {
                ColumnType::Varchar | ColumnType::ReadAsVarchar => "the loader never parses text into values",
                _ => "the types are incompatible",
            };
            Err(format!(
                "({duckdb_type}) cannot populate attribute '{attribute}' (value type '{}'): {reason}; stage the \
                 column as {}",
                value_type.name(),
                column_type_holding(value_type)
            ))
        }
        _ => Ok(()),
    }
}

fn can_populate(column_type: &ColumnType, value_type: ValueType) -> bool {
    use ValueType as V;
    match column_type {
        ColumnType::Boolean => value_type == V::Boolean,
        ColumnType::Integer { .. } => matches!(value_type, V::Integer | V::Double | V::Decimal),
        ColumnType::Double => value_type == V::Double,
        ColumnType::Decimal { .. } => matches!(value_type, V::Decimal | V::Double),
        ColumnType::Varchar | ColumnType::ReadAsVarchar => value_type == V::String,
        ColumnType::Date => value_type == V::Date,
        ColumnType::Timestamp { .. } => value_type == V::Datetime,
        ColumnType::TimestampTz { .. } => value_type == V::DatetimeTz,
        ColumnType::Interval => value_type == V::Duration,
        ColumnType::List(_) | ColumnType::Unsupported { .. } => false,
    }
}

fn column_type_holding(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Boolean => duckdb_type::BOOLEAN,
        ValueType::Integer => duckdb_type::BIGINT,
        ValueType::Double => duckdb_type::DOUBLE,
        ValueType::Decimal => duckdb_type::DECIMAL,
        ValueType::String => duckdb_type::VARCHAR,
        ValueType::Date => duckdb_type::DATE,
        ValueType::Datetime => duckdb_type::TIMESTAMP,
        ValueType::DatetimeTz => duckdb_type::TIMESTAMPTZ,
        ValueType::Duration => duckdb_type::INTERVAL,
    }
}

fn warn_about_types_without_source(plan: &MappingPlan, schema: &Schema, findings: &mut Findings) {
    for (types, kind) in [(&schema.entities, "entity"), (&schema.relations, "relation")] {
        for object in types.iter().filter(|object| !object.is_abstract) {
            if !plan.tables.iter().any(|table| table.type_label == object.label) {
                findings.warning(format!(
                    "{kind} type '{}' has no DuckDB source; no instances will be produced",
                    object.label
                ));
            }
        }
    }
    for attribute in schema.attributes.iter().filter(|attribute| !attribute.is_abstract) {
        let is_sourced = plan
            .tables
            .iter()
            .any(|table| table.ownerships.iter().any(|ownership| ownership.attribute == attribute.label));
        if !is_sourced {
            findings.warning(format!(
                "attribute type '{}' has no DuckDB source; no instances will be produced",
                attribute.label
            ));
        }
    }
}

fn find_by_name<'a, T>(types: &'a [T], name: &str, label: impl Fn(&T) -> &str) -> Option<&'a T> {
    types.iter().find(|t| label(t) == name).or_else(|| {
        let dashed = name.replace('_', "-");
        types.iter().find(|t| label(t) == dashed)
    })
}

enum TableDirective {
    Skip,
    Entity { label: String },
    Relation { label: String, role_bindings: Vec<RoleBinding> },
}

struct RoleBinding {
    role: String,
    column: String,
}

#[derive(PartialEq)]
enum ColumnDirective {
    Skip,
    Owns(String),
    References(String),
}

const DIRECTIVE_PREFIX: &str = "@typedb";

fn parse_table_directive(table: &Table) -> Result<Option<TableDirective>, String> {
    let Some((keyword, argument)) = parse_directive(table.comment.as_deref()) else { return Ok(None) };
    match (keyword.as_str(), argument) {
        ("skip", "") => Ok(Some(TableDirective::Skip)),
        ("entity", label) if !label.is_empty() => Ok(Some(TableDirective::Entity { label: label.to_owned() })),
        ("relation", spec) if !spec.is_empty() => parse_relation_directive(spec)
            .map(Some)
            .map_err(|err| format!("table '{}': malformed @typedb relation directive: {err}", table.name)),
        _ => Err(format!(
            "table '{}': unrecognised directive '{}' \
             (expected: skip | entity <label> | relation <label> (<role> = <column>, ...))",
            table.name,
            table.comment.as_deref().unwrap_or_default().trim()
        )),
    }
}

fn parse_column_directive(table: &Table, column: &Column) -> Result<Option<ColumnDirective>, String> {
    let Some((keyword, argument)) = parse_directive(column.comment.as_deref()) else { return Ok(None) };
    match (keyword.as_str(), argument) {
        ("skip", "") => Ok(Some(ColumnDirective::Skip)),
        ("owns", label) if !label.is_empty() => Ok(Some(ColumnDirective::Owns(label.to_owned()))),
        ("references", table) if !table.is_empty() => Ok(Some(ColumnDirective::References(table.to_owned()))),
        ("relation", _) => {
            Err(format!("table '{}': column '{}' cannot be a relation: {RELATION_IS_A_TABLE}", table.name, column.name))
        }
        _ => Err(format!(
            "table '{}': column '{}': unrecognised directive '{}' (expected: skip | owns <label> | references <table>)",
            table.name,
            column.name,
            column.comment.as_deref().unwrap_or_default().trim()
        )),
    }
}

// `@typedb <keyword> [<argument>]`, the keyword in any case.
fn parse_directive(comment: Option<&str>) -> Option<(String, &str)> {
    let rest = comment.and_then(strip_directive_prefix)?;
    let (keyword, argument) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
    Some((keyword.to_ascii_lowercase(), argument.trim()))
}

fn strip_directive_prefix(comment: &str) -> Option<&str> {
    let comment = comment.trim();
    let prefix = comment.get(..DIRECTIVE_PREFIX.len())?;
    prefix.eq_ignore_ascii_case(DIRECTIVE_PREFIX).then(|| comment[DIRECTIVE_PREFIX.len()..].trim())
}

fn parse_relation_directive(spec: &str) -> Result<TableDirective, String> {
    let spec = spec.trim();
    let Some(open) = spec.find('(') else {
        return Ok(TableDirective::Relation { label: spec.to_owned(), role_bindings: Vec::new() });
    };
    let label = spec[..open].trim().to_owned();
    let inner = spec[open + 1..].trim().strip_suffix(')').ok_or("missing closing ')'")?;
    let mut role_bindings = Vec::new();
    for part in inner.split(',') {
        let (role, column) = part.split_once('=').ok_or_else(|| format!("expected '<role> = <column>' in '{part}'"))?;
        role_bindings.push(RoleBinding { role: role.trim().to_owned(), column: column.trim().to_owned() });
    }
    Ok(TableDirective::Relation { label, role_bindings })
}
