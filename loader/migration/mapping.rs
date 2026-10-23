/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    Diagnostics,
    catalog::{Catalog, Column, ColumnType, ForeignKey, Table},
    schema::{ObjectType, Relates, Schema, ValueType},
    value::DECIMAL_FRACTIONAL_DIGITS,
};

pub(crate) const IID_COLUMN: &str = "_iid";

pub(crate) fn quoted(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// A column populating an attribute type. The SQL expressions below read its values in
/// the attribute's value type, the same way for attributes and ownerships, so the two agree
/// on every id.
#[derive(Debug)]
pub(crate) struct OwnsMapping {
    pub(crate) column: String,
    pub(crate) attribute: String,
    pub(crate) value_type: ValueType,
    /// A LIST column: one ownership per element.
    pub(crate) multi: bool,
    /// The column's type, or the element type of a LIST column.
    pub(crate) column_type: ColumnType,
}

impl OwnsMapping {
    /// A cell or list element as read and deduplicated. Strings compare byte-wise, as in
    /// TypeDB, whatever the column's collation.
    pub(crate) fn read(&self, expr: &str) -> String {
        let value = match self.cast() {
            Some(sql_type) => format!("{expr}::{sql_type}"),
            None => expr.to_owned(),
        };
        if self.value_type == ValueType::String { format!("{value} COLLATE \"binary\"") } else { value }
    }

    /// The column as read with its owner's row; a list column stays whole.
    pub(crate) fn projection(&self) -> String {
        let column = quoted(&self.column);
        match (self.multi, self.cast()) {
            (true, Some(sql_type)) => format!("{column}::{sql_type}[]"),
            (true, None) => column,
            (false, _) => self.read(&column),
        }
    }

    /// The SQL type the column is read in, unless read as is. Numeric columns of one
    /// attribute are all read in one type, chosen to hold every value: SQL's own
    /// unification of a UNION could round (DECIMAL(38,0) with DECIMAL(10,9) gives
    /// DECIMAL(38,0)), and DECIMAL(38,19) holds every TypeDB decimal exactly. Intervals are
    /// read as text: DuckDB compares them normalised (1 month = 30 days), while TypeDB keeps
    /// months, days and nanoseconds apart, as the text does, and the binary form's 64-bit
    /// nanoseconds wrap for long spans.
    fn cast(&self) -> Option<&'static str> {
        match (&self.column_type, self.value_type) {
            (ColumnType::StringLike | ColumnType::Interval, _) => Some("VARCHAR"),
            (ColumnType::Double, ValueType::Double) => None,
            (_, ValueType::Double) => Some("DOUBLE"),
            (_, ValueType::Decimal) => Some("DECIMAL(38,19)"),
            _ => None,
        }
    }
}

/// An FK column of a relation table, holding the players of one role.
#[derive(Debug)]
pub(crate) struct RoleBinding {
    pub(crate) column: String,
    pub(crate) role: String,
    /// The role label as the import format wants it: scoped to the declaring relation.
    pub(crate) scoped_role: String,
    /// The table the FK references, and its type.
    pub(crate) player_table: String,
    pub(crate) player_label: String,
}

#[derive(Debug)]
pub(crate) struct TableMapping {
    pub(crate) name: String,
    pub(crate) type_label: String,
    pub(crate) is_relation: bool,
    pub(crate) owns: Vec<OwnsMapping>,
    pub(crate) roles: Vec<RoleBinding>,
}

#[derive(Debug, Default)]
pub(crate) struct MappingPlan {
    /// In import order: entity tables, then relation tables after their players' tables.
    pub(crate) tables: Vec<TableMapping>,
}

impl MappingPlan {
    pub(crate) fn summary(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| {
                format!(
                    "table '{}' -> {} '{}' ({} attribute column(s){})",
                    table.name,
                    if table.is_relation { "relation" } else { "entity" },
                    table.type_label,
                    table.owns.len(),
                    if table.roles.is_empty() {
                        String::new()
                    } else {
                        format!(", {} role column(s)", table.roles.len())
                    },
                )
            })
            .collect()
    }
}

enum TableDirective {
    Skip,
    Entity(String),
    /// The label and the `role = column` bindings.
    Relation(String, Vec<(String, String)>),
}

#[derive(PartialEq)]
enum ColumnDirective {
    Skip,
    Owns(String),
}

const DIRECTIVE_PREFIX: &str = "@typedb";

const RELATION_IS_A_TABLE: &str = "relations are tables; stage the relation as its own table, \
     then '@typedb skip' this column (see the README's modelling section)";

/// The prefix is matched case-insensitively, so that `@TypeDB` is not taken for a plain comment.
fn strip_directive_prefix(comment: &str) -> Option<&str> {
    let comment = comment.trim();
    let prefix = comment.get(..DIRECTIVE_PREFIX.len())?;
    prefix.eq_ignore_ascii_case(DIRECTIVE_PREFIX).then(|| comment[DIRECTIVE_PREFIX.len()..].trim())
}

fn parse_table_directive(table: &Table) -> Result<Option<TableDirective>, String> {
    let Some(rest) = table.comment.as_deref().and_then(strip_directive_prefix) else { return Ok(None) };
    if rest == "skip" {
        Ok(Some(TableDirective::Skip))
    } else if let Some(label) = rest.strip_prefix("entity ") {
        Ok(Some(TableDirective::Entity(label.trim().to_owned())))
    } else if let Some(spec) = rest.strip_prefix("relation ") {
        let (label, bindings) = parse_bindings(spec)
            .map_err(|err| format!("table '{}': malformed @typedb relation directive: {err}", table.name))?;
        Ok(Some(TableDirective::Relation(label, bindings)))
    } else {
        Err(format!(
            "table '{}': unrecognised directive '@typedb {rest}' \
             (expected: skip | entity <label> | relation <label> (<role> = <column>, ...))",
            table.name
        ))
    }
}

fn parse_column_directive(table: &Table, column: &Column) -> Result<Option<ColumnDirective>, String> {
    let Some(rest) = column.comment.as_deref().and_then(strip_directive_prefix) else { return Ok(None) };
    if rest == "skip" {
        Ok(Some(ColumnDirective::Skip))
    } else if let Some(label) = rest.strip_prefix("owns ") {
        Ok(Some(ColumnDirective::Owns(label.trim().to_owned())))
    } else if rest.starts_with("relation ") {
        Err(format!("table '{}': column '{}' cannot be a relation: {RELATION_IS_A_TABLE}", table.name, column.name))
    } else {
        Err(format!(
            "table '{}': column '{}': unrecognised directive '@typedb {rest}' (expected: skip | owns <label>)",
            table.name, column.name
        ))
    }
}

/// Parses `<label> (<key> = <value>, ...)`; the binding list is optional.
fn parse_bindings(spec: &str) -> Result<(String, Vec<(String, String)>), String> {
    let spec = spec.trim();
    let Some(open) = spec.find('(') else { return Ok((spec.to_owned(), Vec::new())) };
    let label = spec[..open].trim().to_owned();
    let inner = spec[open + 1..].trim().strip_suffix(')').ok_or("missing closing ')'")?;
    let mut bindings = Vec::new();
    for part in inner.split(',') {
        let (key, value) = part.split_once('=').ok_or_else(|| format!("expected '<key> = <value>' in '{part}'"))?;
        bindings.push((key.trim().to_owned(), value.trim().to_owned()));
    }
    Ok((label, bindings))
}

/// The type named exactly, else the one named with `_` read as `-`.
fn find_by_name<'a, T>(types: &'a [T], name: &str, label: impl Fn(&T) -> &str) -> Option<&'a T> {
    types.iter().find(|t| label(t) == name).or_else(|| {
        let dashed = name.replace('_', "-");
        types.iter().find(|t| label(t) == dashed)
    })
}

/// A table with its TypeDB type decided; its columns are mapped afterwards, since role
/// columns refer to other tables' decisions.
struct ResolvedTable<'a> {
    table: &'a Table,
    object: &'a ObjectType,
    is_relation: bool,
    /// `role = column` from the relation directive.
    bound_columns: Vec<(String, String)>,
}

pub(crate) fn resolve(catalog: &Catalog, schema: &Schema, diagnostics: &mut Diagnostics) -> MappingPlan {
    for ignored in &catalog.ignored {
        diagnostics.warning(format!(
            "{ignored} is not loaded: only tables of the 'main' schema are read (materialise a view as a table)"
        ));
    }
    let targets = resolve_tables(catalog, schema, diagnostics);
    let mut plan = MappingPlan::default();

    for target in &targets {
        let mut mapping = TableMapping {
            name: target.table.name.clone(),
            type_label: target.object.label.clone(),
            is_relation: target.is_relation,
            owns: Vec::new(),
            roles: Vec::new(),
        };
        let mut skipped = Vec::new();
        for column in &target.table.columns {
            if column.name == IID_COLUMN {
                if column.comment.as_deref().and_then(strip_directive_prefix).is_some() {
                    diagnostics.error(format!(
                        "table '{}': {IID_COLUMN} holds the instances' ids and takes no directive",
                        target.table.name
                    ));
                }
                continue;
            }
            let directive = match parse_column_directive(target.table, column) {
                Ok(directive) => directive,
                Err(message) => {
                    diagnostics.error(message);
                    continue;
                }
            };
            if directive == Some(ColumnDirective::Skip) {
                skipped.push(column.name.as_str());
                continue;
            }
            let explicit_role = target.bound_columns.iter().find(|(_, bound)| *bound == column.name);
            let holds_players = explicit_role.is_some() || target.table.fk_of_column(&column.name).is_some();
            if matches!(directive, Some(ColumnDirective::Owns(_))) && holds_players {
                diagnostics.error(format!(
                    "table '{}': column '{}' holds role players (a FOREIGN KEY or a bound column), so it cannot own \
                     an attribute",
                    target.table.name, column.name
                ));
            } else if let Some((role, _)) = explicit_role {
                bind_role(&mut mapping, target, column, role, &targets, schema, diagnostics);
            } else if let Some(fk) = target.table.fk_of_column(&column.name) {
                if target.is_relation {
                    infer_role(&mut mapping, target, column, fk, &targets, schema, diagnostics);
                } else {
                    diagnostics.error(format!(
                        "table '{}': FK column '{}' on an entity table has no TypeDB form: {RELATION_IS_A_TABLE}",
                        target.table.name, column.name
                    ));
                }
            } else {
                let explicit_attribute = match &directive {
                    Some(ColumnDirective::Owns(label)) => Some(label.as_str()),
                    _ => None,
                };
                bind_owns(&mut mapping, target, column, explicit_attribute, schema, diagnostics);
            }
        }
        check_role_binding_columns(target, &skipped, diagnostics);

        for relates in &target.object.relates {
            if !mapping.roles.iter().any(|r| r.role == relates.role) {
                diagnostics.warning(format!(
                    "relation table '{}' never fills role '{}:{}'",
                    target.table.name, target.object.label, relates.role
                ));
            }
        }
        plan.tables.push(mapping);
    }

    unmapped_schema_warnings(&plan, schema, diagnostics);
    order_for_import(&mut plan);
    plan
}

/// A directive binding a misspelt, repeated or skipped column would otherwise silently
/// leave the role to inference.
fn check_role_binding_columns(target: &ResolvedTable<'_>, skipped: &[&str], diagnostics: &mut Diagnostics) {
    let bindings = &target.bound_columns;
    for (index, (role, column)) in bindings.iter().enumerate() {
        let problem = if target.table.column(column).is_none() {
            "which the table does not have"
        } else if column == IID_COLUMN {
            "which holds the relations' own ids"
        } else if bindings[..index].iter().any(|(_, earlier)| earlier == column) {
            "which the directive already binds; a column holds one role's players"
        } else if skipped.contains(&column.as_str()) {
            "which is marked '@typedb skip'"
        } else {
            continue;
        };
        diagnostics.error(format!(
            "table '{}': the relation directive binds role '{role}' to column '{column}', {problem}",
            target.table.name
        ));
    }
}

/// Entity tables first, then relation tables after their players' tables, so that a
/// relation's players are imported before it. DuckDB cannot declare foreign keys that
/// reference each other across tables, so only self-references remain unordered.
fn order_for_import(plan: &mut MappingPlan) {
    let (mut ordered, mut pending): (Vec<TableMapping>, Vec<TableMapping>) =
        std::mem::take(&mut plan.tables).into_iter().partition(|table| !table.is_relation);
    pending.sort_by(|a, b| a.name.cmp(&b.name));
    while !pending.is_empty() {
        let placed = |name: &str| ordered.iter().any(|table| table.name == name);
        let next = pending
            .iter()
            .position(|table| {
                table.roles.iter().all(|role| role.player_table == table.name || placed(&role.player_table))
            })
            .expect("foreign keys cannot form cycles across DuckDB tables");
        ordered.push(pending.remove(next));
    }
    plan.tables = ordered;
}

fn resolve_tables<'a>(
    catalog: &'a Catalog,
    schema: &'a Schema,
    diagnostics: &mut Diagnostics,
) -> Vec<ResolvedTable<'a>> {
    let mut targets = Vec::new();
    for table in &catalog.tables {
        let directive = match parse_table_directive(table) {
            Ok(directive) => directive,
            Err(message) => {
                diagnostics.error(message);
                continue;
            }
        };
        let (label, is_relation, bound_columns) = match directive {
            Some(TableDirective::Skip) => continue,
            Some(TableDirective::Entity(label)) => (label, false, Vec::new()),
            Some(TableDirective::Relation(label, bindings)) => (label, true, bindings),
            None => {
                let entity = find_by_name(&schema.entities, &table.name, |e| &e.label);
                let relation = find_by_name(&schema.relations, &table.name, |r| &r.label);
                match (entity, relation) {
                    (Some(entity), None) => (entity.label.clone(), false, Vec::new()),
                    (None, Some(relation)) => (relation.label.clone(), true, Vec::new()),
                    (Some(_), Some(_)) => {
                        diagnostics.error(format!(
                            "table '{}' matches both an entity and a relation type; disambiguate with \
                             COMMENT ON TABLE ... IS '@typedb entity <label>' or '@typedb relation <label> (...)'",
                            table.name
                        ));
                        continue;
                    }
                    (None, None) => {
                        diagnostics.error(format!(
                            "table '{}' has no matching entity or relation type in the TypeDB schema \
                             (add the type, or mark the table '@typedb skip')",
                            table.name
                        ));
                        continue;
                    }
                }
            }
        };

        let Some(object) = schema.object(&label, is_relation) else {
            diagnostics.error(format!(
                "table '{}' is mapped to '{label}', which is not {} in the TypeDB schema",
                table.name,
                if is_relation { "a relation type" } else { "an entity type" }
            ));
            continue;
        };
        if object.abstract_ {
            diagnostics.error(format!(
                "table '{}' is mapped to abstract type '{label}', which cannot have instances",
                table.name
            ));
            continue;
        }
        if !has_instance_id_column(table, diagnostics) {
            continue;
        }
        targets.push(ResolvedTable { table, object, is_relation, bound_columns });
    }
    targets
}

/// `_iid UBIGINT NOT NULL UNIQUE` (or PRIMARY KEY) identifies the instances within their
/// table; DuckDB enforces the constraints, so the values need no check.
fn has_instance_id_column(table: &Table, diagnostics: &mut Diagnostics) -> bool {
    let contract = format!(
        "declare `{IID_COLUMN} UBIGINT NOT NULL` with a UNIQUE or PRIMARY KEY constraint, holding \
         the row's instance id (unsigned 64-bit; see the README's modelling section)"
    );
    let Some(column) = table.column(IID_COLUMN) else {
        diagnostics.error(format!("table '{}' has no {IID_COLUMN} column: {contract}", table.name));
        return false;
    };
    if table.fk_of_column(IID_COLUMN).is_some() {
        diagnostics.error(format!(
            "table '{}': {IID_COLUMN} is a FOREIGN KEY, which cannot stand for an instance of another table; \
             an instance belongs to one table (see the README's modelling section)",
            table.name
        ));
        return false;
    }
    if column.raw_type != "UBIGINT" {
        diagnostics.error(format!(
            "table '{}': {IID_COLUMN} must be UBIGINT (an instance id is unsigned 64-bit), found {}: {contract}",
            table.name, column.raw_type
        ));
        return false;
    }
    let is_pk = table.primary_key.as_slice() == [IID_COLUMN];
    if !is_pk && !table.not_null.contains(IID_COLUMN) {
        diagnostics.error(format!("table '{}': {IID_COLUMN} must be declared NOT NULL: {contract}", table.name));
        return false;
    }
    let is_unique = is_pk || table.unique_column_sets.iter().any(|set| set.as_slice() == [IID_COLUMN]);
    if !is_unique {
        diagnostics.error(format!(
            "table '{}': {IID_COLUMN} must carry a declared UNIQUE or PRIMARY KEY constraint: {contract}",
            table.name
        ));
        return false;
    }
    true
}

fn role_binding(relates: &Relates, column: &Column, player: &ResolvedTable<'_>) -> RoleBinding {
    RoleBinding {
        column: column.name.clone(),
        role: relates.role.clone(),
        scoped_role: format!("{}:{}", relates.declared_by, relates.role),
        player_table: player.table.name.clone(),
        player_label: player.object.label.clone(),
    }
}

fn bind_role(
    mapping: &mut TableMapping,
    target: &ResolvedTable<'_>,
    column: &Column,
    role: &str,
    targets: &[ResolvedTable<'_>],
    schema: &Schema,
    diagnostics: &mut Diagnostics,
) {
    let relation = &target.object.label;
    let Some(relates) = target.object.relates.iter().find(|r| r.role == role) else {
        diagnostics.error(format!(
            "table '{}': column '{}' is bound to role '{role}', but relation '{relation}' has no such role",
            target.table.name, column.name
        ));
        return;
    };
    let Some(fk) = target.table.fk_of_column(&column.name) else {
        diagnostics.error(format!(
            "table '{}': column '{}' is bound to role '{role}' but is not a FOREIGN KEY",
            target.table.name, column.name
        ));
        return;
    };
    let Some(player) = resolve_player(target, column, fk, targets, diagnostics) else { return };
    let ancestry = schema.relation_ancestry(relation);
    if !player.object.plays.iter().any(|plays| plays.role == role && ancestry.contains(&plays.relation)) {
        diagnostics.error(format!(
            "table '{}': column '{}' is bound to role '{relation}:{role}', but its players' type '{}' (table '{}') \
             does not play that role",
            target.table.name, column.name, player.object.label, player.table.name
        ));
        return;
    }
    mapping.roles.push(role_binding(relates, column, player));
}

/// Without a directive, an FK column binds to the one role its players' type plays in the
/// relation; a type playing several roles binds to the one no other column binds, by the
/// directive or by inference so far.
fn infer_role(
    mapping: &mut TableMapping,
    target: &ResolvedTable<'_>,
    column: &Column,
    fk: &ForeignKey,
    targets: &[ResolvedTable<'_>],
    schema: &Schema,
    diagnostics: &mut Diagnostics,
) {
    let relation = &target.object.label;
    let Some(player) = resolve_player(target, column, fk, targets, diagnostics) else { return };
    let ancestry = schema.relation_ancestry(relation);
    let played_roles: Vec<&Relates> = target
        .object
        .relates
        .iter()
        .filter(|relates| {
            player.object.plays.iter().any(|plays| plays.role == relates.role && ancestry.contains(&plays.relation))
        })
        .collect();
    let plays_none = played_roles.is_empty();
    let candidate_roles: Vec<&Relates> = match played_roles.as_slice() {
        [_] => played_roles,
        _ => played_roles
            .into_iter()
            .filter(|relates| {
                !mapping.roles.iter().any(|bound| bound.role == relates.role)
                    && !target.bound_columns.iter().any(|(bound, _)| *bound == relates.role)
            })
            .collect(),
    };
    match candidate_roles.as_slice() {
        [relates] => mapping.roles.push(role_binding(relates, column, player)),
        [] if plays_none => diagnostics.error(format!(
            "table '{}': FK column '{}' targets '{}', whose type '{}' plays no role of relation '{relation}'",
            target.table.name, column.name, fk.referenced_table, player.object.label
        )),
        [] => diagnostics.error(format!(
            "table '{}': FK column '{}' targets '{}', whose type '{}' plays no role of relation '{relation}' that \
             another column does not already fill",
            target.table.name, column.name, fk.referenced_table, player.object.label
        )),
        _ => diagnostics.error(format!(
            "table '{}': FK column '{}' is ambiguous: '{}' plays {} roles of '{relation}'; bind the columns with \
             COMMENT ON TABLE {} IS '@typedb relation {relation} ({})'",
            target.table.name,
            column.name,
            player.object.label,
            candidate_roles.len(),
            quoted(&target.table.name),
            candidate_roles.iter().map(|relates| format!("{} = <column>", relates.role)).collect::<Vec<_>>().join(", ")
        )),
    }
}

/// The FK must reference a mapped table's instance-id column, which identifies the player
/// within its table.
fn resolve_player<'t, 'a>(
    target: &ResolvedTable<'_>,
    column: &Column,
    fk: &ForeignKey,
    targets: &'t [ResolvedTable<'a>],
    diagnostics: &mut Diagnostics,
) -> Option<&'t ResolvedTable<'a>> {
    if fk.columns.len() > 1 {
        diagnostics.error(format!(
            "table '{}': column '{}' is part of the composite FOREIGN KEY {:?}, which cannot bind a role: \
             reference the players' table by its {IID_COLUMN} column instead",
            target.table.name, column.name, fk.columns
        ));
        return None;
    }
    let Some(player) = targets.iter().find(|t| t.table.name == fk.referenced_table) else {
        diagnostics.error(format!(
            "table '{}': FK column '{}' references table '{}', which is not mapped (is it skipped or invalid?)",
            target.table.name, column.name, fk.referenced_table
        ));
        return None;
    };
    if fk.referenced_columns.as_slice() != [IID_COLUMN] {
        diagnostics.error(format!(
            "table '{}': FK column '{}' must reference '{}.{IID_COLUMN}' (the instance-id column); found {:?}",
            target.table.name, column.name, fk.referenced_table, fk.referenced_columns
        ));
        return None;
    }
    Some(player)
}

fn bind_owns(
    mapping: &mut TableMapping,
    target: &ResolvedTable<'_>,
    column: &Column,
    explicit_attribute: Option<&str>,
    schema: &Schema,
    diagnostics: &mut Diagnostics,
) {
    let attribute_label = match explicit_attribute {
        Some(label) => label.to_owned(),
        None => match find_by_name(&schema.attributes, &column.name, |a| &a.label) {
            Some(attribute) => attribute.label.clone(),
            None => {
                diagnostics.error(format!(
                    "table '{}': column '{}' has no matching attribute in the TypeDB schema \
                     (add the attribute, or mark the column '@typedb skip' / '@typedb owns <label>')",
                    target.table.name, column.name
                ));
                return;
            }
        },
    };

    let Some(attribute) = schema.attribute(&attribute_label) else {
        diagnostics.error(format!(
            "table '{}': column '{}' maps to attribute '{attribute_label}', which does not exist in the schema",
            target.table.name, column.name
        ));
        return;
    };
    if !target.object.owns.contains(&attribute_label) {
        diagnostics.error(format!(
            "table '{}': column '{}' maps to attribute '{attribute_label}', but type '{}' does not own it",
            target.table.name, column.name, target.object.label
        ));
        return;
    }
    if attribute.abstract_ {
        diagnostics.error(format!(
            "table '{}': column '{}' maps to attribute '{attribute_label}', which is abstract and cannot \
             have instances; map the column to a concrete subtype with '@typedb owns <label>'",
            target.table.name, column.name
        ));
        return;
    }
    let Some(value_type) = attribute.value_type else {
        diagnostics.error(format!(
            "table '{}': column '{}' maps to attribute '{attribute_label}', which has no value type in the schema",
            target.table.name, column.name
        ));
        return;
    };

    let (element_type, multi) = match &column.column_type {
        ColumnType::List(element) => (element.as_ref(), true),
        other => (other, false),
    };
    if let ColumnType::Unsupported { type_name, hint } = element_type {
        diagnostics.error(format!(
            "table '{}': column '{}' has unsupported type {type_name}: {hint}",
            target.table.name, column.name
        ));
        return;
    }
    if matches!(element_type, ColumnType::List(_)) {
        diagnostics.error(format!(
            "table '{}': column '{}' is a nested list, which has no TypeDB mapping",
            target.table.name, column.name
        ));
        return;
    }
    if let ColumnType::Decimal { scale } = element_type
        && *scale > DECIMAL_FRACTIONAL_DIGITS
    {
        diagnostics.error(format!(
            "table '{}': column '{}' has DECIMAL scale {scale}, beyond TypeDB's {DECIMAL_FRACTIONAL_DIGITS} \
             fractional digits; DuckDB would round the values when reading them",
            target.table.name, column.name
        ));
        return;
    }
    if !is_compatible(element_type, value_type) {
        diagnostics.error(format!(
            "table '{}': column '{}' ({}) cannot populate attribute '{attribute_label}' (value type \
             '{}'): the types are incompatible, and the loader never parses or coerces values",
            target.table.name,
            column.name,
            column.raw_type,
            value_type.name()
        ));
        return;
    }

    mapping.owns.push(OwnsMapping {
        column: column.name.clone(),
        attribute: attribute_label,
        value_type,
        multi,
        column_type: element_type.clone(),
    });
}

fn is_compatible(column_type: &ColumnType, value_type: ValueType) -> bool {
    use ValueType as V;
    match column_type {
        ColumnType::Boolean => value_type == V::Boolean,
        ColumnType::Integer => matches!(value_type, V::Integer | V::Double | V::Decimal),
        ColumnType::Double => value_type == V::Double,
        ColumnType::Decimal { .. } => matches!(value_type, V::Decimal | V::Double),
        ColumnType::Varchar | ColumnType::StringLike => value_type == V::String,
        ColumnType::Date => value_type == V::Date,
        ColumnType::Timestamp => value_type == V::Datetime,
        ColumnType::TimestampTz => value_type == V::DatetimeTz,
        ColumnType::Interval => value_type == V::Duration,
        ColumnType::List(_) | ColumnType::Unsupported { .. } => false,
    }
}

fn unmapped_schema_warnings(plan: &MappingPlan, schema: &Schema, diagnostics: &mut Diagnostics) {
    for (types, kind) in [(&schema.entities, "entity"), (&schema.relations, "relation")] {
        for object in types.iter().filter(|o| !o.abstract_) {
            let mapped = plan.tables.iter().any(|t| t.type_label == object.label);
            if !mapped {
                diagnostics.warning(format!(
                    "{kind} type '{}' has no DuckDB source; no instances will be produced",
                    object.label
                ));
            }
        }
    }
    for attribute in schema.attributes.iter().filter(|a| !a.abstract_) {
        let sourced = plan.tables.iter().any(|table| table.owns.iter().any(|owns| owns.attribute == attribute.label));
        if !sourced {
            diagnostics.warning(format!(
                "attribute type '{}' has no DuckDB source; no instances will be produced",
                attribute.label
            ));
        }
    }
}
