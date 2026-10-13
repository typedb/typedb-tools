/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    catalog::{Catalog, Column, ColumnType, ForeignKey, Table},
    diagnostics::{Diagnostic, Diagnostics},
    schema::{ObjectType, Plays, Schema, ValueType},
    validate::quoted,
};

pub(crate) const IID_COLUMN: &str = "_iid";

#[derive(Debug, Clone)]
pub(crate) struct OwnsMapping {
    pub(crate) column: String,
    pub(crate) attribute: String,
    pub(crate) attribute_type_id: u16,
    pub(crate) value_type: ValueType,
    /// The object type declaring the ownership: the scope of `@unique`.
    pub(crate) declared_by: String,
    /// LIST column: one ownership per element.
    pub(crate) multi: bool,
    pub(crate) unique: bool,
    pub(crate) key: bool,
    /// The parsed column type (the element type for LIST columns).
    pub(crate) column_type: ColumnType,
}

impl OwnsMapping {
    /// One value of the column (a cell, or a list element) as SQL compares, deduplicates,
    /// and orders it, already in the attribute's value type: attributes and ownerships are
    /// read through the same expression, so they agree on every value. Strings compare
    /// byte-wise, as in TypeDB: a column collation such as NOCASE would otherwise merge
    /// distinct values.
    pub(crate) fn scalar(&self, expr: &str) -> String {
        match self.cast() {
            Some("VARCHAR") => format!("{expr}::VARCHAR COLLATE \"binary\""),
            Some(sql_type) => format!("{expr}::{sql_type}"),
            None if self.column_type == ColumnType::Varchar => format!("{expr} COLLATE \"binary\""),
            None => expr.to_owned(),
        }
    }

    /// The column as read with its owner's row; list columns stay whole.
    pub(crate) fn projection(&self) -> String {
        let column = quoted(&self.column);
        match (self.multi, self.cast()) {
            (true, Some(sql_type)) => format!("{column}::{sql_type}[]"),
            (true, None) => column,
            (false, _) => self.scalar(&column),
        }
    }

    /// One value per row, or per list element: the column as stored.
    pub(crate) fn value_expression(&self) -> String {
        let column = quoted(&self.column);
        if self.multi { format!("unnest({column})") } else { column }
    }

    /// [`Self::scalar`] where SQL equality differs from TypeDB's: a key that is equal exactly
    /// when TypeDB holds one attribute. DuckDB compares intervals normalised (1 month = 30
    /// days, 1 day = 24 hours), while a TypeDB duration keeps months, days and nanoseconds
    /// apart, as does the interval's text form.
    pub(crate) fn identity_key(&self, expr: &str) -> Option<String> {
        (self.column_type == ColumnType::Interval).then(|| format!("{expr}::VARCHAR"))
    }

    /// A value as compared and counted: equal exactly when TypeDB holds one attribute.
    pub(crate) fn identity(&self, expr: &str) -> String {
        self.identity_key(expr).unwrap_or_else(|| self.scalar(expr))
    }

    /// The row's values as a list of `{t: attribute, v: value}`, without NULLs: values of
    /// different attribute types are different attributes, even when equal.
    pub(crate) fn tagged_values(&self) -> String {
        let column = quoted(&self.column);
        let tag = format!("'{}'", self.attribute.replace('\'', "''"));
        if self.multi {
            format!(
                "coalesce(list_transform(list_filter({}, lambda x: x IS NOT NULL), lambda x: struct_pack(t := {tag}, \
                 v := {})), [])",
                self.projection(),
                self.identity("x")
            )
        } else {
            format!(
                "CASE WHEN {column} IS NULL THEN [] ELSE [struct_pack(t := {tag}, v := {})] END",
                self.identity(&column)
            )
        }
    }

    /// The SQL type a column value is cast to before conversion, if it is not read as is.
    /// Numeric columns populating one attribute must not be unified by SQL's own rules,
    /// which can round (DECIMAL(38,0) with DECIMAL(10,9) unifies to DECIMAL(38,0)): decimals
    /// are read as DECIMAL(38,19), which holds every TypeDB decimal (a 64-bit integer part,
    /// 19 fractional digits) exactly.
    fn cast(&self) -> Option<&'static str> {
        match (&self.column_type, self.value_type) {
            (ColumnType::StringLike, _) => Some("VARCHAR"),
            (ColumnType::Double, ValueType::Double) => None,
            (_, ValueType::Double) => Some("DOUBLE"),
            (_, ValueType::Decimal) => Some("DECIMAL(38,19)"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RoleBinding {
    pub(crate) column: String,
    /// Unscoped role name.
    pub(crate) role: String,
    /// The import format's role label: scoped to the relation that declares the role,
    /// which for inherited roles is a supertype of the instance's relation.
    pub(crate) scoped_role: String,
    /// The table the FK references: the players' table.
    pub(crate) player_table: String,
    pub(crate) player_type_id: u16,
    pub(crate) player_is_relation: bool,
    pub(crate) card_min: u64,
    pub(crate) card_max: Option<u64>,
    /// The player type's `plays` @card upper bound for this role, if declared.
    pub(crate) player_card_max: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct TableMapping {
    pub(crate) table: String,
    pub(crate) type_label: String,
    pub(crate) type_id: u16,
    pub(crate) is_relation: bool,
    pub(crate) iid_column: String,
    pub(crate) owns: Vec<OwnsMapping>,
    /// Junction tables only: FK columns bound to roles.
    pub(crate) roles: Vec<RoleBinding>,
    /// Relation tables only: the tables whose rows this table's rows can reach through
    /// role players and that reach back, this table included when it references itself.
    /// Empty when the table is in no reference cycle.
    pub(crate) reference_cycle: Vec<String>,
}

#[derive(Debug, Default)]
pub(crate) struct MappingPlan {
    /// In import order: entity tables, then relation tables after the tables of their
    /// players, so that a relation's players are imported before it.
    pub(crate) tables: Vec<TableMapping>,
}

impl MappingPlan {
    /// One line per resolved table, for the mapping report.
    pub(crate) fn summary(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| {
                format!(
                    "table '{}' -> {} '{}' (id: {}, {} attribute column(s){})",
                    table.table,
                    if table.is_relation { "relation" } else { "entity" },
                    table.type_label,
                    table.iid_column,
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

#[derive(Debug, Default)]
struct TableDirective {
    skip: bool,
    entity: Option<String>,
    relation: Option<(String, Vec<(String, String)>)>, // label, (role = column)*
}

#[derive(Debug, Default)]
struct ColumnDirective {
    skip: bool,
    owns: Option<String>,
}

const DIRECTIVE_PREFIX: &str = "@typedb";

/// The directive after the `@typedb` prefix, matched case-insensitively so that a
/// differently cased directive is not silently taken for a plain comment.
fn strip_directive_prefix(comment: &str) -> Option<&str> {
    let comment = comment.trim();
    let prefix = comment.get(..DIRECTIVE_PREFIX.len())?;
    prefix.eq_ignore_ascii_case(DIRECTIVE_PREFIX).then(|| comment[DIRECTIVE_PREFIX.len()..].trim())
}

fn parse_table_directive(comment: &str, table: &str) -> Result<Option<TableDirective>, String> {
    let Some(rest) = strip_directive_prefix(comment) else { return Ok(None) };
    let mut directive = TableDirective::default();
    if rest == "skip" {
        directive.skip = true;
    } else if let Some(label) = rest.strip_prefix("entity ") {
        directive.entity = Some(label.trim().to_owned());
    } else if let Some(spec) = rest.strip_prefix("relation ") {
        let (label, bindings) = parse_bindings(spec)
            .map_err(|err| format!("table '{table}': malformed @typedb relation directive: {err}"))?;
        directive.relation = Some((label, bindings));
    } else {
        return Err(format!(
            "table '{table}': unrecognised directive '@typedb {rest}' \
             (expected: skip | entity <label> | relation <label> (<role> = <column>, ...))"
        ));
    }
    Ok(Some(directive))
}

fn parse_column_directive(comment: &str, table: &str, column: &str) -> Result<Option<ColumnDirective>, String> {
    let Some(rest) = strip_directive_prefix(comment) else { return Ok(None) };
    let mut directive = ColumnDirective::default();
    if rest == "skip" {
        directive.skip = true;
    } else if let Some(label) = rest.strip_prefix("owns ") {
        directive.owns = Some(label.trim().to_owned());
    } else if rest.starts_with("relation ") {
        return Err(format!(
            "column '{table}.{column}': relations are tables, not columns; stage the relation as \
             its own table, then '@typedb skip' this column (see the README's 'Relations are tables')"
        ));
    } else {
        return Err(format!(
            "column '{table}.{column}': unrecognised directive '@typedb {rest}' \
             (expected: skip | owns <label>)"
        ));
    }
    Ok(Some(directive))
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

fn normalised(name: &str) -> String {
    name.replace('_', "-")
}

fn matches_label(name: &str, label: &str) -> bool {
    name == label || normalised(name) == label
}

/// Pass-1 product: a table with its TypeDB target decided.
struct ResolvedTable<'a> {
    table: &'a Table,
    label: String,
    type_id: u16,
    is_relation: bool,
    iid_column: String,
    directive: TableDirective,
    plays: Vec<Plays>,
}

pub(crate) fn resolve(catalog: &Catalog, schema: &Schema, diagnostics: &mut Diagnostics) -> MappingPlan {
    for ignored in &catalog.ignored {
        diagnostics.warning(Diagnostic::structure(format!(
            "{ignored} is not loaded: only tables of the 'main' schema are read (materialise a view as a table)"
        )));
    }
    let targets = resolve_tables(catalog, schema, diagnostics);
    let mut plan = MappingPlan::default();

    for target in &targets {
        let object = if target.is_relation {
            schema.relation(&target.label).expect("resolved in pass 1")
        } else {
            schema.entity(&target.label).expect("resolved in pass 1")
        };
        let mut mapping = TableMapping {
            table: target.table.name.clone(),
            type_label: target.label.clone(),
            type_id: target.type_id,
            is_relation: target.is_relation,
            iid_column: target.iid_column.clone(),
            owns: Vec::new(),
            roles: Vec::new(),
            reference_cycle: Vec::new(),
        };
        let explicit_roles: &[(String, String)] =
            target.directive.relation.as_ref().map(|(_, bindings)| bindings.as_slice()).unwrap_or(&[]);

        for column in &target.table.columns {
            let directive =
                match column.comment.as_deref().map(|c| parse_column_directive(c, &target.table.name, &column.name)) {
                    Some(Ok(Some(directive))) => directive,
                    Some(Ok(None)) | None => ColumnDirective::default(),
                    Some(Err(message)) => {
                        diagnostics.error(Diagnostic::structure(message));
                        continue;
                    }
                };
            if directive.skip {
                continue;
            }

            let explicit_role = explicit_roles.iter().find(|(_, col)| *col == column.name).map(|(role, _)| role);
            let fk = target.table.fk_of_column(&column.name);

            if let Some(role) = explicit_role {
                bind_role(&mut mapping, object, target, column, role, fk, &targets, schema, diagnostics);
            } else if let Some(fk) = fk {
                if target.is_relation {
                    infer_role(&mut mapping, object, target, column, fk, &targets, schema, diagnostics);
                } else {
                    diagnostics.error(Diagnostic::structure(format!(
                        "table '{table}': FK column '{column}' on an entity table has no TypeDB form: \
                         relations are tables. Create the relation table with `_iid UBIGINT PRIMARY KEY` \
                         and one FK column per role, fill it with INSERT INTO <relation> SELECT \
                         row_number() OVER (), _iid, {column_sql} FROM {table_sql} WHERE {column_sql} IS \
                         NOT NULL, then mark this column '@typedb skip'",
                        table = target.table.name,
                        column = column.name,
                        table_sql = quoted(&target.table.name),
                        column_sql = quoted(&column.name),
                    )));
                }
            } else {
                bind_owns(&mut mapping, object, target, column, directive.owns.as_deref(), schema, diagnostics);
            }
        }

        check_role_binding_columns(target.table, explicit_roles, diagnostics);

        // Roles of the relation type that no FK column populates.
        if target.is_relation {
            for relates in &object.relates {
                if !mapping.roles.iter().any(|r| r.role == relates.role) {
                    let message = format!(
                        "relation table '{}' never fills role '{}:{}'",
                        target.table.name, target.label, relates.role
                    );
                    if relates.card_min >= 1 {
                        diagnostics.error(Diagnostic::schema(format!(
                            "{message}; every instance would violate the role's @card lower bound"
                        )));
                    } else {
                        diagnostics.warning(Diagnostic::schema(message));
                    }
                }
            }
        }
        plan.tables.push(mapping);
    }

    unmapped_schema_warnings(&plan, schema, diagnostics);
    order_for_import(&mut plan);
    plan
}

/// Every column a relation directive binds must be a column of the table, bound once, and
/// not skipped: a typo would otherwise silently leave the role to inference.
fn check_role_binding_columns(table: &Table, bindings: &[(String, String)], diagnostics: &mut Diagnostics) {
    for (index, (role, column_name)) in bindings.iter().enumerate() {
        let problem = match table.column(column_name) {
            None => "which the table does not have",
            Some(_) if bindings[..index].iter().any(|(_, earlier)| earlier == column_name) => {
                "which the directive already binds; a column holds one role's players"
            }
            Some(column) => {
                match column.comment.as_deref().map(|c| parse_column_directive(c, &table.name, column_name)) {
                    Some(Ok(Some(ColumnDirective { skip: true, .. }))) => "which is marked '@typedb skip'",
                    _ => continue,
                }
            }
        };
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': the relation directive binds role '{role}' to column '{column_name}', {problem}",
            table.name
        )));
    }
}

/// Orders the tables for import, entities first, and marks the relation tables in reference
/// cycles. TypeDB deletes a relation that has no role players when its transaction commits,
/// so a relation must not arrive before all of its players; within a cycle, that is checked
/// row by row (see the data validation).
fn order_for_import(plan: &mut MappingPlan) {
    let relations: Vec<&TableMapping> = plan.tables.iter().filter(|table| table.is_relation).collect();
    let dependencies: Vec<Vec<usize>> = relations
        .iter()
        .map(|table| {
            let mut players: Vec<usize> = table
                .roles
                .iter()
                .filter_map(|role| relations.iter().position(|player| player.table == role.player_table))
                .collect();
            players.sort_unstable();
            players.dedup();
            players
        })
        .collect();
    let components = strongly_connected_components(&dependencies);

    let mut cycles: Vec<(String, Vec<String>)> = Vec::new();
    for component in &components {
        let is_cycle = component.len() > 1 || dependencies[component[0]].contains(&component[0]);
        if is_cycle {
            let mut names: Vec<String> = component.iter().map(|&index| relations[index].table.clone()).collect();
            names.sort();
            cycles.extend(names.iter().map(|name| (name.clone(), names.clone())));
        }
    }
    let order: Vec<String> = components
        .iter()
        .flat_map(|component| {
            let mut names: Vec<String> = component.iter().map(|&index| relations[index].table.clone()).collect();
            names.sort();
            names
        })
        .collect();

    let (mut ordered, mut relation_tables): (Vec<TableMapping>, Vec<TableMapping>) =
        std::mem::take(&mut plan.tables).into_iter().partition(|table| !table.is_relation);
    for name in order {
        let index = relation_tables.iter().position(|table| table.table == name).expect("ordered from these tables");
        let mut table = relation_tables.swap_remove(index);
        table.reference_cycle = cycles
            .iter()
            .find(|(member, _)| *member == table.table)
            .map(|(_, names)| names.clone())
            .unwrap_or_default();
        ordered.push(table);
    }
    plan.tables = ordered;
}

/// Tarjan's algorithm. Components come out dependencies first: a component is listed after
/// every component it depends on.
fn strongly_connected_components(dependencies: &[Vec<usize>]) -> Vec<Vec<usize>> {
    struct State<'a> {
        dependencies: &'a [Vec<usize>],
        next_index: usize,
        index: Vec<Option<usize>>,
        low_link: Vec<usize>,
        stack: Vec<usize>,
        on_stack: Vec<bool>,
        components: Vec<Vec<usize>>,
    }
    fn visit(state: &mut State<'_>, node: usize) {
        state.index[node] = Some(state.next_index);
        state.low_link[node] = state.next_index;
        state.next_index += 1;
        state.stack.push(node);
        state.on_stack[node] = true;
        for &next in &state.dependencies[node] {
            match state.index[next] {
                None => {
                    visit(state, next);
                    state.low_link[node] = state.low_link[node].min(state.low_link[next]);
                }
                Some(next_index) if state.on_stack[next] => {
                    state.low_link[node] = state.low_link[node].min(next_index);
                }
                Some(_) => {}
            }
        }
        if Some(state.low_link[node]) == state.index[node] {
            let mut component = Vec::new();
            while let Some(member) = state.stack.pop() {
                state.on_stack[member] = false;
                component.push(member);
                if member == node {
                    break;
                }
            }
            state.components.push(component);
        }
    }
    let count = dependencies.len();
    let mut state = State {
        dependencies,
        next_index: 0,
        index: vec![None; count],
        low_link: vec![0; count],
        stack: Vec::new(),
        on_stack: vec![false; count],
        components: Vec::new(),
    };
    for node in 0..count {
        if state.index[node].is_none() {
            visit(&mut state, node);
        }
    }
    state.components
}

fn resolve_tables<'a>(catalog: &'a Catalog, schema: &Schema, diagnostics: &mut Diagnostics) -> Vec<ResolvedTable<'a>> {
    let mut targets = Vec::new();
    for table in &catalog.tables {
        let directive = match table.comment.as_deref().map(|c| parse_table_directive(c, &table.name)) {
            Some(Ok(Some(directive))) => directive,
            Some(Ok(None)) | None => TableDirective::default(),
            Some(Err(message)) => {
                diagnostics.error(Diagnostic::structure(message));
                continue;
            }
        };
        if directive.skip {
            continue;
        }

        let (label, is_relation) = if let Some(label) = &directive.entity {
            (label.clone(), false)
        } else if let Some((label, _)) = &directive.relation {
            (label.clone(), true)
        } else {
            let entity = schema.entities.iter().find(|e| matches_label(&table.name, &e.label));
            let relation = schema.relations.iter().find(|r| matches_label(&table.name, &r.label));
            match (entity, relation) {
                (Some(entity), None) => (entity.label.clone(), false),
                (None, Some(relation)) => (relation.label.clone(), true),
                (Some(_), Some(_)) => {
                    diagnostics.error(Diagnostic::schema(format!(
                        "table '{}' matches both an entity and a relation type; disambiguate with \
                         COMMENT ON TABLE ... IS '@typedb entity <label>' or '@typedb relation <label> (...)'",
                        table.name
                    )));
                    continue;
                }
                (None, None) => {
                    diagnostics.error(Diagnostic::schema(format!(
                        "table '{}' has no matching entity or relation type in the TypeDB schema \
                         (add the type, or mark the table '@typedb skip')",
                        table.name
                    )));
                    continue;
                }
            }
        };

        let object = if is_relation { schema.relation(&label) } else { schema.entity(&label) };
        let Some(object) = object else {
            diagnostics.error(Diagnostic::schema(format!(
                "table '{}' is mapped to '{label}', which is not {} in the TypeDB schema",
                table.name,
                if is_relation { "a relation type" } else { "an entity type" }
            )));
            continue;
        };
        if object.abstract_ {
            diagnostics.error(Diagnostic::schema(format!(
                "table '{}' is mapped to abstract type '{label}', which cannot have instances",
                table.name
            )));
            continue;
        }

        let Some(iid_column) = instance_id_column(table, diagnostics) else { continue };
        targets.push(ResolvedTable {
            table,
            label,
            type_id: object.type_id,
            is_relation,
            iid_column,
            directive,
            plays: object.plays.clone(),
        });
    }
    targets
}

/// Every mapped table declares `_iid UBIGINT NOT NULL UNIQUE` (or PRIMARY KEY): the
/// object-id part of its instances' IIDs. DuckDB enforces the declared constraints, so
/// the data needs no id checks.
fn instance_id_column(table: &Table, diagnostics: &mut Diagnostics) -> Option<String> {
    let contract = format!(
        "declare `{IID_COLUMN} UBIGINT NOT NULL` with a UNIQUE or PRIMARY KEY constraint, holding \
         the instance's TypeDB object id (unsigned 64-bit; see the instance-id section of the README)"
    );
    let Some(column) = table.column(IID_COLUMN) else {
        diagnostics
            .error(Diagnostic::structure(format!("table '{}' has no {IID_COLUMN} column: {contract}", table.name)));
        return None;
    };
    if column.raw_type != "UBIGINT" {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': {IID_COLUMN} must be UBIGINT (a TypeDB object id is unsigned 64-bit), found {}: {contract}",
            table.name, column.raw_type
        )));
        return None;
    }
    let is_pk = table.primary_key.as_slice() == [IID_COLUMN.to_owned()];
    if !is_pk && !table.not_null.contains(IID_COLUMN) {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': {IID_COLUMN} must be declared NOT NULL: {contract}",
            table.name
        )));
        return None;
    }
    let is_unique = is_pk || table.unique_column_sets.iter().any(|set| set.as_slice() == [IID_COLUMN.to_owned()]);
    if !is_unique {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': {IID_COLUMN} must carry a declared UNIQUE or PRIMARY KEY constraint: {contract}",
            table.name
        )));
        return None;
    }
    Some(IID_COLUMN.to_owned())
}

impl ResolvedTable<'_> {
    fn plays_card_max(&self, relation_ancestry: &[String], role: &str) -> Option<u64> {
        self.plays
            .iter()
            .find(|plays| plays.role == role && relation_ancestry.contains(&plays.relation))
            .and_then(|plays| plays.card_max)
    }
}

fn find_target<'t, 'a>(targets: &'t [ResolvedTable<'a>], table_name: &str) -> Option<&'t ResolvedTable<'a>> {
    targets.iter().find(|t| t.table.name == table_name)
}

fn bind_role(
    mapping: &mut TableMapping,
    object: &ObjectType,
    target: &ResolvedTable<'_>,
    column: &Column,
    role: &str,
    fk: Option<&ForeignKey>,
    targets: &[ResolvedTable<'_>],
    schema: &Schema,
    diagnostics: &mut Diagnostics,
) {
    let Some(relates) = object.relates.iter().find(|r| r.role == role) else {
        diagnostics.error(Diagnostic::schema(format!(
            "table '{}': column '{}' is bound to role '{role}', but relation '{}' has no such role",
            target.table.name, column.name, mapping.type_label
        )));
        return;
    };
    let Some(fk) = fk else {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': column '{}' is bound to role '{role}' but is not a single-column FOREIGN KEY",
            target.table.name, column.name
        )));
        return;
    };
    let Some(player) = resolve_player(target, column, fk, targets, diagnostics) else { return };
    let ancestry = schema.relation_ancestry(&mapping.type_label);
    if !player.plays.iter().any(|plays| plays.role == role && ancestry.contains(&plays.relation)) {
        diagnostics.error(Diagnostic::schema(format!(
            "table '{}': column '{}' is bound to role '{}:{role}', but its players' type '{}' (table '{}') does \
             not play that role",
            target.table.name, column.name, mapping.type_label, player.label, player.table.name
        )));
        return;
    }
    mapping.roles.push(RoleBinding {
        column: column.name.clone(),
        role: role.to_owned(),
        scoped_role: format!("{}:{}", relates.declared_by, relates.role),
        player_table: player.table.name.clone(),
        player_type_id: player.type_id,
        player_is_relation: player.is_relation,
        card_min: relates.card_min,
        card_max: relates.card_max,
        player_card_max: player.plays_card_max(&ancestry, role),
    });
}

/// Convention path: the FK target's type must play exactly one (still unbound) role.
fn infer_role(
    mapping: &mut TableMapping,
    object: &ObjectType,
    target: &ResolvedTable<'_>,
    column: &Column,
    fk: &ForeignKey,
    targets: &[ResolvedTable<'_>],
    schema: &Schema,
    diagnostics: &mut Diagnostics,
) {
    let Some(player) = resolve_player(target, column, fk, targets, diagnostics) else { return };
    // `plays` may be declared against this relation or any of its supertypes; either way
    // the role must be among this relation's effective (non-specialised) roles.
    let ancestry = schema.relation_ancestry(&mapping.type_label);
    // A player type playing one role binds to it, also when other columns bind it already
    // (one column per player table); one playing several roles binds to the only one no
    // other column binds yet.
    let played_roles: Vec<&str> = player
        .plays
        .iter()
        .filter(|plays| ancestry.contains(&plays.relation))
        .map(|plays| plays.role.as_str())
        .filter(|role| object.relates.iter().any(|relates| relates.role == *role))
        .collect();
    let candidate_roles: Vec<&str> = match played_roles.as_slice() {
        [_] => played_roles,
        _ => played_roles.into_iter().filter(|role| !mapping.roles.iter().any(|bound| bound.role == *role)).collect(),
    };
    match candidate_roles.as_slice() {
        [role] => {
            let relates = object.relates.iter().find(|r| r.role == *role).expect("filtered on relates above");
            mapping.roles.push(RoleBinding {
                column: column.name.clone(),
                role: (*role).to_owned(),
                scoped_role: format!("{}:{}", relates.declared_by, relates.role),
                player_table: player.table.name.clone(),
                player_type_id: player.type_id,
                player_is_relation: player.is_relation,
                card_min: relates.card_min,
                card_max: relates.card_max,
                player_card_max: player.plays_card_max(&ancestry, role),
            });
        }
        [] => diagnostics.error(Diagnostic::schema(format!(
            "table '{}': FK column '{}' targets '{}', whose type '{}' plays no (unbound) role of relation '{}'",
            target.table.name, column.name, fk.referenced_table, player.label, mapping.type_label
        ))),
        _ => diagnostics.error(Diagnostic::schema(format!(
            "table '{}': FK column '{}' is ambiguous — '{}' plays {} roles of '{}'; bind the columns with \
             COMMENT ON TABLE \"{}\" IS '@typedb relation {} ({} = {}, ...)'",
            target.table.name,
            column.name,
            player.label,
            candidate_roles.len(),
            mapping.type_label,
            target.table.name,
            mapping.type_label,
            candidate_roles[0],
            column.name
        ))),
    }
}

/// Resolves and checks an FK's referenced table: it must be mapped, and the FK must point
/// at its instance-id column so role players can be identified without lookups.
fn resolve_player<'t, 'a>(
    target: &ResolvedTable<'_>,
    column: &Column,
    fk: &ForeignKey,
    targets: &'t [ResolvedTable<'a>],
    diagnostics: &mut Diagnostics,
) -> Option<&'t ResolvedTable<'a>> {
    let Some(player) = find_target(targets, &fk.referenced_table) else {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': FK column '{}' references table '{}', which is not mapped (is it skipped or invalid?)",
            target.table.name, column.name, fk.referenced_table
        )));
        return None;
    };
    // DuckDB reports an empty referenced-column list when the FK targets the primary key.
    let references_iid = match fk.referenced_columns.as_slice() {
        [] => player.table.primary_key.as_slice() == [player.iid_column.clone()],
        [referenced] => *referenced == player.iid_column,
        _ => false,
    };
    if !references_iid {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': FK column '{}' must reference '{}.{}' (the instance-id column); found {:?}",
            target.table.name,
            column.name,
            fk.referenced_table,
            player.iid_column,
            if fk.referenced_columns.is_empty() {
                player.table.primary_key.clone()
            } else {
                fk.referenced_columns.clone()
            }
        )));
        return None;
    }
    Some(player)
}

fn bind_owns(
    mapping: &mut TableMapping,
    object: &ObjectType,
    target: &ResolvedTable<'_>,
    column: &Column,
    explicit_attribute: Option<&str>,
    schema: &Schema,
    diagnostics: &mut Diagnostics,
) {
    let attribute_label = match explicit_attribute {
        Some(label) => label.to_owned(),
        None => {
            match schema.attributes.iter().find(|a| matches_label(&column.name, &a.label)) {
                Some(attribute) => attribute.label.clone(),
                // The `_iid` column exists for the loader itself; it is not data.
                None if column.name == IID_COLUMN => return,
                None => {
                    diagnostics.error(Diagnostic::schema(format!(
                        "table '{}': column '{}' has no matching attribute in the TypeDB schema \
                         (add the attribute, or mark the column '@typedb skip' / '@typedb owns <label>')",
                        target.table.name, column.name
                    )));
                    return;
                }
            }
        }
    };

    let Some(attribute) = schema.attribute(&attribute_label) else {
        diagnostics.error(Diagnostic::schema(format!(
            "table '{}': column '{}' maps to attribute '{attribute_label}', which does not exist in the schema",
            target.table.name, column.name
        )));
        return;
    };
    let Some(owns) = object.owns.iter().find(|owns| owns.attribute == attribute_label) else {
        diagnostics.error(Diagnostic::schema(format!(
            "table '{}': column '{}' maps to attribute '{attribute_label}', but type '{}' does not own it",
            target.table.name, column.name, object.label
        )));
        return;
    };
    if attribute.abstract_ {
        diagnostics.error(Diagnostic::schema(format!(
            "table '{}': column '{}' maps to attribute '{attribute_label}', which is abstract and cannot \
             have instances; map the column to a concrete subtype with '@typedb owns <label>'",
            target.table.name, column.name
        )));
        return;
    }
    let Some(value_type) = attribute.value_type else {
        diagnostics.error(Diagnostic::schema(format!("attribute '{attribute_label}' has no value type in the schema")));
        return;
    };

    let (element_type, multi) = match &column.column_type {
        ColumnType::List(element) => (element.as_ref(), true),
        other => (other, false),
    };
    if let ColumnType::Unsupported { type_name, hint } = element_type {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': column '{}' has unsupported type {type_name}: {hint}",
            target.table.name, column.name
        )));
        return;
    }
    if matches!(element_type, ColumnType::List(_)) {
        diagnostics.error(Diagnostic::structure(format!(
            "table '{}': column '{}' is a nested list, which has no TypeDB mapping",
            target.table.name, column.name
        )));
        return;
    }
    if !is_compatible(element_type, value_type) {
        diagnostics.error(Diagnostic::schema(format!(
            "table '{}': column '{}' ({}) cannot populate attribute '{attribute_label}' (value type \
             '{}'): the types are incompatible, and the loader never parses or coerces values",
            target.table.name,
            column.name,
            column.raw_type,
            value_type.name()
        )));
        return;
    }

    mapping.owns.push(OwnsMapping {
        column: column.name.clone(),
        attribute: attribute_label,
        attribute_type_id: attribute.type_id,
        value_type,
        declared_by: owns.declared_by.clone(),
        multi,
        unique: owns.unique,
        key: owns.key,
        column_type: element_type.clone(),
    });
}

fn is_compatible(column_type: &ColumnType, value_type: ValueType) -> bool {
    use ValueType as V;
    match column_type {
        ColumnType::Boolean => value_type == V::Boolean,
        ColumnType::Integer { .. } => matches!(value_type, V::Integer | V::Double | V::Decimal),
        ColumnType::Float | ColumnType::Double => value_type == V::Double,
        ColumnType::Decimal { .. } => matches!(value_type, V::Decimal | V::Double),
        ColumnType::Varchar | ColumnType::StringLike => value_type == V::String,
        ColumnType::Date => value_type == V::Date,
        ColumnType::Timestamp { .. } => value_type == V::Datetime,
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
                diagnostics.warning(Diagnostic::schema(format!(
                    "{kind} type '{}' has no DuckDB source; no instances will be produced",
                    object.label
                )));
            }
        }
    }
    for attribute in schema.attributes.iter().filter(|a| !a.abstract_) {
        let sourced = plan.tables.iter().any(|table| table.owns.iter().any(|owns| owns.attribute == attribute.label));
        if !sourced {
            diagnostics.warning(Diagnostic::schema(format!(
                "attribute type '{}' has no DuckDB source; no instances will be produced",
                attribute.label
            )));
        }
    }
}
