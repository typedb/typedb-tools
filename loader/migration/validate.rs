/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use duckdb::Connection;
use typedb_cli_common::constants::{MICROS_IN_SECOND, NANOS_IN_MICRO};

use crate::{
    Cancellation, Findings,
    catalog::ColumnType,
    mapping::{MappingPlan, OwnershipColumn, RoleColumn, TableKind, TableMapping},
    schema::ValueType,
    sql::{self, IID_COLUMN, ROWID_COLUMN, value_condition},
    value::{MAX_EPOCH_DAYS, MAX_EPOCH_SECONDS, MIN_EPOCH_DAYS, MIN_EPOCH_SECONDS},
};

const EXAMPLES: usize = 3;
const INSTANCE_ID_BOUND: u64 = i64::MAX as u64 + 1;
const I64_SAFE_DIGITS: u8 = i64::MAX.ilog10() as u8;

// The schema's constraints are the server's to enforce at import.
pub(crate) fn check_data(
    connection: &Connection,
    plan: &MappingPlan,
    cancellation: &Cancellation,
    findings: &mut Findings,
) {
    let mut checker =
        DataChecker { connection, cancellation, findings, nanosecond_attributes: nanosecond_attributes(plan) };
    let id_orders = checker.read_id_orders(plan);
    checker.check_ids_unique_within_types(plan, &id_orders);
    for table in &plan.tables {
        checker.check_instance_ids(table);
        if let TableKind::Relation { roles } = &table.kind {
            checker.check_rows_have_players(table, roles);
            for role in roles.iter().filter(|role| !role.is_reference_declared) {
                let player_table = &plan.tables[role.player_table_index];
                checker.check_references(table, role, player_table, id_orders[role.player_table_index]);
            }
        }
        for ownership in &table.ownerships {
            checker.check_values_fit_typedb(table, ownership);
        }
    }
    for (attribute, sources) in &decimal_sources_by_double_attribute(plan) {
        checker.check_decimals_stay_distinct_as_doubles(attribute, sources);
    }
}

struct DataChecker<'a> {
    connection: &'a Connection,
    cancellation: &'a Cancellation,
    findings: &'a mut Findings,
    nanosecond_attributes: HashSet<&'a str>,
}

impl DataChecker<'_> {
    fn read_id_orders(&mut self, plan: &MappingPlan) -> Vec<IdOrder> {
        let referenced: HashSet<usize> = plan
            .tables
            .iter()
            .flat_map(|table| table.roles())
            .filter(|role| !role.is_reference_declared)
            .map(|role| role.player_table_index)
            .collect();
        let mut id_orders = Vec::new();
        for (index, table) in plan.tables.iter().enumerate() {
            let is_needed = !table.id_guarantees.is_unique || referenced.contains(&index);
            let id_order = if is_needed && !table.has_rowid_column && !self.cancellation.is_cancelled() {
                self.read_id_order(table)
            } else {
                IdOrder::Unknown
            };
            id_orders.push(id_order);
        }
        id_orders
    }

    fn read_id_order(&mut self, table: &TableMapping) -> IdOrder {
        let extent = self.connection.query_row(&sql::check::id_order(&table.name), [], |row| {
            Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
        });
        match extent {
            Ok((0, ..)) => IdOrder::Empty,
            Ok((rows, non_null_ids, Some(first), Some(last), Some(is_in_row_order)))
                if is_in_row_order && non_null_ids == rows && last - first == rows - 1 =>
            {
                IdOrder::Dense { first, last }
            }
            Ok(_) => IdOrder::NotDense,
            Err(_) if self.cancellation.is_cancelled() => IdOrder::Unknown,
            Err(err) => {
                self.findings.error(format!("table '{}': its {IID_COLUMN}s could not be read: {err}", table.name));
                IdOrder::Unknown
            }
        }
    }

    // Within a type, across its tables; one table with declared unique ids or dense ids needs none.
    fn check_ids_unique_within_types(&mut self, plan: &MappingPlan, id_orders: &[IdOrder]) {
        let mut tables_by_type: Vec<TablesOfType<'_>> = Vec::new();
        for (index, table) in plan.tables.iter().enumerate() {
            match tables_by_type.iter_mut().find(|type_tables| type_tables.label == table.type_label) {
                Some(type_tables) => type_tables.indices.push(index),
                None => tables_by_type.push(TablesOfType { label: &table.type_label, indices: vec![index] }),
            }
        }
        for TablesOfType { label, indices } in tables_by_type {
            let tables: Vec<&TableMapping> = indices.iter().map(|&index| &plan.tables[index]).collect();
            let subject = match indices.as_slice() {
                &[index]
                    if tables[0].id_guarantees.is_unique
                        || matches!(id_orders[index], IdOrder::Dense { .. } | IdOrder::Empty) =>
                {
                    continue;
                }
                [_] => format!("table '{}'", tables[0].name),
                _ => {
                    let quoted_names: Vec<String> = tables.iter().map(|table| format!("'{}'", table.name)).collect();
                    format!("tables {} mapped to '{label}'", quoted_names.join(", "))
                }
            };
            let names: Vec<&str> = tables.iter().map(|table| table.name.as_str()).collect();
            self.run(Check {
                subject,
                unit: format!("{IID_COLUMN} value(s)"),
                predicate: format!("appear in more than one row; an {IID_COLUMN} is the instance's id within its type"),
                example_label: IID_COLUMN,
                violations: sql::check::repeated_ids(&names),
            });
        }
    }

    fn check_instance_ids(&mut self, table: &TableMapping) {
        self.run(Check {
            subject: format!("table '{}'", table.name),
            unit: "row(s)".into(),
            predicate: format!(
                "have an {IID_COLUMN} of 2^63 or more, beyond TypeDB's instance ids; number the rows, \
                 and the columns referencing them, below that"
            ),
            example_label: IID_COLUMN,
            violations: sql::check::ids_at_or_above(&table.name, INSTANCE_ID_BOUND),
        });
        if !table.id_guarantees.is_not_null {
            self.run(Check {
                subject: format!("table '{}'", table.name),
                unit: "row(s)".into(),
                predicate: format!("have no {IID_COLUMN}, which every row needs as its instance id"),
                example_label: ROWID_COLUMN,
                violations: sql::check::rows_without_id(&table.name),
            });
        }
    }

    fn check_rows_have_players(&mut self, table: &TableMapping, roles: &[RoleColumn]) {
        let role_columns: Vec<&str> = roles.iter().map(|role| role.column.as_str()).collect();
        self.run(Check {
            subject: format!("relation table '{}'", table.name),
            unit: "row(s)".into(),
            predicate: "fill no role (every role column is NULL), and a relation cannot exist without players".into(),
            example_label: IID_COLUMN,
            violations: sql::check::rows_without_players(&table.name, &role_columns),
        });
    }

    fn check_references(
        &mut self,
        table: &TableMapping,
        role: &RoleColumn,
        player_table: &TableMapping,
        player_id_order: IdOrder,
    ) {
        let subject = format!("table '{}': column '{}' (references '{}')", table.name, role.column, player_table.name);
        let violations = match player_id_order {
            IdOrder::Dense { first, last } => sql::check::references_outside(&table.name, &role.column, first, last),
            IdOrder::Empty => sql::check::references(&table.name, &role.column),
            IdOrder::Unknown => return,
            IdOrder::NotDense => return self.findings.error(format!(
                "{subject}: the {IID_COLUMN}s of '{}' are not dense in row order, so the references cannot be checked; \
                 declare the column a FOREIGN KEY to '{}'({IID_COLUMN}), or number the rows of '{}' in order, as \
                 `row_number() OVER ()` does",
                player_table.name, player_table.name, player_table.name
            )),
        };
        self.run(Check {
            subject,
            unit: "row(s)".into(),
            predicate: format!("hold a value that is no {IID_COLUMN} of the referenced table"),
            example_label: "value",
            violations,
        });
    }

    fn check_values_fit_typedb(&mut self, table: &TableMapping, ownership: &OwnershipColumn) {
        let is_read_as_double = ownership.value_type == ValueType::Double;
        let is_beyond_i64 = matches!(ownership.element_type, ColumnType::Integer { bits, is_signed }
            if bits > 64 || bits == 64 && !is_signed);
        let is_widened_to_nanoseconds = self.nanosecond_attributes.contains(ownership.attribute.as_str())
            && !ownership.element_type.has_nanoseconds();
        let (predicate, condition) = match &ownership.element_type {
            ColumnType::Integer { .. } if is_beyond_i64 && !is_read_as_double => {
                ("hold an integer that does not fit TypeDB's 64-bit integer", value_condition::beyond_i64())
            }
            ColumnType::Decimal { precision, scale } if !is_read_as_double && precision - scale > I64_SAFE_DIGITS => (
                "hold a decimal whose integer part is beyond TypeDB's 64-bit range",
                value_condition::integer_part_beyond_i64(),
            ),
            // Rounding would merge distinct values into one attribute; at one scale, exact round-trips stay apart.
            ColumnType::Integer { bits, .. } if is_read_as_double && u32::from(*bits) > f64::MANTISSA_DIGITS => {
                ("hold a value that a double cannot represent exactly", value_condition::inexact_as_double(0))
            }
            ColumnType::Decimal { precision, scale } if is_read_as_double && u32::from(*precision) > f64::DIGITS => {
                ("hold a value that a double cannot represent exactly", value_condition::inexact_as_double(*scale))
            }
            ColumnType::Double => {
                ("hold a double that is not finite, which TypeDB requires", value_condition::not_finite())
            }
            ColumnType::Date => (
                "hold a date that is infinite or outside TypeDB's range",
                value_condition::date_outside(MIN_EPOCH_DAYS, MAX_EPOCH_DAYS),
            ),
            ColumnType::Timestamp { .. } | ColumnType::TimestampTz { .. } if is_widened_to_nanoseconds => (
                "hold a timestamp outside the nanosecond range that the attribute's nanosecond columns impose",
                value_condition::timestamp_outside(i64::MIN / NANOS_IN_MICRO as i64, i64::MAX / NANOS_IN_MICRO as i64),
            ),
            // Nanoseconds since 1970 in 64 bits are within TypeDB's range.
            ColumnType::TimestampTz { has_nanoseconds: true } => {
                ("hold a timestamp that is infinite", value_condition::infinite_nanosecond_timestamptz())
            }
            ColumnType::Timestamp { .. } | ColumnType::TimestampTz { has_nanoseconds: false } => (
                "hold a timestamp that is infinite or outside TypeDB's datetime range",
                value_condition::timestamp_outside(
                    MIN_EPOCH_SECONDS * MICROS_IN_SECOND as i64,
                    (MAX_EPOCH_SECONDS + 1) * MICROS_IN_SECOND as i64 - 1,
                ),
            ),
            ColumnType::Interval => (
                "hold an interval with a negative part or a time part beyond TypeDB's duration range",
                value_condition::interval_negative_or_beyond(u64::MAX / NANOS_IN_MICRO),
            ),
            ColumnType::Boolean
            | ColumnType::Integer { .. }
            | ColumnType::Decimal { .. }
            | ColumnType::Varchar
            | ColumnType::ReadAsVarchar
            | ColumnType::List(_)
            | ColumnType::Unsupported { .. } => return,
        };
        self.run(Check {
            subject: format!("table '{}': column '{}'", table.name, ownership.column),
            unit: "row(s)".into(),
            predicate: predicate.into(),
            example_label: IID_COLUMN,
            violations: sql::check::rows_with_values_where(&table.name, &ownership.unnested_values(), &condition),
        });
    }

    // Decimals of different scales can each round-trip and still make one double: 0.1 and 0.10000000000000001.
    fn check_decimals_stay_distinct_as_doubles(&mut self, attribute: &str, sources: &[DecimalSource<'_>]) {
        let scales: HashSet<u8> = sources.iter().map(|source| source.scale).collect();
        if scales.len() < 2 || sources.iter().all(|source| u32::from(source.precision) <= f64::DIGITS) {
            return;
        }
        let columns: Vec<(&str, String)> =
            sources.iter().map(|source| (source.table.name.as_str(), source.ownership.unnested_values())).collect();
        self.run(Check {
            subject: format!("attribute '{attribute}'"),
            unit: "double(s)".into(),
            predicate: "are made from distinct decimals of different scales, which would merge into one attribute"
                .into(),
            example_label: "decimals",
            violations: sql::check::distinct_decimals_making_one_double(&columns),
        });
    }

    fn run(&mut self, check: Check) {
        if self.cancellation.is_cancelled() {
            return;
        }
        let query = sql::check::count_and_examples(&check.violations, EXAMPLES);
        let counted = self
            .connection
            .query_row(&query, [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?)));
        match counted {
            Err(_) if self.cancellation.is_cancelled() => {}
            Err(err) => self.findings.error(format!("{}: could not be checked: {err}", check.subject)),
            Ok((count, nulls, examples)) if count > 0 => {
                let examples = match (examples.is_empty(), nulls > 0) {
                    (true, _) => "NULL".into(),
                    (false, true) => format!("NULL, {examples}"),
                    (false, false) => examples,
                };
                self.findings.error(format!(
                    "{}: {count} {} {}, e.g. {} {examples}",
                    check.subject, check.unit, check.predicate, check.example_label
                ))
            }
            Ok(_) => {}
        }
    }
}

struct TablesOfType<'a> {
    label: &'a str,
    indices: Vec<usize>,
}

struct Check {
    subject: String,
    unit: String,
    predicate: String,
    example_label: &'static str,
    violations: String,
}

// Ids `first, first + 1, ...` in row order are distinct and present, and a reference is one exactly when in range.
#[derive(Clone, Copy)]
enum IdOrder {
    Dense { first: u64, last: u64 },
    Empty,
    NotDense,
    Unknown,
}

struct DecimalSource<'a> {
    table: &'a TableMapping,
    ownership: &'a OwnershipColumn,
    precision: u8,
    scale: u8,
}

// An attribute read from a nanosecond column anywhere is read in nanoseconds from every column.
fn nanosecond_attributes(plan: &MappingPlan) -> HashSet<&str> {
    plan.tables
        .iter()
        .flat_map(|table| table.ownerships.iter())
        .filter(|ownership| ownership.element_type.has_nanoseconds())
        .map(|ownership| ownership.attribute.as_str())
        .collect()
}

// A double printed exactly at scale 0 is an integer, which no decimal of another scale makes.
fn decimal_sources_by_double_attribute(plan: &MappingPlan) -> Vec<(&str, Vec<DecimalSource<'_>>)> {
    let mut sources_by_attribute: Vec<(&str, Vec<DecimalSource<'_>>)> = Vec::new();
    for table in &plan.tables {
        for ownership in &table.ownerships {
            if let (ValueType::Double, &ColumnType::Decimal { precision, scale: scale @ 1.. }) =
                (ownership.value_type, &ownership.element_type)
            {
                let source = DecimalSource { table, ownership, precision, scale };
                match sources_by_attribute.iter_mut().find(|(attribute, _)| *attribute == ownership.attribute) {
                    Some((_, sources)) => sources.push(source),
                    None => sources_by_attribute.push((&ownership.attribute, vec![source])),
                }
            }
        }
    }
    sources_by_attribute
}
