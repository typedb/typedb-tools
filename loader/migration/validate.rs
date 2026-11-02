/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use duckdb::Connection;

use crate::{
    Diagnostics,
    catalog::ColumnType,
    mapping::{IID_COLUMN, MappingPlan, OwnsMapping, RoleBinding, TableKind, TableMapping, quoted},
    schema::ValueType,
    value::{MAX_EPOCH_DAYS, MAX_EPOCH_SECONDS, MIN_EPOCH_DAYS, MIN_EPOCH_SECONDS},
};

const EXAMPLES: usize = 3;

// Above this many rows in tables with declared keys, staging them is likely to need more memory than it should.
const LARGE_KEYED_ROWS: u64 = 100_000_000;

// What would make the data file invalid is looked for before anything is converted: each check is one query,
// which DuckDB answers far faster than the conversion runs. The schema's constraints are the server's to
// enforce at import.
pub(crate) fn check_data(conn: &Connection, plan: &MappingPlan, diagnostics: &mut Diagnostics) {
    // An attribute read from a TIMESTAMP_NS column anywhere is read in nanoseconds from every column.
    let nanosecond_attributes: HashSet<&str> = plan
        .tables
        .iter()
        .flat_map(|table| table.owns.iter())
        .filter(|owns| owns.column_type == ColumnType::Timestamp { nanoseconds: true })
        .map(|owns| owns.attribute.as_str())
        .collect();
    let mut decimals_by_double: Vec<(&str, Vec<DecimalSource<'_>>)> = Vec::new();
    for table in &plan.tables {
        for owns in &table.owns {
            // A double printed exactly at scale 0 is an integer, which no decimal of another scale makes.
            if let (ValueType::Double, &ColumnType::Decimal { precision, scale: scale @ 1.. }) =
                (owns.value_type, &owns.column_type)
            {
                let source = DecimalSource { table, owns, precision, scale };
                match decimals_by_double.iter_mut().find(|(attribute, _)| *attribute == owns.attribute) {
                    Some((_, sources)) => sources.push(source),
                    None => decimals_by_double.push((&owns.attribute, vec![source])),
                }
            }
        }
    }
    let mut keyed_rows = Vec::new();
    check_ids_within_types(conn, plan, diagnostics);
    for table in &plan.tables {
        check_instance_ids(conn, table, diagnostics);
        if let TableKind::Relation { roles } = &table.kind {
            check_playerless_rows(conn, table, roles, diagnostics);
            for role in roles.iter().filter(|role| role.verify_reference) {
                check_reference(conn, table, role, &plan.tables[role.player], diagnostics);
            }
        }
        if let Some(rows) = table.declares_keys.then(|| row_count(conn, table)).flatten() {
            keyed_rows.push((table.name.as_str(), rows));
        }
        for owns in &table.owns {
            let nanoseconds = nanosecond_attributes.contains(owns.attribute.as_str());
            check_value_domain(conn, table, owns, nanoseconds, diagnostics);
        }
    }
    for (attribute, sources) in &decimals_by_double {
        check_decimals_across_scales(conn, attribute, sources, diagnostics);
    }
    if let Some(warning) = large_keyed_tables_warning(&keyed_rows, LARGE_KEYED_ROWS) {
        diagnostics.warning(warning);
    }
}

fn row_count(conn: &Connection, table: &TableMapping) -> Option<u64> {
    conn.query_row(&format!("SELECT count(*)::UBIGINT FROM {}", quoted(&table.name)), [], |row| row.get(0)).ok()
}

// DuckDB keeps the index of every PRIMARY KEY, UNIQUE and FOREIGN KEY constraint in memory while rows are
// inserted, and cannot spill it, so whoever stages large data with keys needs memory in proportion to the rows.
fn large_keyed_tables_warning(keyed_rows: &[(&str, u64)], threshold: u64) -> Option<String> {
    let total: u64 = keyed_rows.iter().map(|(_, rows)| rows).sum();
    if total < threshold {
        return None;
    }
    let tables: Vec<String> = keyed_rows.iter().map(|(name, _)| format!("'{name}'")).collect();
    Some(format!(
        "{total} rows are in tables with declared keys ({}): DuckDB keeps the indexes of PRIMARY KEY, UNIQUE and \
         FOREIGN KEY constraints in memory while rows are inserted, which can exhaust memory when staging data this \
         large. The keys are optional: without them the loader checks that every {IID_COLUMN} is unique and present \
         and that every reference resolves (mark role columns '@typedb references <table>'), and the server enforces \
         the schema's @key and @unique",
        tables.join(", ")
    ))
}

// An `_iid` is the instance's id within its type, as TypeDB numbers a type's instances below 2^63.
fn check_instance_ids(conn: &Connection, table: &TableMapping, diagnostics: &mut Diagnostics) {
    let iid = quoted(IID_COLUMN);
    let name = quoted(&table.name);
    Check {
        subject: format!("table '{}'", table.name),
        unit: "row(s)",
        predicate: "have an _iid of 2^63 or more, beyond TypeDB's instance ids; number the rows, and the columns \
                    referencing them, below that",
        example: "_iid",
        violations: format!("SELECT {iid} AS example FROM {name} WHERE {iid} >= {}", 1u64 << 63),
    }
    .report(conn, diagnostics);
    if !table.declared_ids.not_null {
        Check {
            subject: format!("table '{}'", table.name),
            unit: "row(s)",
            predicate: "have no _iid, which every row needs as its instance id",
            example: "rowid",
            violations: format!("SELECT rowid AS example FROM {name} WHERE {iid} IS NULL"),
        }
        .report(conn, diagnostics);
    }
}

// An `_iid` is unique within its type, also across the tables mapped to the type: one GROUP BY per type, which a
// single table with a declared unique `_iid` does not need.
fn check_ids_within_types(conn: &Connection, plan: &MappingPlan, diagnostics: &mut Diagnostics) {
    let iid = quoted(IID_COLUMN);
    let mut by_type: Vec<(&str, Vec<&TableMapping>)> = Vec::new();
    for table in &plan.tables {
        match by_type.iter_mut().find(|(label, _)| *label == table.type_label) {
            Some((_, tables)) => tables.push(table),
            None => by_type.push((&table.type_label, vec![table])),
        }
    }
    for (label, tables) in by_type {
        let subject = match tables.as_slice() {
            [table] if table.declared_ids.unique => continue,
            [table] => format!("table '{}'", table.name),
            _ => {
                let names: Vec<String> = tables.iter().map(|table| format!("'{}'", table.name)).collect();
                format!("tables {} mapped to '{label}'", names.join(", "))
            }
        };
        let selects: Vec<String> =
            tables.iter().map(|table| format!("SELECT {iid} FROM {}", quoted(&table.name))).collect();
        Check {
            subject,
            unit: "_iid value(s)",
            predicate: "appear in more than one row; an _iid is the instance's id within its type",
            example: "_iid",
            violations: format!(
                "SELECT {iid} AS example FROM ({}) WHERE {iid} IS NOT NULL GROUP BY {iid} HAVING count(*) > 1",
                selects.join(" UNION ALL ")
            ),
        }
        .report(conn, diagnostics);
    }
}

fn check_reference(
    conn: &Connection,
    table: &TableMapping,
    role: &RoleBinding,
    player: &TableMapping,
    diagnostics: &mut Diagnostics,
) {
    let (iid, column) = (quoted(IID_COLUMN), quoted(&role.column));
    Check {
        subject: format!("table '{}': column '{}' (references '{}')", table.name, role.column, player.name),
        unit: "row(s)",
        predicate: "hold a value that is no _iid of the referenced table",
        example: "value",
        violations: format!(
            "SELECT t.{column} AS example FROM {} t ANTI JOIN {} p ON t.{column} = p.{iid} \
             WHERE t.{column} IS NOT NULL",
            quoted(&table.name),
            quoted(&player.name)
        ),
    }
    .report(conn, diagnostics);
}

struct Check {
    subject: String,
    unit: &'static str,
    predicate: &'static str,
    example: &'static str,
    // Selects one `example` per violation, which the message counts and quotes.
    violations: String,
}

impl Check {
    // One streaming aggregate: the count and a few smallest examples, NULL ones apart, whatever the violations.
    fn report(&self, conn: &Connection, diagnostics: &mut Diagnostics) {
        let sql = format!(
            "SELECT count(*)::BIGINT, (count(*) - count(example))::BIGINT, \
             coalesce(array_to_string(min(example, {EXAMPLES})::VARCHAR[], ', '), '') \
             FROM ({})",
            self.violations
        );
        let read = conn.query_row(&sql, [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get(2)?)));
        match read {
            Err(err) => diagnostics.error(format!("{}: could not be checked: {err}", self.subject)),
            Ok((count, nulls, examples)) if count > 0 => {
                let examples: String = examples;
                let examples = match (examples.is_empty(), nulls > 0) {
                    (true, _) => "NULL".to_owned(),
                    (false, true) => format!("NULL, {examples}"),
                    (false, false) => examples,
                };
                diagnostics.error(format!(
                    "{}: {count} {} {}, e.g. {} {examples}",
                    self.subject, self.unit, self.predicate, self.example
                ))
            }
            Ok(_) => {}
        }
    }
}

// Values of different scales can each pass at their own scale and still make one double, as 0.1 and
// 0.10000000000000001 do. Values of at most 15 digits never do.
struct DecimalSource<'a> {
    table: &'a TableMapping,
    owns: &'a OwnsMapping,
    precision: u8,
    scale: u8,
}

fn check_decimals_across_scales(
    conn: &Connection,
    attribute: &str,
    sources: &[DecimalSource<'_>],
    diagnostics: &mut Diagnostics,
) {
    let scales: HashSet<u8> = sources.iter().map(|source| source.scale).collect();
    if scales.len() < 2 || sources.iter().all(|source| source.precision <= 15) {
        return;
    }
    // Compared as text without trailing fractional zeros, so that 0.1 and 0.10 are one value.
    let selects: Vec<String> = sources
        .iter()
        .map(|source| {
            format!(
                "SELECT e::DOUBLE AS d, rtrim(rtrim(e::VARCHAR, '0'), '.') AS v FROM (SELECT {} AS e FROM {})",
                source.owns.element(),
                quoted(&source.table.name)
            )
        })
        .collect();
    Check {
        subject: format!("attribute '{attribute}'"),
        unit: "double(s)",
        predicate: "are made from distinct decimals of different scales, which would merge into one attribute",
        example: "decimals",
        violations: format!(
            "SELECT string_agg(DISTINCT v, ' and ' ORDER BY v) AS example FROM ({}) WHERE d IS NOT NULL GROUP BY d \
             HAVING count(DISTINCT v) > 1",
            selects.join(" UNION ALL ")
        ),
    }
    .report(conn, diagnostics);
}

fn check_playerless_rows(
    conn: &Connection,
    table: &TableMapping,
    roles: &[RoleBinding],
    diagnostics: &mut Diagnostics,
) {
    let all_null: Vec<String> = roles.iter().map(|role| format!("{} IS NULL", quoted(&role.column))).collect();
    Check {
        subject: format!("relation table '{}'", table.name),
        unit: "row(s)",
        predicate: "fill no role (every role column is NULL), and a relation cannot exist without players",
        example: "_iid",
        violations: format!(
            "SELECT {} AS example FROM {} WHERE {}",
            quoted(IID_COLUMN),
            quoted(&table.name),
            all_null.join(" AND ")
        ),
    }
    .report(conn, diagnostics);
}

// Mirrors the limits of `value::Cells::value`, so that no value fails the conversion after hours of it.
// Timestamps and intervals are compared in integer microseconds: `epoch()` is a double and rounds at the edges.
fn check_value_domain(
    conn: &Connection,
    table: &TableMapping,
    owns: &OwnsMapping,
    nanoseconds: bool,
    diagnostics: &mut Diagnostics,
) {
    let to_double = owns.value_type == ValueType::Double;
    let (predicate, condition) = match &owns.column_type {
        ColumnType::Integer { .. } if !owns.column_type.fits_i64() && !to_double => (
            "hold an integer that does not fit TypeDB's 64-bit integer",
            "e NOT BETWEEN -9223372036854775808 AND 9223372036854775807".to_owned(),
        ),
        // The integer part is the floor: -0.5 is (-1, 0.5), so -9223372036854775808.5 does not fit.
        ColumnType::Decimal { precision, scale } if !to_double && precision - scale > 18 => (
            "hold a decimal whose integer part is beyond TypeDB's 64-bit range",
            "e < -9223372036854775808 OR e >= 9223372036854775808".to_owned(),
        ),
        // Rounding would merge distinct values into one attribute, and move their ownerships onto it. A value is
        // exact when the double, printed at the column's scale, reads back as the value: at one scale, distinct
        // values that pass make distinct doubles. DuckDB's own cast from a double to a wide decimal is not exact,
        // its formatting is.
        ColumnType::Integer { bits, .. } if to_double && *bits > 32 => (
            "hold a value that a double cannot represent exactly",
            "printf('%.0f', e::DOUBLE) <> e::VARCHAR".to_owned(),
        ),
        ColumnType::Decimal { precision, scale } if to_double && *precision > 15 => (
            "hold a value that a double cannot represent exactly",
            format!("printf('%.{scale}f', e::DOUBLE) <> e::VARCHAR"),
        ),
        ColumnType::Double => ("hold a double that is not finite, which TypeDB requires", "NOT isfinite(e)".to_owned()),
        ColumnType::Date => (
            "hold a date that is infinite or outside TypeDB's range",
            format!(
                "CASE WHEN isinf(e) THEN true ELSE (e - DATE '1970-01-01') NOT BETWEEN {MIN_EPOCH_DAYS} AND \
                 {MAX_EPOCH_DAYS} END"
            ),
        ),
        ColumnType::Timestamp { nanoseconds: false } if nanoseconds => (
            "hold a timestamp outside the nanosecond range that the attribute's TIMESTAMP_NS columns impose",
            format!(
                "CASE WHEN isinf(e) THEN true ELSE epoch_us(e) NOT BETWEEN {} AND {} END",
                i64::MIN / 1000,
                i64::MAX / 1000
            ),
        ),
        ColumnType::Timestamp { .. } | ColumnType::TimestampTz => (
            "hold a timestamp that is infinite or outside TypeDB's datetime range",
            format!(
                "CASE WHEN isinf(e) THEN true ELSE epoch_us(e) NOT BETWEEN {} AND {} END",
                MIN_EPOCH_SECONDS * 1_000_000,
                MAX_EPOCH_SECONDS * 1_000_000 + 999_999
            ),
        ),
        // The time part of a TypeDB duration is an unsigned count of nanoseconds; months and days are kept apart.
        ColumnType::Interval => (
            "hold an interval with a negative part or a time part beyond TypeDB's duration range",
            format!(
                "e::VARCHAR LIKE '%-%' OR (date_part('hour', e)::HUGEINT * 60 + date_part('minute', e)) * 60000000 \
                 + date_part('microseconds', e) > {}",
                u64::MAX / 1000
            ),
        ),
        ColumnType::Boolean
        | ColumnType::Integer { .. }
        | ColumnType::Decimal { .. }
        | ColumnType::Varchar
        | ColumnType::StringLike
        | ColumnType::List(_)
        | ColumnType::Unsupported { .. } => return,
    };
    Check {
        subject: format!("table '{}': column '{}'", table.name, owns.column),
        unit: "row(s)",
        predicate,
        example: "_iid",
        violations: format!(
            "SELECT DISTINCT example FROM (SELECT {} AS example, {} AS e FROM {}) WHERE {condition}",
            quoted(IID_COLUMN),
            owns.element(),
            quoted(&table.name)
        ),
    }
    .report(conn, diagnostics);
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn large_data_with_declared_keys_is_warned_about() {
        assert_eq!(large_keyed_tables_warning(&[("a", 60), ("b", 39)], 100), None);
        let warning = large_keyed_tables_warning(&[("a", 60), ("b", 40)], 100).expect("100 rows reach the threshold");
        assert!(warning.starts_with("100 rows are in tables with declared keys ('a', 'b')"), "{warning}");
        assert!(warning.contains("'@typedb references <table>'"), "{warning}");
    }
}
