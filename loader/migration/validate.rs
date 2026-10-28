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
    for table in &plan.tables {
        if let TableKind::Relation { roles } = &table.kind {
            check_playerless_rows(conn, table, roles, diagnostics);
        }
        for owns in &table.owns {
            let nanoseconds = nanosecond_attributes.contains(owns.attribute.as_str());
            check_value_domain(conn, table, owns, nanoseconds, diagnostics);
        }
    }
}

struct Check {
    subject: String,
    predicate: &'static str,
    // Selects the violating rows' `_iid` as `example`.
    violations: String,
}

impl Check {
    fn report(&self, conn: &Connection, diagnostics: &mut Diagnostics) {
        let sql = format!(
            "SELECT n, example::VARCHAR FROM (SELECT count(*) OVER () AS n, example FROM ({}) \
             ORDER BY example LIMIT {EXAMPLES})",
            self.violations
        );
        let mut count: i64 = 0;
        let mut examples = Vec::new();
        let read = conn.prepare(&sql).and_then(|mut statement| {
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                count = row.get(0)?;
                examples.push(row.get::<_, String>(1)?);
            }
            Ok(())
        });
        match read {
            Err(err) => diagnostics.error(format!("{}: could not be checked: {err}", self.subject)),
            Ok(()) if count > 0 => diagnostics.error(format!(
                "{}: {count} row(s) {}, e.g. _iid {}",
                self.subject,
                self.predicate,
                examples.join(", ")
            )),
            Ok(()) => {}
        }
    }
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
        predicate: "fill no role (every role column is NULL), and a relation cannot exist without players",
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
        // exact when the double, printed at the column's scale, reads back as the value: DuckDB's own cast from a
        // double to a wide decimal is not exact, its formatting is.
        column_type if to_double && !column_type.fits_double_exactly() => (
            "hold a value that a double cannot represent exactly",
            format!(
                "printf('%.{}f', e::DOUBLE) <> e::VARCHAR",
                match column_type {
                    ColumnType::Decimal { scale, .. } => *scale,
                    _ => 0,
                }
            ),
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
                "e::VARCHAR LIKE '%-%' OR (epoch_us(e) - (date_part('year', e) * 12 + date_part('month', e)) \
                 * 2592000000000 - date_part('day', e) * 86400000000) > {}",
                u64::MAX / 1000
            ),
        ),
        _ => return,
    };
    Check {
        subject: format!("table '{}': column '{}'", table.name, owns.column),
        predicate,
        violations: format!(
            "SELECT DISTINCT example FROM (SELECT {} AS example, {} AS e FROM {}) WHERE {condition}",
            quoted(IID_COLUMN),
            owns.element(),
            quoted(&table.name)
        ),
    }
    .report(conn, diagnostics);
}
