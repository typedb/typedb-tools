/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use duckdb::Connection;

use crate::{
    Diagnostics,
    catalog::ColumnType,
    mapping::{IID_COLUMN, MappingPlan, OwnsMapping, TableMapping, quoted},
    schema::ValueType,
    value::{MAX_EPOCH_DAYS, MAX_EPOCH_SECONDS, MIN_EPOCH_DAYS, MIN_EPOCH_SECONDS},
};

const EXAMPLES: usize = 3;

// What would make the data file invalid is looked for before anything is converted: each check is one query,
// which DuckDB answers far faster than the conversion runs. The schema's constraints are the server's to
// enforce at import.
pub(crate) fn check_data(conn: &Connection, plan: &MappingPlan, diagnostics: &mut Diagnostics) {
    for table in &plan.tables {
        if table.is_relation {
            check_playerless_rows(conn, table, diagnostics);
        }
        for owns in &table.owns {
            check_value_domain(conn, table, owns, diagnostics);
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

fn check_playerless_rows(conn: &Connection, table: &TableMapping, diagnostics: &mut Diagnostics) {
    let all_null: Vec<String> = table.roles.iter().map(|role| format!("{} IS NULL", quoted(&role.column))).collect();
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

// Mirrors the limits of `value::convert_cell`, so that no value fails the conversion after hours of it.
// Timestamps and intervals are compared in integer microseconds: `epoch()` is a double and rounds at the edges.
fn check_value_domain(conn: &Connection, table: &TableMapping, owns: &OwnsMapping, diagnostics: &mut Diagnostics) {
    let raw = owns.raw_type.as_str();
    let wide_integer = raw.starts_with("UBIGINT") || raw.starts_with("HUGEINT");
    let to_double = owns.value_type == ValueType::Double;
    let (predicate, condition) = match &owns.column_type {
        ColumnType::Integer if wide_integer && !to_double => (
            "hold an integer that does not fit TypeDB's 64-bit integer",
            "e NOT BETWEEN -9223372036854775808 AND 9223372036854775807".to_owned(),
        ),
        // The integer part is the floor: -0.5 is (-1, 0.5), so -9223372036854775808.5 does not fit.
        ColumnType::Decimal { scale } if !to_double && decimal_precision(raw) - u32::from(*scale) > 18 => (
            "hold a decimal whose integer part is beyond TypeDB's 64-bit range",
            "e < -9223372036854775808 OR e >= 9223372036854775808".to_owned(),
        ),
        ColumnType::Double => ("hold a double that is not finite, which TypeDB requires", "NOT isfinite(e)".to_owned()),
        ColumnType::Date => (
            "hold a date that is infinite or outside TypeDB's range",
            format!(
                "CASE WHEN isinf(e) THEN true ELSE (e - DATE '1970-01-01') NOT BETWEEN {MIN_EPOCH_DAYS} AND \
                 {MAX_EPOCH_DAYS} END"
            ),
        ),
        ColumnType::Timestamp | ColumnType::TimestampTz => (
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

// DuckDB's default DECIMAL precision is 18.
fn decimal_precision(raw_type: &str) -> u32 {
    raw_type
        .split_once('(')
        .and_then(|(_, rest)| rest.split([',', ')']).next())
        .and_then(|precision| precision.trim().parse().ok())
        .unwrap_or(18)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn decimal_precision_is_read_from_the_declared_type() {
        assert_eq!(decimal_precision("DECIMAL(38,2)"), 38);
        assert_eq!(decimal_precision("DECIMAL(10, 3)"), 10);
        assert_eq!(decimal_precision("DECIMAL"), 18);
    }
}
