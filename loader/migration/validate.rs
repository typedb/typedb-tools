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
    let mut keyed_rows = Vec::new();
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

fn check_instance_ids(conn: &Connection, table: &TableMapping, diagnostics: &mut Diagnostics) {
    let iid = quoted(IID_COLUMN);
    let name = quoted(&table.name);
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
    if !table.declared_ids.unique {
        Check {
            subject: format!("table '{}'", table.name),
            unit: "_iid value(s)",
            predicate: "appear in more than one row; an instance id must be unique within its table",
            example: "_iid",
            violations: format!(
                "SELECT {iid} AS example FROM {name} WHERE {iid} IS NOT NULL GROUP BY {iid} HAVING count(*) > 1"
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
    fn report(&self, conn: &Connection, diagnostics: &mut Diagnostics) {
        let sql = format!(
            "SELECT n, coalesce(example::VARCHAR, 'NULL') FROM (SELECT count(*) OVER () AS n, example FROM ({}) \
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
                "{}: {count} {} {}, e.g. {} {}",
                self.subject,
                self.unit,
                self.predicate,
                self.example,
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
