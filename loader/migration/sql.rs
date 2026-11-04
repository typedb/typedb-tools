/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) const IID_COLUMN: &str = "_iid";

pub(crate) mod duckdb_type {
    pub(crate) const BOOLEAN: &str = "BOOLEAN";
    pub(crate) const BIGINT: &str = "BIGINT";
    pub(crate) const DOUBLE: &str = "DOUBLE";
    pub(crate) const DECIMAL: &str = "DECIMAL";
    pub(crate) const VARCHAR: &str = "VARCHAR";
    pub(crate) const DATE: &str = "DATE";
    pub(crate) const TIMESTAMP: &str = "TIMESTAMP";
    pub(crate) const TIMESTAMPTZ: &str = "TIMESTAMPTZ";
    pub(crate) const INTERVAL: &str = "INTERVAL";

    pub(crate) fn decimal(precision: u8, scale: u8) -> String {
        format!("{DECIMAL}({precision},{scale})")
    }
}

pub(crate) mod catalog {
    pub(crate) const MAIN_TABLES: &str = "SELECT table_name, comment FROM duckdb_tables() \
         WHERE schema_name = 'main' AND NOT internal ORDER BY table_name";

    pub(crate) const UNREAD_TABLES_AND_VIEWS: &str = "SELECT 'table ' || schema_name || '.' || table_name \
         FROM duckdb_tables() WHERE schema_name <> 'main' AND NOT internal AND NOT temporary \
         UNION ALL SELECT 'view ' || schema_name || '.' || view_name FROM duckdb_views() \
         WHERE NOT internal AND NOT temporary ORDER BY 1";

    pub(crate) const TABLE_COLUMNS: &str = "SELECT column_name, data_type, comment FROM duckdb_columns() \
         WHERE schema_name = 'main' AND table_name = ? ORDER BY column_index";

    pub(crate) const TABLE_CONSTRAINTS: &str = "SELECT constraint_type, constraint_column_names, referenced_table, \
         referenced_column_names FROM duckdb_constraints() WHERE schema_name = 'main' AND table_name = ?";
}

pub(crate) mod setting {
    pub(crate) fn current(name: &str) -> String {
        format!("SELECT current_setting('{name}')::VARCHAR")
    }

    pub(crate) fn set_memory_limit(limit: &str) -> String {
        format!("SET memory_limit = '{}'", limit.replace('\'', "''"))
    }

    pub(crate) fn set_threads(threads: usize) -> String {
        format!("SET threads = {threads}")
    }

    pub(crate) fn set_preserve_insertion_order(is_preserved: bool) -> String {
        format!("SET preserve_insertion_order = {is_preserved}")
    }
}

pub(crate) mod scan {
    use super::quoted;

    pub(crate) fn row_count(table: &str) -> String {
        format!("SELECT count(*)::UBIGINT FROM {}", quoted(table))
    }

    pub(crate) fn rows(table: &str, projections: &[String]) -> String {
        let columns: Vec<String> =
            projections.iter().enumerate().map(|(index, projection)| format!("{projection} AS c{index}")).collect();
        format!("SELECT {} FROM {} {}", columns.join(", "), quoted(table), super::TABLE_ALIAS)
    }

    pub(crate) fn values(table: &str, value: &str) -> String {
        format!("SELECT {value} AS v FROM {}", quoted(table))
    }

    pub(crate) fn distinct_values_in_order(values: &[String]) -> String {
        format!("SELECT DISTINCT v FROM ({}) WHERE v IS NOT NULL ORDER BY 1", values.join(" UNION ALL "))
    }
}

pub(crate) mod check {
    use super::{ELEMENT, IID_COLUMN, quoted};

    pub(crate) fn summary(violations: &str, examples: usize) -> String {
        format!(
            "SELECT count(*)::BIGINT, (count(*) - count(example))::BIGINT, \
             coalesce(array_to_string(min(example, {examples})::VARCHAR[], ', '), '') FROM ({violations})"
        )
    }

    pub(crate) fn ids_from(table: &str, limit: u64) -> String {
        let iid = quoted(IID_COLUMN);
        format!("SELECT {iid} AS example FROM {} WHERE {iid} >= {limit}", quoted(table))
    }

    pub(crate) fn rows_without_id(table: &str) -> String {
        format!("SELECT rowid AS example FROM {} WHERE {} IS NULL", quoted(table), quoted(IID_COLUMN))
    }

    pub(crate) fn repeated_ids(tables: &[&str]) -> String {
        let iid = quoted(IID_COLUMN);
        let ids: Vec<String> = tables.iter().map(|table| format!("SELECT {iid} FROM {}", quoted(table))).collect();
        format!(
            "SELECT {iid} AS example FROM ({}) WHERE {iid} IS NOT NULL GROUP BY {iid} HAVING count(*) > 1",
            ids.join(" UNION ALL ")
        )
    }

    pub(crate) fn id_order(table: &str) -> String {
        let iid = quoted(IID_COLUMN);
        format!(
            "SELECT count(*)::UBIGINT, count({iid})::UBIGINT, min({iid}), max({iid}), \
             min({iid}::HUGEINT - rowid) = max({iid}::HUGEINT - rowid) FROM {}",
            quoted(table)
        )
    }

    pub(crate) fn references_outside(table: &str, column: &str, first: u64, last: u64) -> String {
        let column = quoted(column);
        format!(
            "SELECT {column} AS example FROM {} WHERE {column} IS NOT NULL AND {column} NOT BETWEEN {first} AND {last}",
            quoted(table)
        )
    }

    pub(crate) fn rows_without_players(table: &str, role_columns: &[&str]) -> String {
        let all_null: Vec<String> = role_columns.iter().map(|column| format!("{} IS NULL", quoted(column))).collect();
        format!("SELECT {} AS example FROM {} WHERE {}", quoted(IID_COLUMN), quoted(table), all_null.join(" AND "))
    }

    // Decimals are compared as text without trailing fractional zeros, so that 0.1 and 0.10 are one value.
    pub(crate) fn distinct_decimals_making_one_double(sources: &[(&str, String)]) -> String {
        let selects: Vec<String> = sources
            .iter()
            .map(|(table, element)| {
                format!(
                    "SELECT {ELEMENT}::DOUBLE AS d, rtrim(rtrim({ELEMENT}::VARCHAR, '0'), '.') AS v \
                     FROM (SELECT {element} AS {ELEMENT} FROM {})",
                    quoted(table)
                )
            })
            .collect();
        format!(
            "SELECT string_agg(DISTINCT v, ' and ' ORDER BY v) AS example FROM ({}) WHERE d IS NOT NULL GROUP BY d \
             HAVING count(DISTINCT v) > 1",
            selects.join(" UNION ALL ")
        )
    }

    pub(crate) fn rows_with_values_where(table: &str, element: &str, condition: &str) -> String {
        format!(
            "SELECT DISTINCT example FROM (SELECT {} AS example, {element} AS {ELEMENT} FROM {}) WHERE {condition}",
            quoted(IID_COLUMN),
            quoted(table)
        )
    }
}

pub(crate) mod value_condition {
    use super::ELEMENT;

    pub(crate) fn beyond_i64() -> String {
        format!("{ELEMENT} NOT BETWEEN {} AND {}", i64::MIN, i64::MAX)
    }

    // The integer part is the floor: -0.5 is (-1, 0.5), so -9223372036854775808.5 does not fit.
    pub(crate) fn integer_part_beyond_i64() -> String {
        format!("{ELEMENT} < {} OR {ELEMENT} >= {}", i64::MIN, 1u64 << 63)
    }

    // DuckDB's cast from a double to a wide decimal is not exact, its formatting is.
    pub(crate) fn inexact_as_double(scale: u8) -> String {
        format!("printf('%.{scale}f', {ELEMENT}::DOUBLE) <> {ELEMENT}::VARCHAR")
    }

    pub(crate) fn not_finite() -> String {
        format!("NOT isfinite({ELEMENT})")
    }

    pub(crate) fn date_outside(min_epoch_days: i64, max_epoch_days: i64) -> String {
        format!(
            "CASE WHEN isinf({ELEMENT}) THEN true \
             ELSE ({ELEMENT} - DATE '1970-01-01') NOT BETWEEN {min_epoch_days} AND {max_epoch_days} END"
        )
    }

    // In integer microseconds: `epoch()` is a double, which rounds at the edges.
    pub(crate) fn timestamp_outside(min_epoch_micros: i64, max_epoch_micros: i64) -> String {
        format!(
            "CASE WHEN isinf({ELEMENT}) THEN true \
             ELSE epoch_us({ELEMENT}) NOT BETWEEN {min_epoch_micros} AND {max_epoch_micros} END"
        )
    }

    // `isinf` takes no TIMESTAMPTZ_NS.
    pub(crate) fn infinite_nanosecond_timestamptz() -> String {
        format!("isinf({ELEMENT}::TIMESTAMPTZ)")
    }

    pub(crate) fn interval_negative_or_beyond(max_time_micros: u64) -> String {
        format!(
            "{ELEMENT}::VARCHAR LIKE '%-%' \
             OR (date_part('hour', {ELEMENT})::HUGEINT * 60 + date_part('minute', {ELEMENT})) * 60000000 \
             + date_part('microseconds', {ELEMENT}) > {max_time_micros}"
        )
    }
}

pub(crate) fn quoted(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn column_of_scanned_table(column: &str) -> String {
    format!("{TABLE_ALIAS}.{}", quoted(column))
}

pub(crate) fn cast(expression: &str, duckdb_type: &str) -> String {
    format!("{expression}::{duckdb_type}")
}

pub(crate) fn list_cast(expression: &str, element_type: &str) -> String {
    format!("{expression}::{element_type}[]")
}

pub(crate) fn unnest(expression: &str) -> String {
    format!("unnest({expression})")
}

// TypeDB compares strings byte-wise, whatever the column's collation.
pub(crate) fn byte_wise(expression: &str) -> String {
    format!("{expression} COLLATE {}", quoted("binary"))
}

const TABLE_ALIAS: &str = "t";
const ELEMENT: &str = "e";
