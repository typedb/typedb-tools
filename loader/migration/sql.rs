/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) const IID_COLUMN: &str = "_iid";
pub(crate) const ROWID_COLUMN: &str = "rowid";
const TABLE_ALIAS: &str = "t";
const ELEMENT: &str = "e";
const UNION_ALL: &str = " UNION ALL ";

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
    pub(crate) const LIST_SUFFIX: &str = "[]";
    pub(crate) const MAX_DECIMAL_PRECISION: u8 = 38;

    pub(crate) fn decimal(precision: u8, scale: u8) -> String {
        format!("{DECIMAL}({precision},{scale})")
    }
}

pub(crate) mod catalog {
    pub(crate) const MAIN_TABLES: &str = "SELECT table_name, comment FROM duckdb_tables() \
         WHERE schema_name = 'main' AND NOT internal ORDER BY table_name";

    pub(crate) const IGNORED_TABLES_AND_VIEWS: &str = "SELECT 'table ' || schema_name || '.' || table_name \
         FROM duckdb_tables() WHERE schema_name <> 'main' AND NOT internal AND NOT temporary \
         UNION ALL SELECT 'view ' || schema_name || '.' || view_name FROM duckdb_views() \
         WHERE NOT internal AND NOT temporary ORDER BY 1";

    pub(crate) const TABLE_COLUMNS: &str = "SELECT c.column_name, c.data_type, c.comment, i.is_generated = 'ALWAYS' \
         FROM duckdb_columns() c JOIN system.information_schema.columns i ON i.table_schema = c.schema_name \
         AND i.table_name = c.table_name AND i.column_name = c.column_name \
         WHERE c.schema_name = 'main' AND c.table_name = ? ORDER BY c.column_index";

    pub(crate) const TABLE_CONSTRAINTS: &str = "SELECT constraint_type, constraint_column_names, referenced_table, \
         referenced_column_names FROM duckdb_constraints() WHERE schema_name = 'main' AND table_name = ?";
}

pub(crate) mod setting {
    pub(crate) const MEMORY_LIMIT: &str = "memory_limit";
    pub(crate) const THREADS: &str = "threads";
    pub(crate) const TEMP_DIRECTORY: &str = "temp_directory";
    pub(crate) const PRESERVE_INSERTION_ORDER: &str = "preserve_insertion_order";
    pub(crate) const AUTOINSTALL_KNOWN_EXTENSIONS: &str = "autoinstall_known_extensions";
    pub(crate) const AUTOLOAD_KNOWN_EXTENSIONS: &str = "autoload_known_extensions";

    pub(crate) fn current(name: &str) -> String {
        format!("SELECT current_setting('{name}')::VARCHAR")
    }

    pub(crate) fn set_memory_limit(limit: &str) -> String {
        format!("SET {MEMORY_LIMIT} = '{}'", limit.replace('\'', "''"))
    }

    pub(crate) fn set_threads(threads: usize) -> String {
        format!("SET {THREADS} = {threads}")
    }

    pub(crate) fn set_preserve_insertion_order(is_preserved: bool) -> String {
        format!("SET {PRESERVE_INSERTION_ORDER} = {is_preserved}")
    }
}

pub(crate) mod scan {
    use super::{UNION_ALL, quoted};

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
        format!("SELECT DISTINCT v FROM ({}) WHERE v IS NOT NULL ORDER BY 1", values.join(UNION_ALL))
    }
}

pub(crate) mod check {
    use super::{ELEMENT, IID_COLUMN, ROWID_COLUMN, UNION_ALL, number_text, quoted};

    pub(crate) fn count_and_examples(violations: &str, examples: usize) -> String {
        format!(
            "SELECT count(*)::BIGINT, (count(*) - count(example))::BIGINT, \
             coalesce(array_to_string(min(example, {examples})::VARCHAR[], ', '), '') FROM ({violations})"
        )
    }

    pub(crate) fn ids_at_or_above(table: &str, bound: u64) -> String {
        let iid = quoted(IID_COLUMN);
        format!("SELECT {iid} AS example FROM {} WHERE {iid} >= {bound}", quoted(table))
    }

    pub(crate) fn rows_without_id(table: &str) -> String {
        format!("SELECT {ROWID_COLUMN} AS example FROM {} WHERE {} IS NULL", quoted(table), quoted(IID_COLUMN))
    }

    pub(crate) fn repeated_ids(tables: &[&str]) -> String {
        let iid = quoted(IID_COLUMN);
        let ids: Vec<String> = tables.iter().map(|table| format!("SELECT {iid} FROM {}", quoted(table))).collect();
        format!(
            "SELECT {iid} AS example FROM ({}) WHERE {iid} IS NOT NULL GROUP BY {iid} HAVING count(*) > 1",
            ids.join(UNION_ALL)
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

    pub(crate) fn references(table: &str, column: &str) -> String {
        let column = quoted(column);
        format!("SELECT {column} AS example FROM {} WHERE {column} IS NOT NULL", quoted(table))
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
                    "SELECT {ELEMENT}::DOUBLE AS d, \
                     CASE WHEN contains({text}, '.') THEN rtrim(rtrim({text}, '0'), '.') ELSE {text} END AS v \
                     FROM (SELECT {element} AS {ELEMENT} FROM {})",
                    quoted(table),
                    text = number_text(ELEMENT)
                )
            })
            .collect();
        format!(
            "SELECT string_agg(DISTINCT v, ' and ' ORDER BY v) AS example FROM ({}) WHERE d IS NOT NULL GROUP BY d \
             HAVING count(DISTINCT v) > 1",
            selects.join(UNION_ALL)
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
    use typedb_cli_common::constants::{MICROS_IN_SECOND, SECONDS_IN_HOUR, SECONDS_IN_MINUTE};

    use super::{ELEMENT, number_text};

    pub(crate) fn beyond_i64() -> String {
        format!("{ELEMENT} NOT BETWEEN {} AND {}", i64::MIN, i64::MAX)
    }

    // The integer part is the floor: -0.5 is (-1, 0.5), so -9223372036854775808.5 does not fit.
    pub(crate) fn integer_part_beyond_i64() -> String {
        format!("{ELEMENT} < {} OR {ELEMENT} >= {}", i64::MIN, i64::MAX as u64 + 1)
    }

    // DuckDB's cast from a double to a wide decimal is not exact, its formatting is.
    pub(crate) fn inexact_as_double(scale: u8) -> String {
        format!("printf('%.{scale}f', {ELEMENT}::DOUBLE) <> {}", number_text(ELEMENT))
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
             OR (date_part('hour', {ELEMENT})::HUGEINT * {SECONDS_IN_HOUR} \
             + date_part('minute', {ELEMENT}) * {SECONDS_IN_MINUTE}) * {MICROS_IN_SECOND} \
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
    format!("{expression}::{element_type}{}", duckdb_type::LIST_SUFFIX)
}

pub(crate) fn unnest(expression: &str) -> String {
    format!("unnest({expression})")
}

// TypeDB compares strings byte-wise, whatever the column's collation.
pub(crate) fn byte_wise(expression: &str) -> String {
    format!("{expression} COLLATE {}", quoted("binary"))
}

// DuckDB prints a decimal without an integer part, such as DECIMAL(2,2), as `.5`, where printf gives `0.5`.
fn number_text(expression: &str) -> String {
    format!(r"regexp_replace({expression}::VARCHAR, '^(-?)\.', '\10.')")
}
