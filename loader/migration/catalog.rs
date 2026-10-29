/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use duckdb::{
    Connection,
    types::{Value, ValueRef},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnType {
    Boolean,
    // TINYINT to HUGEINT, signed and unsigned, but not UHUGEINT.
    Integer { bits: u8, signed: bool },
    Double,
    Decimal { precision: u8, scale: u8 },
    Varchar,
    // ENUM, UUID or JSON.
    StringLike,
    Date,
    Timestamp { nanoseconds: bool },
    TimestampTz,
    Interval,
    List(Box<ColumnType>),
    Unsupported { type_name: String, hint: &'static str },
}

impl ColumnType {
    pub(crate) const INSTANCE_ID: ColumnType = ColumnType::Integer { bits: 64, signed: false };

    // Whether every value converts to a double and back unchanged: integers up to 32 bits and decimals up to 15
    // digits do, as a double holds 53 bits.
    pub(crate) fn fits_double_exactly(&self) -> bool {
        match self {
            ColumnType::Integer { bits, .. } => *bits <= 32,
            ColumnType::Decimal { precision, .. } => *precision <= 15,
            _ => true,
        }
    }

    pub(crate) fn fits_i64(&self) -> bool {
        match self {
            ColumnType::Integer { bits, signed } => *bits < 64 || (*bits == 64 && *signed),
            _ => false,
        }
    }

    fn parse(data_type: &str) -> ColumnType {
        let upper = data_type.to_uppercase();
        if let Some(element) = upper.strip_suffix("[]") {
            return ColumnType::List(Box::new(ColumnType::parse(element)));
        }
        let integer = |bits, signed| ColumnType::Integer { bits, signed };
        match upper.as_str() {
            "BOOLEAN" => ColumnType::Boolean,
            "TINYINT" => integer(8, true),
            "SMALLINT" => integer(16, true),
            "INTEGER" => integer(32, true),
            "BIGINT" => integer(64, true),
            "HUGEINT" => integer(128, true),
            "UTINYINT" => integer(8, false),
            "USMALLINT" => integer(16, false),
            "UINTEGER" => integer(32, false),
            "UBIGINT" => integer(64, false),
            "FLOAT" | "DOUBLE" => ColumnType::Double,
            "VARCHAR" => ColumnType::Varchar,
            "UUID" | "JSON" => ColumnType::StringLike,
            "DATE" => ColumnType::Date,
            "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" => ColumnType::Timestamp { nanoseconds: false },
            "TIMESTAMP_NS" => ColumnType::Timestamp { nanoseconds: true },
            "TIMESTAMP WITH TIME ZONE" => ColumnType::TimestampTz,
            "INTERVAL" => ColumnType::Interval,
            "BLOB" => ColumnType::Unsupported {
                type_name: upper,
                hint: "TypeDB has no bytes type; encode the column as base64 VARCHAR first",
            },
            "TIME" | "TIME WITH TIME ZONE" => ColumnType::Unsupported {
                type_name: upper,
                hint: "TypeDB has no time-of-day type; use VARCHAR or a duration-since-midnight INTERVAL",
            },
            _ if upper.starts_with("DECIMAL") => {
                let (precision, scale) = decimal_precision_and_scale(&upper);
                ColumnType::Decimal { precision, scale }
            }
            _ if upper.starts_with("ENUM") => ColumnType::StringLike,
            _ if upper.starts_with("STRUCT") => ColumnType::Unsupported {
                type_name: upper,
                hint: "flatten STRUCT fields into columns, or into a child table",
            },
            _ if upper.starts_with("MAP") => ColumnType::Unsupported {
                type_name: upper,
                hint: "restructure MAP entries into a child table (owner id, key, value)",
            },
            _ if upper.starts_with("UNION") => ColumnType::Unsupported {
                type_name: upper,
                hint: "split UNION alternatives into separate nullable columns",
            },
            _ => ColumnType::Unsupported { type_name: upper, hint: "this DuckDB type has no TypeDB mapping" },
        }
    }
}

// DuckDB's default is DECIMAL(18,3).
fn decimal_precision_and_scale(upper: &str) -> (u8, u8) {
    let mut parts = upper.trim_end_matches(')').splitn(2, '(').nth(1).unwrap_or("").split(',');
    let mut next = || parts.next().and_then(|part| part.trim().parse().ok());
    (next().unwrap_or(18), next().unwrap_or(3))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn decimal_precision_and_scale_default_to_duckdb_values() {
        assert_eq!(decimal_precision_and_scale("DECIMAL(38,2)"), (38, 2));
        assert_eq!(decimal_precision_and_scale("DECIMAL(10, 3)"), (10, 3));
        assert_eq!(decimal_precision_and_scale("DECIMAL"), (18, 3));
        assert!(ColumnType::parse("UBIGINT") == ColumnType::INSTANCE_ID);
        assert!(!ColumnType::parse("UBIGINT").fits_i64() && ColumnType::parse("BIGINT").fits_i64());
        assert!(!ColumnType::parse("HUGEINT").fits_i64() && ColumnType::parse("UINTEGER").fits_i64());
    }
}

#[derive(Debug)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) raw_type: String,
    pub(crate) comment: Option<String>,
}

#[derive(Debug)]
pub(crate) struct ForeignKey {
    pub(crate) columns: Vec<String>,
    pub(crate) referenced_table: String,
    pub(crate) referenced_columns: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) comment: Option<String>,
    pub(crate) columns: Vec<Column>,
    pub(crate) primary_key: Vec<String>,
    pub(crate) unique_column_sets: Vec<Vec<String>>,
    pub(crate) not_null: HashSet<String>,
    pub(crate) foreign_keys: Vec<ForeignKey>,
}

impl Table {
    pub(crate) fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub(crate) fn fk_of_column(&self, name: &str) -> Option<&ForeignKey> {
        self.foreign_keys.iter().find(|fk| fk.columns.iter().any(|column| column == name))
    }

    pub(crate) fn declares_keys(&self) -> bool {
        !self.primary_key.is_empty() || !self.unique_column_sets.is_empty() || !self.foreign_keys.is_empty()
    }
}

#[derive(Debug)]
pub(crate) struct Catalog {
    pub(crate) tables: Vec<Table>,
    pub(crate) ignored: Vec<String>,
}

impl Catalog {
    pub(crate) fn read(conn: &Connection) -> Result<Catalog, String> {
        let mut tables = list_tables(conn).map_err(|err| format!("failed to list tables: {err}"))?;
        for table in &mut tables {
            read_columns(conn, table)
                .and_then(|()| read_constraints(conn, table))
                .map_err(|err| format!("failed to read table '{}': {err}", table.name))?;
        }
        let ignored = read_ignored(conn).map_err(|err| format!("failed to list schemas and views: {err}"))?;
        Ok(Catalog { tables, ignored })
    }
}

fn list_tables(conn: &Connection) -> duckdb::Result<Vec<Table>> {
    let mut stmt = conn.prepare(
        "SELECT table_name, comment FROM duckdb_tables() \
         WHERE schema_name = 'main' AND NOT internal ORDER BY table_name",
    )?;
    let tables = stmt.query_map([], |row| {
        Ok(Table {
            name: row.get(0)?,
            comment: row.get(1)?,
            columns: Vec::new(),
            primary_key: Vec::new(),
            unique_column_sets: Vec::new(),
            not_null: HashSet::new(),
            foreign_keys: Vec::new(),
        })
    })?;
    tables.collect()
}

fn read_ignored(conn: &Connection) -> duckdb::Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT 'table ' || schema_name || '.' || table_name FROM duckdb_tables() \
         WHERE schema_name <> 'main' AND NOT internal AND NOT temporary \
         UNION ALL SELECT 'view ' || schema_name || '.' || view_name FROM duckdb_views() \
         WHERE NOT internal AND NOT temporary ORDER BY 1",
    )?;
    stmt.query_map([], |row| row.get(0))?.collect()
}

fn read_columns(conn: &Connection, table: &mut Table) -> duckdb::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT column_name, data_type, comment FROM duckdb_columns() \
         WHERE schema_name = 'main' AND table_name = ? ORDER BY column_index",
    )?;
    let mut rows = stmt.query([&table.name])?;
    while let Some(row) = rows.next()? {
        let raw_type: String = row.get(1)?;
        table.columns.push(Column {
            name: row.get(0)?,
            column_type: ColumnType::parse(&raw_type),
            raw_type,
            comment: row.get(2)?,
        });
    }
    Ok(())
}

fn read_constraints(conn: &Connection, table: &mut Table) -> duckdb::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT constraint_type, constraint_column_names, referenced_table, referenced_column_names \
         FROM duckdb_constraints() WHERE schema_name = 'main' AND table_name = ?",
    )?;
    let mut rows = stmt.query([&table.name])?;
    while let Some(row) = rows.next()? {
        let constraint_type: String = row.get(0)?;
        let columns = list_column(row.get_ref(1)?);
        match constraint_type.as_str() {
            "PRIMARY KEY" => table.primary_key = columns,
            "UNIQUE" => table.unique_column_sets.push(columns),
            "NOT NULL" => table.not_null.extend(columns),
            "FOREIGN KEY" => {
                let referenced_table = row.get(2)?;
                let referenced_columns = list_column(row.get_ref(3)?);
                table.foreign_keys.push(ForeignKey { columns, referenced_table, referenced_columns });
            }
            _ => {}
        }
    }
    Ok(())
}

fn list_column(cell: ValueRef<'_>) -> Vec<String> {
    match Value::from(cell) {
        Value::List(items) => items
            .into_iter()
            .filter_map(|item| match item {
                Value::Text(text) => Some(text),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}
