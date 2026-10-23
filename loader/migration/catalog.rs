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

#[derive(Debug, Clone)]
pub(crate) enum ColumnType {
    Boolean,
    /// TINYINT to HUGEINT, signed and unsigned, but not UHUGEINT.
    Integer,
    /// FLOAT or DOUBLE.
    Double,
    Decimal {
        scale: u8,
    },
    Varchar,
    /// ENUM, UUID or JSON: read through a VARCHAR cast.
    StringLike,
    Date,
    /// Every precision, TIMESTAMP_S to TIMESTAMP_NS.
    Timestamp,
    TimestampTz,
    Interval,
    List(Box<ColumnType>),
    Unsupported {
        type_name: String,
        hint: &'static str,
    },
}

impl ColumnType {
    fn parse(data_type: &str) -> ColumnType {
        let upper = data_type.to_uppercase();
        if let Some(element) = upper.strip_suffix("[]") {
            return ColumnType::List(Box::new(ColumnType::parse(element)));
        }
        match upper.as_str() {
            "BOOLEAN" => ColumnType::Boolean,
            "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT"
            | "HUGEINT" => ColumnType::Integer,
            "FLOAT" | "DOUBLE" => ColumnType::Double,
            "VARCHAR" => ColumnType::Varchar,
            "UUID" | "JSON" => ColumnType::StringLike,
            "DATE" => ColumnType::Date,
            "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_NS" => ColumnType::Timestamp,
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
            _ if upper.starts_with("DECIMAL") => ColumnType::Decimal { scale: decimal_scale(&upper) },
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

/// The scale of `DECIMAL(width,scale)`; DuckDB's default is 3.
fn decimal_scale(upper: &str) -> u8 {
    upper.split(',').nth(1).and_then(|scale| scale.trim_end_matches(')').trim().parse().ok()).unwrap_or(3)
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
}

/// The tables of the `main` schema; views and other schemas are only listed as ignored.
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

/// The VARCHAR[] of a constraint's column names; NULL for a constraint without columns.
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
