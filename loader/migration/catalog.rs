/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use duckdb::Connection;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColumnType {
    Boolean,
    /// Any of DuckDB's signed/unsigned integer flavours, including HUGEINT.
    Integer {
        always_fits_i64: bool,
    },
    Float,
    Double,
    Decimal {
        width: u8,
        scale: u8,
    },
    Varchar,
    /// ENUM / UUID / JSON: read as strings through a VARCHAR cast.
    StringLike,
    Date,
    Timestamp {
        nanoseconds: bool,
    },
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
            "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "UTINYINT" | "USMALLINT" | "UINTEGER" => {
                ColumnType::Integer { always_fits_i64: true }
            }
            "UBIGINT" | "HUGEINT" => ColumnType::Integer { always_fits_i64: false },
            "FLOAT" => ColumnType::Float,
            "DOUBLE" => ColumnType::Double,
            "VARCHAR" => ColumnType::Varchar,
            "UUID" => ColumnType::StringLike,
            "JSON" => ColumnType::StringLike,
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
            _ if upper.starts_with("DECIMAL") => parse_decimal(&upper),
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

fn parse_decimal(upper: &str) -> ColumnType {
    let digits: Vec<u8> = upper
        .trim_start_matches("DECIMAL")
        .trim_matches(|c| c == '(' || c == ')')
        .split(',')
        .filter_map(|part| part.trim().parse().ok())
        .collect();
    match digits.as_slice() {
        [width, scale] => ColumnType::Decimal { width: *width, scale: *scale },
        _ => ColumnType::Decimal { width: 18, scale: 3 }, // DuckDB's default
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) raw_type: String,
    pub(crate) comment: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ForeignKey {
    pub(crate) columns: Vec<String>,
    pub(crate) referenced_table: String,
    pub(crate) referenced_columns: Vec<String>,
}

#[derive(Debug, Clone)]
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
        self.foreign_keys.iter().find(|fk| fk.columns.len() == 1 && fk.columns[0] == name)
    }
}

#[derive(Debug)]
pub(crate) struct Catalog {
    pub(crate) tables: Vec<Table>,
    pub(crate) ignored: Vec<String>,
}

impl Catalog {
    pub(crate) fn read(conn: &Connection) -> Result<Catalog, String> {
        let mut tables = Vec::new();

        let mut stmt = conn
            .prepare(
                "SELECT table_name, comment FROM duckdb_tables() \
                 WHERE schema_name = 'main' AND NOT internal ORDER BY table_name",
            )
            .map_err(|err| format!("failed to list tables: {err}"))?;
        let mut rows = stmt.query([]).map_err(|err| format!("failed to list tables: {err}"))?;
        while let Some(row) = rows.next().map_err(|err| err.to_string())? {
            let name: String = row.get(0).map_err(|err| err.to_string())?;
            let comment: Option<String> = row.get(1).map_err(|err| err.to_string())?;
            tables.push(Table {
                name,
                comment,
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_column_sets: Vec::new(),
                not_null: HashSet::new(),
                foreign_keys: Vec::new(),
            });
        }

        for table in &mut tables {
            read_columns(conn, table)?;
            read_constraints(conn, table)?;
        }
        Ok(Catalog { tables, ignored: read_ignored(conn)? })
    }
}

fn read_ignored(conn: &Connection) -> Result<Vec<String>, String> {
    let sql = "SELECT 'table ' || schema_name || '.' || table_name FROM duckdb_tables() \
               WHERE schema_name <> 'main' AND NOT internal AND NOT temporary \
               UNION ALL SELECT 'view ' || schema_name || '.' || view_name FROM duckdb_views() \
               WHERE NOT internal AND NOT temporary ORDER BY 1";
    let mut stmt = conn.prepare(sql).map_err(|err| format!("failed to list schemas and views: {err}"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0)).map_err(|err| err.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|err| format!("failed to list schemas and views: {err}"))
}

fn read_columns(conn: &Connection, table: &mut Table) -> Result<(), String> {
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type, is_nullable, comment FROM duckdb_columns() \
             WHERE schema_name = 'main' AND table_name = ? ORDER BY column_index",
        )
        .map_err(|err| err.to_string())?;
    let mut rows = stmt.query([&table.name]).map_err(|err| err.to_string())?;
    while let Some(row) = rows.next().map_err(|err| err.to_string())? {
        let name: String = row.get(0).map_err(|err| err.to_string())?;
        let raw_type: String = row.get(1).map_err(|err| err.to_string())?;
        let comment: Option<String> = row.get(3).map_err(|err| err.to_string())?;
        table.columns.push(Column { column_type: ColumnType::parse(&raw_type), name, raw_type, comment });
    }
    Ok(())
}

fn read_constraints(conn: &Connection, table: &mut Table) -> Result<(), String> {
    let mut stmt = conn
        .prepare(
            "SELECT constraint_type, constraint_column_names, referenced_table, referenced_column_names \
             FROM duckdb_constraints() WHERE schema_name = 'main' AND table_name = ?",
        )
        .map_err(|err| err.to_string())?;
    let mut rows = stmt.query([&table.name]).map_err(|err| err.to_string())?;
    while let Some(row) = rows.next().map_err(|err| err.to_string())? {
        let constraint_type: String = row.get(0).map_err(|err| err.to_string())?;
        let columns = list_column(row.get_ref(1).map_err(|err| err.to_string())?)?;
        match constraint_type.as_str() {
            "PRIMARY KEY" => table.primary_key = columns,
            "UNIQUE" => table.unique_column_sets.push(columns),
            "NOT NULL" => {
                table.not_null.extend(columns);
            }
            "FOREIGN KEY" => {
                let referenced_table: String = row.get(2).map_err(|err| err.to_string())?;
                let referenced_columns = list_column(row.get_ref(3).map_err(|err| err.to_string())?)?;
                table.foreign_keys.push(ForeignKey { columns, referenced_table, referenced_columns });
            }
            _ => {} // CHECK constraints need no mapping
        }
    }
    Ok(())
}

fn list_column(cell: duckdb::types::ValueRef<'_>) -> Result<Vec<String>, String> {
    use duckdb::types::{Value, ValueRef};
    match cell {
        ValueRef::Null => Ok(Vec::new()),
        list @ ValueRef::List(..) => match Value::from(list) {
            Value::List(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    Value::Text(text) => Some(text),
                    _ => None,
                })
                .collect()),
            _ => Ok(Vec::new()),
        },
        other => Err(format!("unexpected constraint column list: {other:?}")),
    }
}
