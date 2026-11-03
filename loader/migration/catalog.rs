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
    TimestampTz { nanoseconds: bool },
    Interval,
    List(Box<ColumnType>),
    Unsupported { hint: &'static str },
}

impl ColumnType {
    pub(crate) const INSTANCE_ID: ColumnType = ColumnType::Integer { bits: 64, signed: false };

    pub(crate) fn fits_i64(&self) -> bool {
        match self {
            ColumnType::Integer { bits, signed } => *bits < 64 || (*bits == 64 && *signed),
            ColumnType::Boolean
            | ColumnType::Double
            | ColumnType::Decimal { .. }
            | ColumnType::Varchar
            | ColumnType::StringLike
            | ColumnType::Date
            | ColumnType::Timestamp { .. }
            | ColumnType::TimestampTz { .. }
            | ColumnType::Interval
            | ColumnType::List(_)
            | ColumnType::Unsupported { .. } => false,
        }
    }

    fn parse(data_type: &str) -> ColumnType {
        let upper = data_type.to_uppercase();
        if let Some(element) = upper.strip_suffix("[]") {
            return ColumnType::List(Box::new(ColumnType::parse(element)));
        }
        let integer = |bits, signed| ColumnType::Integer { bits, signed };
        let unsupported = |hint| ColumnType::Unsupported { hint };
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
            "TIMESTAMP WITH TIME ZONE" => ColumnType::TimestampTz { nanoseconds: false },
            "TIMESTAMPTZ_NS" => ColumnType::TimestampTz { nanoseconds: true },
            "INTERVAL" => ColumnType::Interval,
            "UHUGEINT" | "BIGNUM" => unsupported("TypeDB integers are 64-bit; cast to BIGINT, DECIMAL or VARCHAR"),
            "BLOB" => unsupported("TypeDB has no bytes type; encode the column as base64 VARCHAR first"),
            "BIT" | "INET" => unsupported("cast it to VARCHAR"),
            "TIME" | "TIME_NS" | "TIME WITH TIME ZONE" => {
                unsupported("TypeDB has no time-of-day type; use VARCHAR or a duration-since-midnight INTERVAL")
            }
            "GEOMETRY" => unsupported("convert it to text, such as well-known text"),
            "VARIANT" => unsupported("cast it to the type of its values"),
            "\"NULL\"" => unsupported("it holds only NULLs and has no type; give it one, or '@typedb skip' it"),
            _ if upper.starts_with("DECIMAL") => {
                let (precision, scale) = decimal_precision_and_scale(&upper);
                ColumnType::Decimal { precision, scale }
            }
            _ if upper.starts_with("ENUM") => ColumnType::StringLike,
            _ if upper.ends_with(']') => unsupported("cast the fixed-size ARRAY to a LIST, as `column::INTEGER[]`"),
            _ if upper.starts_with("STRUCT") || upper.starts_with("TUPLE") => {
                unsupported("flatten STRUCT fields into columns, or into a child table")
            }
            _ if upper.starts_with("MAP") => {
                unsupported("restructure MAP entries into a child table (owner id, key, value)")
            }
            _ if upper.starts_with("UNION") => unsupported("split UNION alternatives into separate nullable columns"),
            _ => unsupported("this DuckDB type has no TypeDB mapping"),
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

    // A DuckDB upgrade that adds or renames a type fails here until the loader classifies it.
    #[test]
    fn every_duckdb_type_is_classified() {
        let refused = || ColumnType::Unsupported { hint: "" };
        let integer = |bits, signed| ColumnType::Integer { bits, signed };
        let types = [
            ("ARRAY", "INTEGER[2]", refused()),
            ("BIGINT", "BIGINT", integer(64, true)),
            ("BIGNUM", "BIGNUM", refused()),
            ("BIT", "BIT", refused()),
            ("BLOB", "BLOB", refused()),
            ("BOOLEAN", "BOOLEAN", ColumnType::Boolean),
            ("DATE", "DATE", ColumnType::Date),
            ("DECIMAL", "DECIMAL(10,2)", ColumnType::Decimal { precision: 10, scale: 2 }),
            ("DOUBLE", "DOUBLE", ColumnType::Double),
            ("ENUM", "ENUM('a')", ColumnType::StringLike),
            ("FLOAT", "FLOAT", ColumnType::Double),
            ("GEOMETRY", "GEOMETRY", refused()),
            ("HUGEINT", "HUGEINT", integer(128, true)),
            ("INTEGER", "INTEGER", integer(32, true)),
            ("INTERVAL", "INTERVAL", ColumnType::Interval),
            ("LIST", "INTEGER[]", ColumnType::List(Box::new(integer(32, true)))),
            ("MAP", "MAP(VARCHAR, INTEGER)", refused()),
            ("NULL", "", refused()),
            ("SMALLINT", "SMALLINT", integer(16, true)),
            ("STRUCT", "STRUCT(a INTEGER)", refused()),
            ("TIME", "TIME", refused()),
            ("TIME WITH TIME ZONE", "TIMETZ", refused()),
            ("TIME_NS", "TIME_NS", refused()),
            ("TIMESTAMP", "TIMESTAMP", ColumnType::Timestamp { nanoseconds: false }),
            ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", ColumnType::TimestampTz { nanoseconds: false }),
            ("TIMESTAMP_MS", "TIMESTAMP_MS", ColumnType::Timestamp { nanoseconds: false }),
            ("TIMESTAMP_NS", "TIMESTAMP_NS", ColumnType::Timestamp { nanoseconds: true }),
            ("TIMESTAMP_S", "TIMESTAMP_S", ColumnType::Timestamp { nanoseconds: false }),
            ("TIMESTAMPTZ_NS", "TIMESTAMPTZ_NS", ColumnType::TimestampTz { nanoseconds: true }),
            ("TINYINT", "TINYINT", integer(8, true)),
            ("TUPLE", "", refused()),
            ("UBIGINT", "UBIGINT", integer(64, false)),
            ("UHUGEINT", "UHUGEINT", refused()),
            ("UINTEGER", "UINTEGER", integer(32, false)),
            ("UNION", "UNION(a INTEGER)", refused()),
            ("USMALLINT", "USMALLINT", integer(16, false)),
            ("UTINYINT", "UTINYINT", integer(8, false)),
            ("UUID", "UUID", ColumnType::StringLike),
            ("VARCHAR", "VARCHAR", ColumnType::Varchar),
            ("VARIANT", "VARIANT", refused()),
        ];
        let conn = Connection::open_in_memory().unwrap();
        let listed: Vec<String> = conn
            .prepare("SELECT DISTINCT logical_type FROM duckdb_types() WHERE logical_type <> 'TYPE' ORDER BY 1")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let mut classified: Vec<&str> = types.iter().map(|(logical, _, _)| *logical).collect();
        classified.sort();
        assert_eq!(listed, classified, "DuckDB's types changed: classify the new ones");

        // Columns of the types that a declaration cannot name come from a query.
        let declared: Vec<String> = types
            .iter()
            .enumerate()
            .filter(|(_, (_, declared, _))| !declared.is_empty())
            .map(|(index, (_, declared, _))| format!("c{index} {declared}"))
            .collect();
        conn.execute_batch(&format!(
            "CREATE TABLE t ({}); CREATE TABLE u AS SELECT NULL AS null_column, (1, 'a') AS tuple_column;",
            declared.join(", ")
        ))
        .unwrap();
        let mut found = Vec::new();
        for name in ["t", "u"] {
            let mut table = Table {
                name: name.to_owned(),
                comment: None,
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_column_sets: Vec::new(),
                not_null: HashSet::new(),
                foreign_keys: Vec::new(),
            };
            read_columns(&conn, &mut table).unwrap();
            found.extend(table.columns);
        }
        let expected = types
            .iter()
            .filter(|(_, declared, _)| !declared.is_empty())
            .chain(types.iter().filter(|(logical, _, _)| matches!(*logical, "NULL" | "TUPLE")));
        assert_eq!(found.len(), types.len());
        for (column, (logical, _, expected)) in found.iter().zip(expected) {
            let refused_with_hint = matches!(&column.column_type, ColumnType::Unsupported { hint }
                if *hint != "this DuckDB type has no TypeDB mapping");
            match expected {
                ColumnType::Unsupported { .. } => {
                    assert!(refused_with_hint, "{logical} ({}): {:?}", column.raw_type, column.column_type)
                }
                expected => assert_eq!(&column.column_type, expected, "{logical} ({})", column.raw_type),
            }
        }
    }

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
