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

use crate::sql::{self, ROWID_COLUMN, duckdb_type};

const DUCKDB_DEFAULT_DECIMAL_PRECISION: u8 = 18;
const DUCKDB_DEFAULT_DECIMAL_SCALE: u8 = 3;

#[derive(Debug)]
pub(crate) struct Catalog {
    pub(crate) tables: Vec<Table>,
    pub(crate) ignored_tables_and_views: Vec<String>,
}

impl Catalog {
    pub(crate) fn read(connection: &Connection) -> Result<Catalog, String> {
        let mut tables = read_main_tables(connection).map_err(|err| format!("failed to list tables: {err}"))?;
        for table in &mut tables {
            read_columns(connection, table)
                .and_then(|()| read_constraints(connection, table))
                .map_err(|err| format!("failed to read table '{}': {err}", table.name))?;
        }
        let ignored_tables_and_views = read_ignored_tables_and_views(connection)
            .map_err(|err| format!("failed to list schemas and views: {err}"))?;
        Ok(Catalog { tables, ignored_tables_and_views })
    }
}

#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) comment: Option<String>,
    pub(crate) columns: Vec<Column>,
    pub(crate) primary_key: Vec<String>,
    pub(crate) unique_column_sets: Vec<Vec<String>>,
    pub(crate) not_null_columns: HashSet<String>,
    pub(crate) foreign_keys: Vec<ForeignKey>,
}

impl Table {
    pub(crate) fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub(crate) fn has_rowid_column(&self) -> bool {
        self.columns.iter().any(|column| column.name.eq_ignore_ascii_case(ROWID_COLUMN))
    }

    pub(crate) fn foreign_key_of(&self, column: &str) -> Option<&ForeignKey> {
        self.foreign_keys.iter().find(|foreign_key| foreign_key.columns.iter().any(|name| name == column))
    }

    // Constraints keep a column's name as the constraint spelled it, and DuckDB resolves names case-insensitively.
    fn declared_column_name(&self, name: String) -> String {
        let declared = self.columns.iter().find(|column| column.name.eq_ignore_ascii_case(&name));
        declared.map_or(name, |column| column.name.clone())
    }

    fn without_columns(name: String, comment: Option<String>) -> Table {
        Table {
            name,
            comment,
            columns: Vec::new(),
            primary_key: Vec::new(),
            unique_column_sets: Vec::new(),
            not_null_columns: HashSet::new(),
            foreign_keys: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) duckdb_type: String,
    pub(crate) comment: Option<String>,
    pub(crate) is_generated: bool,
}

#[derive(Debug)]
pub(crate) struct ForeignKey {
    pub(crate) columns: Vec<String>,
    pub(crate) referenced_table: String,
    pub(crate) referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnType {
    Boolean,
    Integer { bits: u8, is_signed: bool },
    Double,
    Decimal { precision: u8, scale: u8 },
    Varchar,
    ReadAsVarchar,
    Date,
    Timestamp { has_nanoseconds: bool },
    TimestampTz { has_nanoseconds: bool },
    Interval,
    List(Box<ColumnType>),
    Unsupported { hint: &'static str },
}

impl ColumnType {
    pub(crate) const INSTANCE_ID: ColumnType = ColumnType::Integer { bits: 64, is_signed: false };

    pub(crate) fn has_nanoseconds(&self) -> bool {
        matches!(
            self,
            ColumnType::Timestamp { has_nanoseconds: true } | ColumnType::TimestampTz { has_nanoseconds: true }
        )
    }

    fn parse(duckdb_type: &str) -> ColumnType {
        let upper = duckdb_type.to_uppercase();
        if let Some(element) = upper.strip_suffix(duckdb_type::LIST_SUFFIX) {
            return ColumnType::List(Box::new(ColumnType::parse(element)));
        }
        let integer = |bits, is_signed| ColumnType::Integer { bits, is_signed };
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
            "UUID" | "JSON" => ColumnType::ReadAsVarchar,
            "DATE" => ColumnType::Date,
            "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" => ColumnType::Timestamp { has_nanoseconds: false },
            "TIMESTAMP_NS" => ColumnType::Timestamp { has_nanoseconds: true },
            "TIMESTAMP WITH TIME ZONE" => ColumnType::TimestampTz { has_nanoseconds: false },
            "TIMESTAMPTZ_NS" => ColumnType::TimestampTz { has_nanoseconds: true },
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
            _ if upper.ends_with(']') => unsupported("cast the fixed-size ARRAY to a LIST, as `column::INTEGER[]`"),
            _ if upper.starts_with("DECIMAL") => {
                let (precision, scale) = decimal_precision_and_scale(&upper);
                ColumnType::Decimal { precision, scale }
            }
            _ if upper.starts_with("ENUM") => ColumnType::ReadAsVarchar,
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

fn decimal_precision_and_scale(upper: &str) -> (u8, u8) {
    let arguments = upper.trim_end_matches(')').split_once('(').map(|(_, arguments)| arguments).unwrap_or("");
    let mut parts = arguments.split(',');
    let mut next = || parts.next().and_then(|part| part.trim().parse().ok());
    (next().unwrap_or(DUCKDB_DEFAULT_DECIMAL_PRECISION), next().unwrap_or(DUCKDB_DEFAULT_DECIMAL_SCALE))
}

fn read_main_tables(connection: &Connection) -> duckdb::Result<Vec<Table>> {
    let mut statement = connection.prepare(sql::catalog::MAIN_TABLES)?;
    let tables = statement.query_map([], |row| Ok(Table::without_columns(row.get(0)?, row.get(1)?)))?;
    tables.collect()
}

fn read_columns(connection: &Connection, table: &mut Table) -> duckdb::Result<()> {
    let mut statement = connection.prepare(sql::catalog::TABLE_COLUMNS)?;
    let mut rows = statement.query([&table.name])?;
    while let Some(row) = rows.next()? {
        let duckdb_type: String = row.get(1)?;
        table.columns.push(Column {
            name: row.get(0)?,
            column_type: ColumnType::parse(&duckdb_type),
            duckdb_type,
            comment: row.get(2)?,
            is_generated: row.get(3)?,
        });
    }
    Ok(())
}

fn read_constraints(connection: &Connection, table: &mut Table) -> duckdb::Result<()> {
    let mut statement = connection.prepare(sql::catalog::TABLE_CONSTRAINTS)?;
    let mut rows = statement.query([&table.name])?;
    while let Some(row) = rows.next()? {
        let constraint_type: String = row.get(0)?;
        let columns = text_list(row.get_ref(1)?).into_iter().map(|name| table.declared_column_name(name)).collect();
        match constraint_type.as_str() {
            "PRIMARY KEY" => table.primary_key = columns,
            "UNIQUE" => table.unique_column_sets.push(columns),
            "NOT NULL" => table.not_null_columns.extend(columns),
            "FOREIGN KEY" => {
                let referenced_table = row.get(2)?;
                let referenced_columns = text_list(row.get_ref(3)?);
                table.foreign_keys.push(ForeignKey { columns, referenced_table, referenced_columns });
            }
            _ => {}
        }
    }
    Ok(())
}

fn read_ignored_tables_and_views(connection: &Connection) -> duckdb::Result<Vec<String>> {
    let mut statement = connection.prepare(sql::catalog::IGNORED_TABLES_AND_VIEWS)?;
    statement.query_map([], |row| row.get(0))?.collect()
}

fn text_list(cell: ValueRef<'_>) -> Vec<String> {
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

#[cfg(test)]
mod test {
    use super::*;

    // A DuckDB upgrade that adds or renames a type fails here until the loader classifies it.
    #[test]
    fn every_duckdb_type_is_classified() {
        let refused = || ColumnType::Unsupported { hint: "" };
        let integer = |bits, is_signed| ColumnType::Integer { bits, is_signed };
        let timestamp = |has_nanoseconds| ColumnType::Timestamp { has_nanoseconds };
        let timestamp_tz = |has_nanoseconds| ColumnType::TimestampTz { has_nanoseconds };
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
            ("ENUM", "ENUM('a')", ColumnType::ReadAsVarchar),
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
            ("TIMESTAMP", "TIMESTAMP", timestamp(false)),
            ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", timestamp_tz(false)),
            ("TIMESTAMP_MS", "TIMESTAMP_MS", timestamp(false)),
            ("TIMESTAMP_NS", "TIMESTAMP_NS", timestamp(true)),
            ("TIMESTAMP_S", "TIMESTAMP_S", timestamp(false)),
            ("TIMESTAMPTZ_NS", "TIMESTAMPTZ_NS", timestamp_tz(true)),
            ("TINYINT", "TINYINT", integer(8, true)),
            ("TUPLE", "", refused()),
            ("UBIGINT", "UBIGINT", integer(64, false)),
            ("UHUGEINT", "UHUGEINT", refused()),
            ("UINTEGER", "UINTEGER", integer(32, false)),
            ("UNION", "UNION(a INTEGER)", refused()),
            ("USMALLINT", "USMALLINT", integer(16, false)),
            ("UTINYINT", "UTINYINT", integer(8, false)),
            ("UUID", "UUID", ColumnType::ReadAsVarchar),
            ("VARCHAR", "VARCHAR", ColumnType::Varchar),
            ("VARIANT", "VARIANT", refused()),
        ];
        let connection = Connection::open_in_memory().unwrap();
        let listed: Vec<String> = connection
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
        connection
            .execute_batch(&format!(
                "CREATE TABLE t ({}); CREATE TABLE u AS SELECT NULL AS null_column, (1, 'a') AS tuple_column;",
                declared.join(", ")
            ))
            .unwrap();
        let mut found = Vec::new();
        for name in ["t", "u"] {
            let mut table = Table::without_columns(name.to_owned(), None);
            read_columns(&connection, &mut table).unwrap();
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
                    assert!(refused_with_hint, "{logical} ({}): {:?}", column.duckdb_type, column.column_type)
                }
                expected => assert_eq!(&column.column_type, expected, "{logical} ({})", column.duckdb_type),
            }
        }
    }

    #[test]
    fn decimal_precision_and_scale_default_to_duckdb_values() {
        assert_eq!(decimal_precision_and_scale("DECIMAL(38,2)"), (38, 2));
        assert_eq!(decimal_precision_and_scale("DECIMAL(10, 3)"), (10, 3));
        assert_eq!(decimal_precision_and_scale("DECIMAL"), (18, 3));
    }

    #[test]
    fn fixed_size_arrays_are_refused_whatever_their_elements() {
        for duckdb_type in ["INTEGER[2]", "DECIMAL(10,2)[2]", "ENUM('sad', 'ok')[2]"] {
            assert!(matches!(ColumnType::parse(duckdb_type), ColumnType::Unsupported { .. }), "{duckdb_type}");
        }
    }
}
