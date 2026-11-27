## Distribution

Download from TypeDB Package Repository: https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-console+version:3.16.0-rc0


## New Features
- **Introduce database-based relational migration**
  Add relational import to the loader: loading data from an external database, currently DuckDB.
  
  DuckDB supports converting multiple data sources (CSV, Parquet, JSON, SQLite, Postgres, MySQL) into a unified, SQL-like database, which makes it a comfortable intermediate step for migrating from any external storage to TypeDB. As a result, the loader now supports using DuckDB databases to import data into a TypeDB server.
  
  `typedb loader --duckdb` loads the tables of a DuckDB database file into a new TypeDB database. You write the TypeDB schema -> the loader maps tables and columns onto it by name, checks the data, converts it to the server's import format, and imports it.
  
  ```bash
  typedb loader --duckdb data.duckdb --schema-file schema.tql --database mydb --address localhost:1729 --username admin
  typedb loader --duckdb data.duckdb --schema-file schema.tql --dry-run            # check and convert, write nothing
  typedb loader --duckdb data.duckdb --schema-file schema.tql --database mydb --files-only --output-dir out/
  ```
  
  Relational import needs the tables in a specific shape, which data is usually staged into with SQL first. In return, on very large data it loads up to 20x faster than a plain TypeDB export and import of the same data, and the data is validated before anything is loaded.
  
  Important requirements for the source database:
  1. A table per entity or relation type, each with an `_id UBIGINT` column: the row's id, unique within its type. It is how rows refer to each other, and it is not data.
  2. A relation table has a column per role, holding the players' `_id`s and naming the players' table with `@typedb references <table>`. A column references one table: a role played by several tables takes a column per table. `COMMENT ON` directives (`@typedb entity|relation|owns|references|skip`) rename or skip.
  3. No keys are needed: the loader checks ids and references itself, which requires the `_id`s of a referenced table to number its rows in order, as `row_number() OVER ()` numbers them. Declared `PRIMARY KEY` and `FOREIGN KEY` constraints are an alternative for smaller data, and lift that requirement.
  
  Important: since relational import is expected to be used for huge sizes of data, it compresses the data file into `zstd` frames. The importer must be ready to decode it (use the same minor versions of TypeDB software for this).
  
  
- **Introduce zstd compression for database export**
  `database export` accepts an optional compression argument for the data file: `uncompressed` (the default) or `zstd`, which compresses at level 3 with a content checksum. 
  ```
  database export my-db schema.tql data.typedb zstd
  ```
  
  `database import` detects compressed files automatically, so it needs no extra argument.
  
  
## Bugs Fixed

## Code Refactors
- **Simplify workspace referencing**
  
  Each TypeDB dependency's commit or tag is now written once, in its `git_override` in `MODULE.bazel`. 
  
  Before, refs were duplicated in `workspace_refs`' commit and tag dicts, with commented-out alternatives for switching between the two. 
  
  
  
## Other Improvements
- **Update TypeQL, TypeDB Driver and TypeDB Protocol for the vector value type**
  Build against the TypeQL, TypeDB Driver and TypeDB Protocol versions that introduce the vector value type.
  
  
    
