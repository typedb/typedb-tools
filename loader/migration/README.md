# Migrating DuckDB databases into TypeDB

`typedb loader --duckdb` maps the tables of a DuckDB file onto a TypeDB schema that you write, checks the data,
converts it to the server's import format and imports it. The DuckDB file is opened read-only. The server enforces
the schema's constraints at import.

```bash
# convert and import. The database must not exist yet, and --database defaults to the file's stem
typedb loader --duckdb data.duckdb --schema-file schema.tql --database mydb --address localhost:1729 --username admin

# check and convert, writing nothing
typedb loader --duckdb data.duckdb --schema-file schema.tql --dry-run

# write out/mydb.data.typedb without importing it (--overwrite replaces an existing file), and import it later
typedb loader --duckdb data.duckdb --schema-file schema.tql --database mydb --files-only --output-dir out/
typedb console --command="database import mydb schema.tql out/mydb.data.typedb"
```

## Staging other sources

The loader reads DuckDB files only, and DuckDB reads most other formats:

```sql
CREATE TABLE person (_iid UBIGINT, name VARCHAR, age INTEGER);
INSERT INTO person SELECT row_number() OVER (), name, age FROM 'people.csv';  -- or read_parquet(), read_json()

ATTACH 'app.db' AS src (TYPE sqlite);                                          -- or postgres, mysql
CREATE TABLE company AS SELECT id::UBIGINT AS _iid, name FROM src.company;
```

Copied tables carry no keys. Either create the tables with their keys and insert into them, or mark each role column
`@typedb references <table>` and number the referenced table's rows with `row_number() OVER ()`
([Keys](#keys-are-optional)). A non-negative integer primary key, declared as one, can serve as `_iid`, and the
foreign key columns then already hold `_iid` values.

## DuckDB versions

The loader reads the file with DuckDB, at the version it is built with. Which files that version opens, and what
changes between versions, is DuckDB's to define. A file it cannot open is refused with DuckDB's error. A file
written by a later DuckDB can be copied into a storage version the loader reads, with its tables, keys and comments:

```sql
ATTACH 'data.duckdb' AS src (READ_ONLY);
ATTACH 'for-loader.duckdb' AS dst (STORAGE_VERSION 'v2.0.0');
COPY FROM DATABASE src TO dst;
```

The loader loads no DuckDB extension. Generated columns are refused, as DuckDB computes them anew on every read.
Materialise them when staging.

## Mapping

The schema is a single `define` query. The loader matches by name and does not guess:

- A table maps to the entity or relation type of its name, a column to the attribute of its name. Names match
  exactly, or with `_` read as `-` (`country_code` matches `country-code`).
- A table or column without a counterpart in the schema is an error. `@typedb skip` leaves it out. A schema type
  without a source is a warning.
- Only tables of DuckDB's `main` schema are read. Views and other schemas are reported and left out.
- Labels that are TypeQL keywords (`from`, `end`, `in`, ...) are refused, as the server refuses them.

Every mapped table has a column `_iid UBIGINT`: the row's instance id, unique within its type, present, and below
2^63 ([Ids](#ids)).

- **An entity type is a table.** Its columns are the attributes it owns, and `NULL` is no ownership. A `LIST` column
  is a multi-valued attribute: repeated elements are one ownership, `NULL` elements none.
- **A relation type is a table** with one column per role, holding the players' `_iid`s. A `FOREIGN KEY` to the
  player table's `_iid`, or `@typedb references <table>`, names the player table, and the role is the one that the
  player's type plays in the relation. The other columns are the relation's attributes.
- **Several columns may bind one role**, one per player table for instance. A row's players are the distinct
  non-`NULL` values. When a player's type plays several roles of the relation, the table directive binds the columns.
- **A relation playing a role** is a column referencing the relation table's `_iid`.
- **Subtypes take tables of their own**, abstract types none. An instance belongs to one table, and a reference names
  one table, so a role open to subtypes takes a column per table. Subtypes inherit ownerships and roles.
- **Several tables may map to one type** with a directive. Their `_iid`s must differ.
- **A reference on an entity table** has no TypeDB form. Stage the relation as its own table and skip the column.
- **`_iid` is not data.** It is not queryable after the import. Keep the ids you query by as ordinary columns, such
  as `owns id @key`.

```sql
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL, nickname VARCHAR[]);
CREATE TABLE company (_iid UBIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE employment (
    _iid UBIGINT PRIMARY KEY,
    person_id UBIGINT NOT NULL REFERENCES person(_iid),
    company_id UBIGINT NOT NULL REFERENCES company(_iid),
    salary DECIMAL(12,2)
);
CREATE TABLE mentorship (_iid UBIGINT, mentor_id UBIGINT, student_id UBIGINT);  -- without keys
COMMENT ON TABLE mentorship IS '@typedb relation mentorship (mentor = mentor_id, student = student_id)';
COMMENT ON COLUMN mentorship.mentor_id IS '@typedb references person';
COMMENT ON COLUMN mentorship.student_id IS '@typedb references person';
```

```typeql
define
  attribute name, value string;
  attribute nickname, value string;
  attribute salary, value decimal;
  entity person, owns name @card(1), owns nickname @card(0..),
    plays employment:employee, plays mentorship:mentor, plays mentorship:student;
  entity company, owns name, plays employment:employer;
  relation employment, relates employee, relates employer, owns salary;
  relation mentorship, relates mentor, relates student;
```

Directives are DuckDB comments, kept in the file:

| Directive | On | Effect |
|---|---|---|
| `@typedb skip` | table, column | not loaded |
| `@typedb entity <label>` | table | maps the table to that entity type |
| `@typedb relation <label> (<role> = <column>, ...)` | table | maps the table to that relation type and binds the listed columns. The bindings are optional |
| `@typedb owns <label>` | column | maps the column to that attribute |
| `@typedb references <table>` | column | the column holds that table's `_iid`s, instead of a `FOREIGN KEY` |

## Type conversions

| DuckDB | TypeDB | Notes |
|---|---|---|
| `BOOLEAN` | `boolean` | |
| integer types | `integer` | must fit 64 bits. Also into `decimal`, and into `double` when the double holds the value exactly |
| `FLOAT`, `DOUBLE` | `double` | finite only. `-0.0` loads as `0.0` |
| `DECIMAL(p,s)` | `decimal` | exact. Scale at most 19, integer part within 64 bits. Into `double` only when the double, printed at the column's scale, reads back as the value, and no two distinct decimals of the attribute make one double |
| `VARCHAR`, `ENUM`, `UUID`, `JSON` | `string` | compared byte-wise whatever the collation: `'Bob'` and `'BOB'` are two values |
| `DATE` | `date` | finite, years -262143 to 262142 |
| `TIMESTAMP`, `_S`, `_MS`, `_NS` | `datetime` | exact to the nanosecond, finite |
| `TIMESTAMPTZ`, `TIMESTAMPTZ_NS` | `datetime-tz` | as `datetime`, in UTC: DuckDB stores the instant, not the zone |
| `INTERVAL` | `duration` | months, days and nanoseconds kept apart (`1 month` and `30 days` are two values). No negative parts, and a time part below 2^64 ns (584 years) |
| `BLOB`, `TIME`, `STRUCT`, `MAP`, `UNION`, `UHUGEINT`, fixed-size `ARRAY`, nested lists, others | | refused |

An attribute fed by a nanosecond column reads all its columns in nanoseconds, so their timestamps must fit years
1677 to 2262. Text is never parsed into values: a column of an incompatible type is refused, naming the DuckDB type
to stage it as.

## Keys are optional

The loader needs to know that each `_iid` is unique within its type and present, and that each role value is an
`_iid` of the referenced table. Declared keys (`PRIMARY KEY`, `UNIQUE` with `NOT NULL`, `FOREIGN KEY`) make DuckDB
guarantee this. Without them, the loader checks the data, and a role column names its table with
`@typedb references <table>`. Both give the same data file, and they can be mixed by table and by column. The
mapping summary says which are declared and which are checked.

Without a `FOREIGN KEY`, the `_iid`s of the referenced table must be dense in row order, `first, first + 1, ...`, as
`row_number() OVER ()` numbers the rows while DuckDB preserves insertion order (its default). One scan then proves
them unique, and every reference is checked against their range. Other ids of a referenced table are refused. The
ids of a table that no such column references may be anything: their uniqueness takes a GROUP BY, which spills to
disk on large data.

DuckDB keeps the index of every declared key in memory while rows are inserted, and cannot spill it. For data of
hundreds of millions of rows and more, stage without keys.

## What is checked

Before converting anything, the loader checks the source in three steps. Any error stops the run with nothing
written, and every error is reported, each data violation with a count and a few examples.

1. **Structure.** Every mapped table has an `_iid UBIGINT` column that is neither generated, a `FOREIGN KEY`, nor
   under a directive. Every mapped column has a supported type (see [Type conversions](#type-conversions)) and is not
   generated. Directives are well-formed and name columns and tables that exist.
2. **Mapping.** Every table and column has a counterpart in the schema, or is skipped; every role column names its
   players' table, and the role is unambiguous; value types are compatible; abstract types and roles have no tables
   or columns; ordered (list) ownerships and roles, and struct-valued attributes, are refused.
3. **Data**, with one DuckDB query per check:

| Check | Runs on | Unless |
|---|---|---|
| `_iid` unique within a type, across all of its tables | every type | its one table declares `PRIMARY KEY` or `UNIQUE` on `_iid`, or its ids are dense in row order |
| `_iid` below 2^63 | every table | |
| `_iid` present | every table | `_iid` is `PRIMARY KEY` or `NOT NULL` |
| every row fills a role | every relation table | |
| a role value is an `_iid` of the referenced table, whose ids are dense in row order | every role column | the column is a `FOREIGN KEY` |
| an integer fits 64 bits | `HUGEINT`, `UBIGINT` columns | |
| a decimal's integer part fits 64 bits | `DECIMAL` columns with over 18 integer digits | the attribute is `double` |
| a double holds the value exactly | into `double`: integers wider than 53 bits, decimals of over 15 digits | |
| distinct decimals stay distinct doubles | a `double` attribute fed by decimals of several scales | every decimal has at most 15 digits |
| a double is finite | `FLOAT`, `DOUBLE` columns | |
| a date is finite and within TypeDB's range | `DATE` columns | |
| a timestamp is finite and within TypeDB's range | `TIMESTAMP`, `_S`, `_MS`, `_NS`, `TIMESTAMPTZ` columns | |
| a timestamp fits nanoseconds (years 1677 to 2262) | microsecond and coarser columns of an attribute that also has a nanosecond column | |
| a nanosecond timestamp is finite | `TIMESTAMPTZ_NS` columns | |
| an interval has no negative part, and a time part below 2^64 ns | `INTERVAL` columns | |

The schema's constraints (`@card`, `@key`, `@unique`, `@regex`, `@values`, `@range`) are the server's to enforce at
import. Equal values are one attribute, and a player named twice in a role is one player.

## Ids

The server creates each instance with its `_iid` as its number within its type, and resolves players by type and
number, so ids need not start at 0 or be dense, unless undeclared references point at them. Values must be below
2^63, which leaves TypeDB room for instances created later. Renumber larger ones, such as hashes, and the columns
referencing them.

## Memory and disk

DuckDB gets half of the memory it detects (the machine's, or the container's limit) unless `--memory-limit` sets
another limit (`8GB`, `512MiB`, at least 512 MiB). It runs a thread per 64 MB of the limit, up to one per core, and
spills what does not fit. The conversion needs one to two GB more for its own buffers.

The output directory needs room for the data file and for DuckDB's spill. `--dry-run` spills into the system's
temporary directory (`TMPDIR`), as `.typedb-loader.<process id>.tmp`, which a killed dry run leaves behind. With
`--overwrite` the old file stays until the new one replaces it.

## The data file and the import

The file is the `typedb export` format: length-delimited `Migration.Item` protobuf messages, as zstd frames that each
carry a checksum, so a damaged or truncated file fails to decompress. It holds a header, every attribute (each
attribute's values in sorted order, which the server imports faster), every entity, every relation, and checksums.
The schema is not copied. Import the file with the schema file it was prepared with.

The file is `<database>.data.typedb` next to the DuckDB file, or in `--output-dir`. It is written under a
temporary name and renamed when complete, and never over the DuckDB file. An existing file is refused unless
`--overwrite` is given. That, an existing database, and a server that is unreachable or does not answer within 30 s are
all found before the checks start. One loader at a time prepares a
database in a directory: `.<database>.lock`, which stays, makes a second one fail at once, and the next run removes
what a run that died left behind.

The import starts once the file is complete, over a fresh connection. A successful import removes the file. A failed
import keeps it, and the loader prints the `typedb console` command that imports it.

Ctrl+C or SIGTERM stops the run. An interrupted conversion removes what it wrote. An interrupted import keeps the
data file, and the server discards the partial import. A second interrupt exits at once, and the next run removes
what it left.

| Exit code | Meaning |
|---|---|
| 0 | done |
| 1 | the import failed on the server |
| 2 | wrong arguments or paths, a run of the same database and directory in progress, or the checks failed |
| 3 | the server could not be reached |
| 4 | the conversion failed, such as on a full disk |
| 130, 143 | stopped by Ctrl+C (SIGINT), or by SIGTERM |
