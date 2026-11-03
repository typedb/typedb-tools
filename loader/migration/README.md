# Loading DuckDB databases into TypeDB

`typedb loader --duckdb` maps the tables of a DuckDB file onto a TypeDB schema that you write, checks the data,
converts it to the server's import format and imports it. The DuckDB file is opened read-only. The server enforces
the schema's constraints at import.

```bash
# convert and import; the database must not exist yet, and --database defaults to the file's stem
typedb loader --duckdb data.duckdb --schema-file schema.tql --database mydb --address localhost:1729 --username admin

# check and convert, writing nothing
typedb loader --duckdb data.duckdb --schema-file schema.tql --dry-run

# write out/mydb.data.typedb without importing it (--overwrite replaces an existing file), and import it later
typedb loader --duckdb data.duckdb --schema-file schema.tql --database mydb --files-only --output-dir out/
typedb console --command="database import mydb schema.tql out/mydb.data.typedb"
```

## Staging other sources

The loader reads DuckDB files only; DuckDB reads most other formats:

```sql
CREATE TABLE person (_iid UBIGINT, name VARCHAR, age INTEGER);
INSERT INTO person SELECT row_number() OVER (), name, age FROM 'people.csv';  -- or read_parquet(), read_json()

ATTACH 'app.db' AS src (TYPE sqlite);                                          -- or postgres, mysql
CREATE TABLE company AS SELECT id::UBIGINT AS _iid, name FROM src.company;
```

Copied tables carry no keys. Either create the tables with their keys and insert into them, or mark each role column
`@typedb references <table>` and let the loader check what the keys would guarantee ([Keys](#keys-are-optional)).
A non-negative integer primary key can serve as `_iid`; the foreign key columns then already hold `_iid` values.

## DuckDB versions

The loader links DuckDB 2.0.0, pinned exactly, and reads files written by DuckDB 0.9 to 2.0 (tested: 0.9.2, 0.10.3,
1.0 to 1.5, 2.0). Directives need `COMMENT ON`, which arrived in DuckDB 0.10. A file written by a later DuckDB may use
a storage version that 2.0 cannot read, and fails to open: "created with a newer version of DuckDB". The later DuckDB
can write a copy that the loader reads, with its tables, keys and comments:

```sql
ATTACH 'data.duckdb' AS src (READ_ONLY);
ATTACH 'for-loader.duckdb' AS dst (STORAGE_VERSION 'v2.0.0');
COPY FROM DATABASE src TO dst;
```

The loader loads no DuckDB extension, so generated columns are computed without one: time zone conversions in UTC,
and a column that needs an extension fails to read. Materialise such columns when staging.

## Mapping

The schema is a single `define` query. The loader matches by name and does not guess:

- A table maps to the entity or relation type of its name, a column to the attribute of its name. Names match
  exactly, or with `_` read as `-` (`country_code` matches `country-code`).
- A table or column without a counterpart in the schema is an error; `@typedb skip` leaves it out. A schema type
  without a source is a warning.
- Only tables of DuckDB's `main` schema are read. Views and other schemas are reported and left out.
- Labels that are TypeQL keywords (`from`, `end`, `in`, ...) are refused, as the server refuses them.

Every mapped table has a column `_iid UBIGINT`: the row's instance id, unique within its type, present, and below
2^63 ([Ids](#ids)).

- **An entity type is a table.** Its columns are the attributes it owns; `NULL` is no ownership. A `LIST` column is
  a multi-valued attribute: repeated elements are one ownership, `NULL` elements none.
- **A relation type is a table** with one column per role, holding the players' `_iid`s. A `FOREIGN KEY` to the
  player table's `_iid`, or `@typedb references <table>`, names the player table, and the role is the one that the
  player's type plays in the relation. The other columns are the relation's attributes.
- **Several columns may bind one role**, one per player table for instance; a row's players are the distinct
  non-`NULL` values. When a player's type plays several roles of the relation, the table directive binds the columns.
- **A relation playing a role** is a column referencing the relation table's `_iid`.
- **Subtypes take tables of their own**, abstract types none. An instance belongs to one table, and a reference names
  one table: a role open to subtypes takes a column per table. Subtypes inherit ownerships and roles.
- **Several tables may map to one type** with a directive; their `_iid`s must differ.
- **A reference on an entity table** has no TypeDB form: stage the relation as its own table and skip the column.
- **`_iid` is not data**: it is not queryable after the import. Keep the ids you query by as ordinary columns, such
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
| `@typedb relation <label> (<role> = <column>, ...)` | table | maps the table to that relation type and binds the listed columns; the bindings are optional |
| `@typedb owns <label>` | column | maps the column to that attribute |
| `@typedb references <table>` | column | the column holds that table's `_iid`s (instead of a `FOREIGN KEY`) |

## Type conversions

| DuckDB | TypeDB | Notes |
|---|---|---|
| `BOOLEAN` | `boolean` | |
| integer types | `integer` | must fit 64 bits; also into `decimal`, and into `double` when the double holds the value exactly |
| `FLOAT`, `DOUBLE` | `double` | finite only; `-0.0` loads as `0.0` |
| `DECIMAL(p,s)` | `decimal` | exact; scale at most 19, integer part within 64 bits. Into `double` only when the double, printed at the column's scale, reads back as the value, and no two distinct decimals of the attribute make one double |
| `VARCHAR`, `ENUM`, `UUID`, `JSON` | `string` | compared byte-wise whatever the collation: `'Bob'` and `'BOB'` are two values |
| `DATE` | `date` | finite, years -262143 to 262142 |
| `TIMESTAMP`, `_S`, `_MS`, `_NS` | `datetime` | exact to the nanosecond, finite |
| `TIMESTAMPTZ`, `TIMESTAMPTZ_NS` | `datetime-tz` | as `datetime`, in UTC: DuckDB stores the instant, not the zone |
| `INTERVAL` | `duration` | months, days and nanoseconds kept apart (`1 month` and `30 days` are two values); no negative parts; time part below 2^64 ns (584 years) |
| `BLOB`, `TIME`, `STRUCT`, `MAP`, `UNION`, `UHUGEINT`, fixed-size `ARRAY`, nested lists, others | | refused, with a hint |

An attribute fed by a nanosecond column reads all its columns in nanoseconds, so their timestamps must fit years
1677 to 2262. Text is never parsed into values: a column of an incompatible type is refused, naming the DuckDB type
to stage it as.

## Keys are optional

The loader needs to know that each `_iid` is unique within its type and present, and that each role value is an
`_iid` of the referenced table. Declared keys (`PRIMARY KEY`, `UNIQUE` with `NOT NULL`, `FOREIGN KEY`) make DuckDB
guarantee this. Without them, the loader checks every row, and a role column names its table with
`@typedb references <table>`. Both give the same data file, and they can be mixed by table and by column. The
mapping summary says which are declared and which are checked.

The checks are fastest when a table's ids run in row order with a constant step, as `row_number() OVER ()` numbers
them: a scan each then proves the ids unique and the references valid, in seconds per billion rows. Other ids take a
GROUP BY and an ANTI JOIN, which spill: about 14 minutes for 10 billion rows on 10 cores.

DuckDB keeps the index of every declared key in memory while rows are inserted, and cannot spill it: about 70 bytes
per row for random keys. The loader warns above 100 million keyed rows. For data that size, stage without keys.

## What is checked

Before converting anything, the loader checks the structure (the `_iid` column, role columns, column types,
directives), the mapping against the schema (types, attributes and roles exist, with compatible value types), and
the data, one DuckDB query per check: values TypeDB cannot hold (above), relation rows that fill no role, and, where
no key declares them, duplicate or missing `_iid`s and role values that are no `_iid` of the referenced table. Each
violation is reported with a count and a few examples. Any error stops the run with nothing written.

The schema's constraints (`@card`, `@key`, `@unique`, `@regex`, `@values`, `@range`) are the server's to enforce at
import. Equal values are one attribute, and a player named twice in a role is one player.

## Ids

The server creates each instance with its `_iid` as its number within its type, and resolves players by type and
number, so sparse ids and source keys are fine. Values must be below 2^63, which leaves TypeDB room for instances
created later; renumber larger ones, such as hashes, and the columns referencing them.

## Memory and disk

DuckDB gets half of the memory it detects (the machine's, or the container's limit) unless `--memory-limit` sets
another limit (`8GB`, `512MiB`, at least 256 MiB). It runs a thread per 64 MB of the limit, up to one per core, and
spills what does not fit. The conversion needs one to two GB more for its own buffers.

The output directory needs room for the data file (31 bytes per row on the benchmark's narrow tables) and for
DuckDB's spill. `--dry-run` spills into the system's temporary directory (`TMPDIR`). With `--overwrite` the old file
stays until the new one replaces it.

A run prints, every 30 seconds while rows are converted, the table or attribute, the rows so far and the bytes
encoded, and a line every minute while it checks or DuckDB sorts.

## The data file and the import

The file is the `typedb export` format: length-delimited `Migration.Item` protobuf messages, as zstd frames that each
carry a checksum, so a damaged or truncated file fails to decompress. It holds a header, every attribute (each
attribute's values in sorted order, which the server imports faster), every entity, every relation, and checksums.
The schema is not copied: import the file with the schema file it was prepared with.

The file is `<database>.data.typedb` next to the DuckDB file, or in `--output-dir`. It is written under a
temporary name and renamed when complete. An existing file is refused unless `--overwrite` is given; that, an
unreachable server and an existing database are all found before the checks start. One loader at a time prepares a
database in a directory: `.<database>.lock` makes a second one fail at once, and the next run removes what a run
that died left behind.

The import starts once the file is complete. If the connection was lost during the conversion, the loader reconnects
(12 attempts, 10 s apart). If an import attempt loses its connection, the server may still complete it, so a
database that is being imported is waited for, and an existing database ends the run with exit code 3 and the file
kept, as the loader cannot confirm that the import was its own. A successful import removes the file. A failed or
interrupted import (Ctrl+C, SIGTERM, SIGHUP) keeps it, and the loader prints the `typedb console` command that
imports it. An interrupted conversion removes what it wrote.
