# Loading DuckDB databases into TypeDB

`typedb loader --duckdb` migrates a DuckDB database into TypeDB: it maps the tables onto a
hand-written TypeDB schema, converts the data to the server's import format and imports
it, in one run. The schema's constraints are enforced by the server at import. The DuckDB
file is opened read-only.

```bash
# load into a server
typedb loader --duckdb my-data.duckdb --schema-file my-schema.tql \
  --database mydb --address localhost:1729 --username admin

# check the mapping and convert without writing or loading anything
typedb loader --duckdb my-data.duckdb --schema-file my-schema.tql --dry-run

# write the data file instead of loading (offline; --overwrite replaces an existing file)
typedb loader --duckdb my-data.duckdb --schema-file my-schema.tql --database mydb --files-only --output-dir out/
# → out/mydb.data.typedb, importable together with the original schema file:
#   typedb console --command="database import mydb my-schema.tql out/mydb.data.typedb"
```

The file is opened as a DuckDB database whatever its extension. The
target database is created by the import and must not exist yet; `--database` defaults to
the file's stem.

## Starting from CSV, Parquet, SQLite, Postgres, MySQL, ...

The loader reads one format, DuckDB, and DuckDB reads most others, so any source is a
few SQL statements away. The staged `.duckdb` file is then a queryable record of exactly
what gets imported:

```sql
-- CSV / Parquet / JSON: declare the table (ids and keys), then load into it
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL, age INTEGER);
INSERT INTO person FROM 'people.csv';          -- read_parquet(...) / read_json(...) likewise

-- SQLite file (live Postgres / MySQL likewise: INSTALL postgres; ATTACH 'dbname=... host=...')
INSTALL sqlite; LOAD sqlite;
ATTACH 'app.db' AS src (TYPE sqlite);
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, id BIGINT NOT NULL UNIQUE, name VARCHAR);
INSERT INTO person SELECT id, id, name FROM src.person;
CREATE TABLE company (_iid UBIGINT PRIMARY KEY, name VARCHAR);
INSERT INTO company SELECT id, name FROM src.company;
CREATE TABLE employment (
    _iid UBIGINT PRIMARY KEY,
    person_id UBIGINT NOT NULL REFERENCES person(_iid),
    company_id UBIGINT NOT NULL REFERENCES company(_iid)
);
INSERT INTO employment SELECT row_number() OVER (), person_id, company_id FROM src.employment;
```

File readers and database scanners do not carry constraints over: a copied table arrives
without its primary and foreign keys, which the loader derives the mapping from. Declare
them in DuckDB by creating each table with its keys and inserting into it, as above. When
the source's primary key is a non-negative integer, use it as `_iid` too: the source's FK
columns then already hold `_iid` values.

## The TypeDB schema is the source of truth

You write the TypeDB schema (`.tql`, a single `define` query). The loader maps tables and
columns onto it by name and refuses to guess:

- Anything in **DuckDB without a schema counterpart is an error**: an unmapped table or
  column fails the run, unless marked `@typedb skip`.
- Anything in the **schema without a DuckDB counterpart is a warning**: those types
  produce no instances.

Table and column names match exactly (case included), or with `_` read as `-`
(`country_code` matches `country-code`); names given in directives are taken as written.
Only tables of DuckDB's `main` schema are read: views and other schemas
are reported and skipped. Subtypes inherit ownerships and roles as on the server; abstract
types take no table.

## Modelling

Every mapped table identifies its rows by a column `_iid UBIGINT` declared `NOT NULL
UNIQUE` or `PRIMARY KEY` on its own (see [Ids](#ids)), and foreign keys reference it. The
examples below leave the attribute declarations out: every column name is an attribute of
the matching name, e.g. `attribute name value string;`.

**An entity type is a table**; its columns are the attributes it owns. `NULL` is no
ownership.

```sql
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL, age INTEGER);
```
```typeql
entity person, owns name @card(1), owns age;
```

**A multi-valued attribute is a LIST column.** Ownerships are a set: repeated elements are
one attribute, `NULL` elements are nothing, and order is lost.

```sql
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, nickname VARCHAR[]);
```
```typeql
entity person, owns nickname @card(0..);
```

**A relation type is a table too**, with one FK column per role. The FK references the
player table's `_iid` (composite foreign keys are not recognised: give the referenced
table an `_iid`), and the role is inferred from the role the player's type plays. Columns
that are not FKs are the relation's own attributes.

```sql
CREATE TABLE employment (
    _iid UBIGINT PRIMARY KEY,
    person_id UBIGINT NOT NULL REFERENCES person(_iid),
    company_id UBIGINT NOT NULL REFERENCES company(_iid),
    salary DECIMAL(12,2)
);
```
```typeql
entity person, plays employment:employee;
entity company, plays employment:employer;
relation employment, relates employee, relates employer, owns salary;
```

**Several columns may bind one role**, one per player table for instance. A row's
players in the role are the distinct non-`NULL` values of those columns, within the role's
`@card`, which the server enforces.

```sql
CREATE TABLE ownership (
    _iid UBIGINT PRIMARY KEY,
    person_id UBIGINT REFERENCES person(_iid),      -- owner: a person ...
    company_id UBIGINT REFERENCES company(_iid),    -- ... or a company (one of the two)
    car_id UBIGINT NOT NULL REFERENCES car(_iid)
);
```
```typeql
entity person, plays ownership:owner;
entity company, plays ownership:owner;
entity car, plays ownership:owned;
relation ownership, relates owner, relates owned;
```

**Two roles played by one type** cannot be told apart by inference: bind the columns
with a table directive.

```sql
CREATE TABLE mentorship (
    _iid UBIGINT PRIMARY KEY,
    mentor_id UBIGINT NOT NULL REFERENCES person(_iid),
    student_id UBIGINT NOT NULL REFERENCES person(_iid)
);
COMMENT ON TABLE mentorship IS '@typedb relation mentorship (mentor = mentor_id, student = student_id)';
```
```typeql
entity person, plays mentorship:mentor, plays mentorship:student;
relation mentorship, relates mentor, relates student;
```

**A relation playing a role** is an FK to the relation table's `_iid`.

```sql
CREATE TABLE approval (
    _iid UBIGINT PRIMARY KEY,
    employment_id UBIGINT NOT NULL REFERENCES employment(_iid),
    manager_id UBIGINT NOT NULL REFERENCES person(_iid)
);
```
```typeql
relation employment, relates employee, relates employer, plays approval:subject;
relation approval, relates subject, relates approver;
entity person, plays employment:employee, plays approval:approver;
```

**Subtypes take tables of their own**, abstract types none. An instance belongs to one
table, so an `employee` row is not also a `person` row, and its table carries the inherited
columns. Inherited ownerships and roles are mapped like declared ones. An FK to
`person(_iid)` names persons only; a role open to employees too takes a second column
referencing `employee(_iid)`.

```sql
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE employee (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL, salary DECIMAL(12,2));
```
```typeql
entity person, owns name @card(1);
entity employee sub person, owns salary;
```

**Several tables may map to one type**, for instance when staged from several sources.
Their `_iid` values may overlap: an `_iid` identifies a row within its table, and an FK
names the table.

```sql
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE contractor (_iid UBIGINT PRIMARY KEY, full_name VARCHAR);
COMMENT ON TABLE contractor IS '@typedb entity person';
COMMENT ON COLUMN contractor.full_name IS '@typedb owns name';
```

**Names that differ from the schema's** are mapped with directives, stored as DuckDB
comments inside the `.duckdb` file:

```sql
COMMENT ON TABLE staging_people IS '@typedb entity person';
COMMENT ON TABLE works_for      IS '@typedb relation employment (employee = person_id, employer = company_id)';
COMMENT ON TABLE scratch        IS '@typedb skip';
COMMENT ON COLUMN people.full_name IS '@typedb owns name';
COMMENT ON COLUMN people.photo     IS '@typedb skip';
```

**An FK column on an entity table has no TypeDB form** and is rejected: a relation is a
table of its own. Stage it, with its keys declared up front (DuckDB cannot add foreign
keys to an existing table), and skip the original column:

```sql
CREATE TABLE management (
    _iid UBIGINT PRIMARY KEY,
    manager_id UBIGINT NOT NULL REFERENCES person(_iid),
    report_id UBIGINT NOT NULL REFERENCES person(_iid)
);
INSERT INTO management
    SELECT row_number() OVER (), manager_id, _iid FROM person WHERE manager_id IS NOT NULL;
COMMENT ON TABLE management IS '@typedb relation management (manager = manager_id, report = report_id)';
COMMENT ON COLUMN person.manager_id IS '@typedb skip';
```

**Business ids** stay data: `_iid` is consumed by the loader and is not queryable after
the import. Keep ids you query by as ordinary columns, ideally `@key`:

```sql
CREATE TABLE person (
    _iid UBIGINT PRIMARY KEY,      -- instance id: consumed by the loader
    id BIGINT NOT NULL UNIQUE,     -- business id: owns id @key in the schema
    ...
);
```

For data without natural 64-bit ids, `row_number() OVER ()` at staging time is a fine
`_iid`, as above.

## Type conversions

| DuckDB | TypeDB value | Notes |
|---|---|---|
| `BOOLEAN` | `boolean` | |
| integer types | `integer` | `UBIGINT`/`HUGEINT` values must fit 64 bits; also accepted into `decimal` (exactly) and `double` (nearest double, as SQL converts) |
| `FLOAT`/`DOUBLE` | `double` | finite values only; `FLOAT` widens exactly; `-0.0` is loaded as `0.0`, since DuckDB does not keep the two apart |
| `DECIMAL(w,s)` | `decimal` | exact; `s ≤ 19`, integer part within 64 bits; also accepted into `double` (nearest double) |
| `VARCHAR`, `ENUM`, `UUID`, `JSON` | `string` | compared byte-wise, as in TypeDB, whatever the column's collation: `'Bob'` and `'BOB'` are distinct values |
| `DATE` | `date` | finite, within TypeDB's range (years -262143 to 262142) |
| `TIMESTAMP[_S/_MS/_NS]` | `datetime` | exact to the nanosecond; finite, within TypeDB's range. The columns of one attribute are read together in one type, so beside a `TIMESTAMP_NS` column, other timestamps must fit its range (years 1677 to 2262) |
| `TIMESTAMPTZ` | `datetime-tz` | always UTC: DuckDB stores the instant, not the zone; keep the zone in a separate column if it is data |
| `INTERVAL` | `duration` | months, days and nanoseconds map one to one, so `1 month` and `30 days` are distinct durations (DuckDB compares them equal); no negative components |
| `BLOB`, `TIME`, `STRUCT`, `MAP`, `UNION` | — | rejected with a restructuring hint |
| nested lists, other types | — | rejected |

## What is checked, and what is the server's

Before anything is written, the loader checks the structure (the `_iid` contract, FKs
referencing `_iid`, supported column types, well-formed directives) and the mapping (every
mapped type, attribute and role exists in the schema, with the right kind and a compatible
value type). It then checks, with one DuckDB query per column, what would make the data
file invalid: values TypeDB cannot hold (see the table above) and relation rows filling no
role, since a relation cannot exist without players. Each violation is reported with its
count and the first `_iid`s, so a dry run finds them all in minutes, not after hours of
conversion. Any error stops the run with nothing written; warnings are printed and the run
proceeds.

Everything the schema constrains is enforced by the server at import, as for any other
data: `@regex`, `@values` and `@range` as each attribute is created, which the import does
first; `@card` of ownerships, roles and `plays`, `@key` and `@unique` when the import
completes. Equal values are one attribute, and a player named twice in a role is one
player. A relation table without a role column fails validation.

## Ids

`_iid` identifies a row within its table only, so sparse ids and source keys are fine.
The loader numbers the instances of each type 0, 1, 2, ... in file order, table after
table and by `_iid` within a table. Entities, relations and role players are named by
their type and that number, attributes by their type and value. The file also carries
each item's id as a string (`person:0`, `name:Alice`) for an importer that resolves ids by
lookup; both name the same item.

## Output format

The data file is the `typedb export` format: length-delimited protobuf `Migration.Item`
messages, in the order `database import` expects: a header stating the file's guarantees
(the item order, unique attributes, players and owned attributes named as above), every
attribute, every entity, every relation, then checksums. The schema is not copied: import
the data file with the schema file it was prepared against.

Tables are written in name order, entity tables first. A relation that plays a role may
come after the relation it plays in; the import links such players once every relation is
read. A one-shot load stages the data file as `<database>.data.typedb` next to the DuckDB
file, or in `--output-dir`, and removes it once the import has succeeded; `--files-only`
writes it without importing. Either way an existing file is refused unless `--overwrite` is
passed. The file is written under a temporary name and renamed on completion, and an
interrupted conversion (Ctrl+C or SIGTERM) removes it. The import starts once the file is
complete; if the connection opened at the start did not survive the conversion, the loader
reconnects (up to twelve attempts, ten seconds apart) and starts the import over, and a
retry that finds the database already created reports that the earlier attempt completed.
If the import still fails, or the run is interrupted after the file is complete, the file is
kept and the loader prints the `typedb console` command that imports it, so the conversion
need not be repeated.
