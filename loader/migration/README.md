# Loading DuckDB databases into TypeDB

`typedb loader --db` loads a DuckDB database into TypeDB: the data is validated against
a hand-written TypeDB schema and the schema's constraints, converted to the server's
import format, and imported, in one run. Validation comes first: the DuckDB structure,
the mapping against the schema, and the data itself are all checked before anything is
written or sent. The DuckDB file is opened read-only and never modified.

```bash
# load into a server
typedb loader --db my-data.duckdb --schema-file my-schema.tql \
  --database mydb --address localhost:1729 --username admin

# validate only
typedb loader --db my-data.duckdb --schema-file my-schema.tql --dry-run

# write the data file instead of loading (offline)
typedb loader --db my-data.duckdb --schema-file my-schema.tql \
  --database mydb --files-only out/
# → out/mydb.data.typedb, importable together with the original schema file:
#   typedb console --command="database import mydb my-schema.tql out/mydb.data.typedb"
```

The source kind is recognised from the file extension (`.duckdb`, `.ddb`, `.db`). The
target database is created by the import and must not already exist.

## Starting from CSV, Parquet, SQLite, Postgres, MySQL, ...

The loader reads exactly one database file format: DuckDB. DuckDB itself reads
most other formats, so any source is one or two SQL statements away, and the staged
`.duckdb` file is a queryable record of exactly what will be imported:

```sql
-- CSV / Parquet / JSON: declare the schema (so the id contract and keys exist), then load
CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL, age INTEGER);
INSERT INTO person FROM 'people.csv';          -- read_parquet(...) / read_json(...) likewise

-- SQLite file
INSTALL sqlite; LOAD sqlite;
ATTACH 'app.db' AS src (TYPE sqlite);
CREATE TABLE person AS FROM src.person;

-- live Postgres / MySQL
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=mydb host=... user=...' AS src (TYPE postgres);
CREATE TABLE person AS FROM src.person;
```

Note that file readers and database scanners do not carry constraints over: a table
staged from CSV or copied from SQLite/Postgres arrives without its primary and foreign
keys. Declare them in DuckDB: create the table with its keys first and `INSERT ...
FROM`, as above, or use `ALTER TABLE` afterwards. The loader derives the
entity/relation mapping from these constraints, and its structure validation reports any
that are missing. The staging step is also where `@typedb` directives are attached.

The loader stays single-dialect by design: all sources funnel through DuckDB, so its
catalog reading, type mapping and validation SQL stay exact instead of approximating
several source systems.

## The TypeDB schema is the source of truth

You write the TypeDB schema (`.tql`, a single `define` query). The loader maps tables
and columns onto it and refuses to guess:

- Anything in **DuckDB without a schema counterpart is an error**: an unmapped table or
  column fails the run, unless explicitly marked `@typedb skip`.
- Anything in the **schema without a DuckDB counterpart is a warning**: those types and
  ownerships simply produce no instances.

Subtyping resolves as the server would: subtypes inherit ownerships, roles and `plays`;
`plays` declared against a supertype relation covers its subtypes; `relates <role> as
<parent-role>` replaces the specialised role. Abstract types cannot be mapped to tables.

## Mapping conventions

Well-shaped data needs no configuration:

| DuckDB | TypeDB |
|---|---|
| table `person` | entity type `person` (matched by name; `snake_case` also matches `snake-case`) |
| table matching a relation type + FK columns | relation type; each FK column binds to the role its target's type plays (several columns may bind one role, e.g. one per player table) |
| column `age` | `owns age` (same name matching) |
| `LIST` column (`VARCHAR[]`) | multiple ownerships of one attribute type |
| FK column on an entity table | rejected: relations are tables (see below) |
| `NULL` | no ownership / no role player |
| `ENUM` / `UUID` / `JSON` column | string attribute (cast automatically) |

## Instance ids

Every mapped table must carry its instances' TypeDB ids in a column named `_iid`,
declared as

```sql
_iid UBIGINT NOT NULL UNIQUE   -- or: _iid UBIGINT PRIMARY KEY
```

The loader validates the declaration (name, type, NOT NULL, and a UNIQUE or PRIMARY
KEY constraint) and refuses to run without it; the constraints themselves are then
enforced by DuckDB. Tables mapped to the same type share its ids, so their `_iid` values
must not overlap, which the loader checks. Foreign keys that bind role players must
reference the target table's `_iid`.

The output carries ids in TypeDB's own storage format, so that the importer can create
instances under them directly instead of keeping a map from exported to new ids. (Today's
importer still keeps that map and treats the ids as opaque.) An entity or relation id is
11 bytes:

```
[kind prefix: 1 byte][type id: 2 bytes, big-endian][object id: 8 bytes, big-endian]
 0x1E entity          position of the type in       the table's _iid value
 0x1F relation        schema definition order,
                      counted per kind
```

The `_iid` column supplies the object id verbatim; the loader derives the prefix from the
mapped type's kind, and the type id from the schema, which the import defines in a fresh
database. Attribute ids are derived from the type and the value as TypeDB derives them:
strings over 16 bytes are identified by a prefix and a hash, and the rare values whose
hashes collide are disambiguated in the order the import creates them. All ids are
written base64-encoded, exactly as `typedb export` writes them, and are deterministic
across runs.

Ids are not queryable after import. To preserve business ids as data, keep them as
ordinary columns mapped to attributes, ideally `@key`:

```sql
CREATE TABLE person (
    _iid UBIGINT PRIMARY KEY,     -- instance id: consumed by the loader, not data
    id BIGINT NOT NULL UNIQUE,    -- business id: owns id @key in the schema
    ...
);
```

For data without natural 64-bit ids, `row_number() OVER ()::UBIGINT` at staging time is
a fine source.

## `@typedb` comment directives

When names or roles diverge from convention, attach directives as DuckDB comments. They
live inside the `.duckdb` file, versioned with the data:

```sql
-- tables
COMMENT ON TABLE staging_people IS '@typedb entity person';
COMMENT ON TABLE works_for      IS '@typedb relation employment (employee = person_id, employer = company_id)';
COMMENT ON TABLE scratch        IS '@typedb skip';

-- columns
COMMENT ON COLUMN people.full_name IS '@typedb owns name';
COMMENT ON COLUMN people.photo     IS '@typedb skip';
```

The role bindings of `@typedb relation` are required when two FK columns reference the
same player table, e.g. `mentor_id` and `student_id` both referencing `person`, since
roles are otherwise inferred from the player types.

## Relations are tables

As in TypeDB, a relation is a table of its own: an FK column on an entity table has no
TypeDB form and is rejected. Stage the relation table it stands for, with its keys
declared up front (DuckDB cannot add foreign keys to an existing table):

```sql
CREATE TABLE management (
    _iid UBIGINT PRIMARY KEY,
    manager_id UBIGINT NOT NULL REFERENCES person(_iid),
    report_id UBIGINT NOT NULL REFERENCES person(_iid)
);
INSERT INTO management
    SELECT row_number() OVER (ORDER BY _iid), manager_id, _iid FROM person WHERE manager_id IS NOT NULL;
COMMENT ON TABLE management IS '@typedb relation management (manager = manager_id, report = report_id)';
COMMENT ON COLUMN person.manager_id IS '@typedb skip';
```

## Type conversions

| DuckDB | TypeDB value | Notes |
|---|---|---|
| `BOOLEAN` | `boolean` | |
| integer types | `integer` | `UBIGINT`/`HUGEINT` bounds-checked; also accepted into `decimal`, and into `double` where exact |
| `FLOAT`/`DOUBLE` | `double` | finite values only; `FLOAT` is widened exactly (`0.1` becomes `0.10000000149011612`); `DECIMAL` is also accepted, rounded to the nearest double; `-0.0` is loaded as `0.0`, since DuckDB does not keep the two apart |
| `DECIMAL(w,s)` | `decimal` | exact; `s ≤ 19`, integer part must fit 64 bits |
| `VARCHAR`, `ENUM`, `UUID`, `JSON` | `string` | compared byte-wise, as in TypeDB, whatever the column's collation: `'Bob'` and `'BOB'` are distinct values |
| `DATE` | `date` | finite, within TypeDB's range (years -262143 to 262142) |
| `TIMESTAMP[_S/_MS/_NS]` | `datetime` | exact to the nanosecond; finite, within TypeDB's range. Columns of one attribute are deduplicated together, so beside a `TIMESTAMP_NS` column, other timestamps must fit its range (years 1677 to 2262) |
| `TIMESTAMPTZ` | `datetime-tz` | always UTC: DuckDB stores the instant, not the zone; keep the zone in a separate column if it is data |
| `INTERVAL` | `duration` | months/days/nanos map exactly, so `1 month` and `30 days` are distinct durations (DuckDB compares them equal); negative components are rejected |
| `BLOB`, `TIME`, `STRUCT`, `MAP`, `UNION`, nested lists | — | rejected with a restructuring hint |

## Modelling recommendations

1. **One table per entity type**: an `_iid` column and one column per attribute.
2. **One junction table per relation type**: an `_iid` column, one FK per role (n-ary
   relations have n FKs), and relation attributes as plain columns. A relation playing a
   role in another relation is just an FK to the junction table's `_iid`.
3. **Multi-valued attributes**: a `LIST` column for small sets. TypeDB ownerships are
   sets: duplicate values collapse into one attribute and order is lost. If order
   matters, model it explicitly, e.g. as an index attribute on a relation.
4. **Self-referencing or multi-same-type relations** need the table directive binding
   the role columns, since role inference disambiguates by player type.
5. **Preserve your ids** by declaring `owns <id> @key` for business-id columns you want
   to query by after migration; `_iid` itself is consumed by the loader and is not data.
6. `TIMESTAMPTZ` stores a UTC instant and loses the original zone. If the zone is data,
   store its name in a separate `VARCHAR` column.
7. **Stay in range**: integers within 64 bits; `DECIMAL` scale ≤ 19 with ≤ 18 integer
   digits; everything else belongs in `string`.
8. Run `--dry-run` first on large datasets: the constraints the server would only
   enforce at the end of a long import are checked up front, where SQL can express them.

## Output format

The data file is the `typedb export` format: length-delimited protobuf `Migration.Item`
messages, in the order `database import` expects: a header, then every attribute, then
every entity, then every relation, then checksums. Relation tables come after the tables
of their players, so every ownership and role player refers to an item already imported,
except within tables that reference each other in a cycle (including a table referencing
itself). There, TypeDB would delete a relation whose players all arrive after its
transaction commits, so each row must have a player outside the cycle, or one that is an
earlier row (by `_iid`) of its own table; the loader checks this. The schema is not copied: import the
data file with the schema file it was prepared against.

A one-shot load stages the data file next to the DuckDB file and removes it after the
import; `--files-only` writes it into the given directory instead.
