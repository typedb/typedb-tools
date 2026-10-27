# TypeDB Loader

The TypeDB Loader bulk-loads rows from a CSV file into a TypeDB database by repeatedly
executing a user-supplied TypeQL insert pipeline whose `given` stage names the columns to
bind. Rows are read in batches, each batch is committed in its own write transaction, and
progress is checkpointed so an interrupted run can be resumed.

## Loading from database files

`typedb loader --duckdb <file>` migrates a DuckDB database into TypeDB: the tables are
mapped onto `--schema-file`, converted, and imported into `--database` in one run, with
the schema's constraints enforced by the server. `--dry-run` converts without writing;
`--files-only <dir>` writes the data file for a later `database import` instead of
loading. See
[migration/README.md](migration/README.md) for conventions, directives, and modelling
recommendations.

## Running TypeDB Loader

Go to the directory where you have your `typedb-all` or `typedb-loader` distribution unarchived,
and run `./typedb loader`:
```bash
cd <your_typedb_loader_dir>/
./typedb loader --query=<query.tql> --database=<db> --data=<data.csv> --username=<username> --address=<address>
```

To build and run from Cargo, use:
```bash
cargo run -p typedb-loader -- --query=<query.tql> --database=<db> --data=<data.csv> --username=<username> --address=<address>
```

Or to use bazel, use:
```bash
bazel run //loader:loader-native -- --query=<query.tql> --database=<db> --data=<data.csv> --username=<username> --address=<address>
```

TypeDB Loader binaries are platform-specific, so cannot be moved across platforms - please use
the correct platform-specific distribution.

## How it works

1. Parse the query file. The pipeline must contain a `given` stage that declares the input
   variables and their value types. These names are matched against the CSV header row (or
   used positionally when the CSV has no header).
2. Optionally run a schema file in a schema transaction, and/or create the database, before
   the data load begins.
3. Read the CSV in batches of `--batch-rows` rows. Each batch becomes one
   `query_with_inputs` call inside its own write transaction; up to `--parallel-batches`
   batches run concurrently.
4. Rows that fail to parse or whose batch fails to commit are written to a rejects CSV and
   per-rejection log file, then the run continues (unless `--stop-on-error` or
   `--max-rejects` is set).
5. After each batch finishes, a checkpoint file is updated. If the run is interrupted,
   resuming from the checkpoint will pick up at the next un-committed batch.

## Command line arguments

The typically-used arguments are:

- `--query=<path>` : Path to the TypeQL query file containing the `given` stage and
  insert pipeline (mandatory).
- `--database=<db>` : Name of the database to load into (mandatory).
- `--data=<path>` : Path to the CSV data file (mandatory).
- `--username=<username>` : TypeDB server username to log in with (mandatory).
- `--address=<address>` : TypeDB server address to connect to (mandatory). Use
  `--addresses host1:port1,host2:port2,...` for a multi-node cluster.
- `--header[=true|false]` / `--no-header` : Whether the CSV has a header row. Default: `false`.
- `--batch-rows=<n>` : Rows per write transaction. Default: `1000`.
- `--parallel-batches=<n>` : Maximum number of batches in flight concurrently. Default: `1`
  (strictly sequential).
- `--schema-file=<path>` : Apply this TypeQL schema in a schema transaction before loading.
- `--create-db[=true|false]` : Create the database if it does not exist. Default: `false`.
- `--output-dir=<path>` : Directory to write `rejects.csv`, `rejects.log`, and
  `checkpoint.json` into. Created if it does not exist. Defaults to
  `loader_<data-stem>_progress` next to the data file.
- `--resume=<output-dir>` : Resume a previous run from the given output directory (which
  must contain a `checkpoint.json` from that run).
- `-h, --help` : Show the full help message.

The loader will by default prompt you for your password in a safe way. If you must, you are
still able to pass in the password with `--password=<password>`.

**By default, TLS encryption is enabled to ensure passwords are not sent over the network
in plaintext.**

For development or local work, you can disable this with:

`--tls-disabled`

For TypeDB Cloud deployments, there is **no reason to use this setting** as they can only
operate with network TLS encryption.

Alternatively, you may securely connect by managing your own certificates and provide your
root CA to the loader with:
`--tls-root-ca=<path>`

## Query template

The query file must be a TypeQL pipeline whose first stage is `given`, declaring one
variable per CSV column that the pipeline consumes. For each row, the loader binds the
declared variables to the parsed cell values and runs the rest of the pipeline.

```typeql
given $name: string, $age: integer?, $active: boolean;
insert
  $p isa person, has name == $name;
  try { $p has age == $age; };
  try { $p has active == $active; };
```

Supported value types in `given` declarations are TypeDB's built-in types:
`boolean`, `integer`, `double`, `decimal`, `string`, `date`, `datetime`, `datetime-tz`,
and `duration`. Append `?` to mark an input as optional; optional inputs accept null
cells and pass an empty binding into the pipeline, which works naturally with
`try { ... };`.

Date / datetime parsing:
- `date`: `YYYY-MM-DD`.
- `datetime`: `YYYY-MM-DDTHH:MM:SS[.fff]` or `YYYY-MM-DD HH:MM:SS`.
- `datetime-tz`: a datetime followed by a time zone, per the
  [TypeQL reference](https://typedb.com/docs/typeql-reference/values/datetimetz/).
  The zone is either an ISO-8601 UTC offset (`Z`, `±HH`, `±HH:MM`, or `±HHMM`)
  attached directly to the datetime, or an IANA TZ identifier
  (`Europe/London`, `Asia/Kolkata`, ...) separated from the datetime by a single
  space. Examples: `2024-03-30T12:00:00Z`, `1920-04-26T16:30-09:30`,
  `1987-12-22T17:29 Asia/Kolkata`.
- `duration`: ISO-8601 duration (e.g. `P1Y2M3DT4H5M6S`).

## CSV data file

By default the loader treats the CSV as headerless and binds columns positionally to the
`given` variables in declaration order. With `--header`, the first row is read as a header
and each variable is bound to the column whose name matches it — extra columns are ignored,
and a missing required column is an error.

Null handling:
- Without `--null-values`, only empty cells are treated as null.
- With `--null-values`, the supplied list **replaces** the default — include `""`
  explicitly if you still want empty cells to count as null. Repeat the flag for multiple
  tokens: `--null-values=NULL --null-values=N/A --null-values=""`.

Null cells in a non-optional column are rejected; null cells in an optional column produce
an empty binding (the corresponding `try { ... }` clause becomes a no-op).

Use `--max-rows=<n>` to process at most `n` data rows (handy for dry runs).

## Batching and parallelism

`--batch-rows` (default 1000) controls how many rows are bundled into a single
`query_with_inputs` invocation and committed together. Larger batches reduce commit
overhead; smaller batches reduce the blast radius of a single bad row (the whole batch is
rejected if the commit fails).

`--parallel-batches` (default 1) caps the number of write transactions in flight at once.
Increasing this raises throughput when network latency dominates, at the cost of weaker
ordering guarantees: a later batch may commit before an earlier one. The loader records
each completion in the checkpoint regardless of order, so resume still works correctly.

See [CONCURRENCY.md](CONCURRENCY.md) for a deeper description of the worker / rejects /
shutdown model.

## Schema and database setup

`--schema-file=<path>` runs the file's contents in a schema transaction before any data is
loaded; `--create-db` creates the database first if it doesn't already exist. Both flags
are ignored (with a warning) when `--resume` is used, since the prior run is assumed to
have already done this work.

## Output directory

All of the loader's output — `rejects.csv`, `rejects.log`, and `checkpoint.json` — is written
to a single output directory, which is created if it does not already exist. By default the
directory is `loader_<data-stem>_progress`, alongside the data file. Override the location
with `--output-dir=<path>`.

## Errors and rejects

When a row fails to parse, or a batch's commit fails, the loader writes:
- the offending CSV rows to `rejects.csv` in the output directory (with the original header
  if any), and
- a per-rejection message to `rejects.log` in the output directory.

By default the loader skips the rejected rows and continues. Use:
- `--stop-on-error` to abort on the first parse error or batch commit failure (offending
  rows are still recorded before exit), or
- `--max-rejects=<n>` to abort once the cumulative rejected-row count exceeds `n`.

Note: with `--parallel-batches > 1`, a triggered stop may over-shoot the threshold by up
to `parallel_batches × batch_rows` rows, because batches already in flight are allowed to
finish so they are fully recorded.

## Checkpointing and resume

A `checkpoint.json` file inside the output directory is written after each batch finishes,
recording the resolved parameters, content hashes of the data / query / live schema, and
the batches that have been dispatched and finished. Checkpointing can be disabled with
`--no-checkpoint`.

To resume an interrupted run, point `--resume` at the previous run's output directory:
```bash
./typedb loader --resume=<path-to-output-dir>
```
Resume re-uses every parameter from the checkpoint (except the password, which is prompted
again); anything you pass on the command line overrides the checkpointed value, except
`--batch-rows`, which must match the checkpoint exactly. `--schema-file` and `--create-db`
are ignored on resume with a warning.

On resume the loader will warn and ask for confirmation if:
- the data file hash, live schema hash, or query file hash differs from the checkpoint, or
- the checkpoint records any in-flight batches (dispatched but not confirmed as committed
  before the previous run exited).

For in-flight batches, the loader prints the first CSV row of each batch and asks whether
to reprocess all of them, skip all of them (treat as already committed), or decide one by
one — so you can manually verify against the database before choosing.

## A complete example

Given this schema:
```typeql
define
  attribute name, value string;
  attribute age, value integer;
  attribute active, value boolean;
  entity person, owns name @key, owns age, owns active;
```

This query template:
```typeql
given $name: string, $age: integer?, $active: boolean;
insert
  $p isa person, has name == $name;
  try { $p has age == $age; };
  try { $p has active == $active; };
```

And this CSV (`data.csv`):
```
name,age,active
user_1,68,true
user_2,,false
user_3,73,true
```

Create the database, apply the schema, and load:
```bash
./typedb loader \
  --query=query.tql \
  --schema-file=schema.tql \
  --database=people \
  --create-db \
  --data=data.csv \
  --header \
  --batch-rows=1000 \
  --parallel-batches=4 \
  --username=admin \
  --address=localhost:1729 \
  --tls-disabled
```

If the run is interrupted, resume it with:
```bash
./typedb loader --resume=loader_data_progress --username=admin --tls-disabled
```
