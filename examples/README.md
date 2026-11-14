# Examples

## Public Iceberg catalog

[`public_catalog.exs`](public_catalog.exs) reads a table of a real Iceberg catalog with a
Broadway pipeline and exits when the scan is done. It needs Elixir and network access,
nothing else: no credentials, no Docker, and no DuckDB to install or start.

It demonstrates:

- DuckDB managed by QuackDB under the script's own supervision tree.
- Loading the Iceberg extensions and attaching a REST catalog in DuckDB's boot SQL,
  before the QuackDB pool and Broadway pipeline start.
- Reading a snapshot selected by one fixed as-of timestamp for the run.
- `QuackDB.Columns` batches of at most 5,000 rows, processed with backpressure by two
  processors that each take one batch at a time.
- Telemetry handlers that tell a completed scan from a failed one, and an exit status
  that reflects it.

### The catalog

The data comes from [LakehouseBox's public catalog](https://lakehousebox.com/docs/sample-data/),
an Iceberg REST catalog that allows anonymous reads:

| Catalog           | Endpoint                                 | Auth |
| ----------------- | ---------------------------------------- | ---- |
| `commons/pubdata` | `https://public-catalog.lakehousebox.com` | none |

The default table is `environment.earthquakes`, about 190,000 earthquakes since 2000. It is
large enough to take several round trips to DuckDB, so the batches really are fetched as the
processors ask for them. Any other table of the catalog can be passed as an argument, for
example `reference.countries`, whose 249 rows fit in one batch.

LakehouseBox is run by a third party, with no guarantee that it stays available or keeps its
tables as they are. The tests of this repository do not depend on it.

### Running it

> **Draft dependency:** The examples in this branch assume a future QuackDB
> `:init_sql` server option. They cannot run with the currently pinned QuackDB
> `~> 0.7.0` release until that upstream change is available.

From the repository root, so that mise picks up the project's toolchain:

```shell
mise install
elixir examples/public_catalog.exs
elixir examples/public_catalog.exs reference.countries
```

The first run takes longer. `Mix.install/1` fetches and compiles the dependencies, QuackDB
downloads the DuckDB 2.0 binary, and DuckDB downloads its `iceberg` and `httpfs` extensions.
All three are cached for later runs.

The output looks like this; the snapshot, batch boundaries and timings come from the run:

```text
Starting DuckDB (the first run downloads it)...

Reading pubdata.environment.earthquakes
Snapshot: 5964070490105136742

Slice 0, batch 0: 5000 rows, strongest M8.0 2000 New Ireland Earthquake
Slice 0, batch 1: 5000 rows, strongest M8.4 6 km SSW of Atico, Peru
...
Slice 0, batch 39: 4650 rows, strongest M7.8 25 km SW of Kablalan, Philippines

Completed.
Rows processed: 188490
Batches processed: 40
Failed batches: 0
Elapsed: 2.105s
```

Two processors handle batches at the same time, so batch lines can appear out of order. The
`Reading` and `Snapshot` lines come from another process and can even appear after the first
batch.

### How it works

`QuackDB.Server.child_specs/1` starts DuckDB as a local process and a QuackDB pool connected
to it. The proposed `:init_sql` option installs and loads the extensions and
attaches the catalog as `pubdata` **before DuckDB starts serving**. The pipeline
starts only after the server and pool are ready. Stopping the supervisor stops the pipeline,
then the pool, then DuckDB.

The script chooses an as-of timestamp once, when it builds its supervision tree. The
producer resolves the snapshot that was current then, prints it in `Snapshot:`, and
reads every slice at that snapshot. The timestamp is kept if DuckDB restarts the pipeline.
These examples run one app instance; a multi-instance job must pass the same configured
snapshot id or timestamp to every instance. If the provider publishes new data while
the scan runs, the scan does not see it. The provider does republish its tables, so snapshot
ids change over time and old ones expire. Pass `snapshot_id:` to pin a specific one, but
expect it to stop working once it expires.

The script attaches its telemetry handlers before anything starts. The handlers run inside
the producer, so they only forward the event to the script's main process, which owns the
run:

- `:stop` with `failed_batches: 0`: every batch was processed. Exit status 0.
- `:stop` with `failed_batches` above 0: the whole snapshot was read, but some batches failed
  in the processors. Exit status 1.
- `:exception`: the scan could not read the snapshot, for example because the table does not
  exist. Exit status 1.

On any of them, the script stops the supervision tree and exits. It also exits with status 1
if the tree fails to start, for example when the catalog cannot be attached.

### Troubleshooting

- **`Could not start` during DuckDB initialization.** Check that the catalog answers:
  `curl "https://public-catalog.lakehousebox.com/v1/config?warehouse=commons/pubdata"` should
  return JSON. A proxy or firewall that blocks it blocks DuckDB too.
- **TLS errors.** DuckDB makes the HTTPS requests to the catalog and its data files, not the
  BEAM. A proxy that intercepts TLS needs its certificate trusted by DuckDB's `httpfs`
  extension; see DuckDB's
  [httpfs documentation](https://duckdb.org/docs/stable/core_extensions/httpfs/overview).
- **Extensions fail to install.** DuckDB downloads them from `extensions.duckdb.org`. Without
  access to it, install them once on a machine that has it, and copy `~/.duckdb/extensions`.
- **DuckDB fails to start or is the wrong version.** QuackDB needs DuckDB 2.0 and downloads it
  itself. If `QUACKDB_BINARY_PATH` is set, it points QuackDB at another binary, which must be
  2.0 too.
- **The table does not exist.** The script prints DuckDB's error, which suggests similar
  names. To list the tables, attach the catalog in the DuckDB CLI and run
  `SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'pubdata'`.

## PostgreSQL backfill

[`postgres_backfill.exs`](postgres_backfill.exs) is the backfill this library is for: it
reads the same `environment.earthquakes` table, validates every row, and upserts the valid
rows into a PostgreSQL table. Besides what the first example needs, it needs a PostgreSQL
server.

It demonstrates:

- Application logic in plain functions, `Earthquakes.validate/1` and `Earthquakes.store!/2`,
  that a pipeline reading the same events from Pulsar could call unchanged.
- One `INSERT` per batch, with each column passed as one array parameter.
- Idempotent, version-aware upserts: running it again changes nothing.
- Invalid rows recorded in a rejects table instead of failing their batch.

### Running it

[`docker-compose.yml`](docker-compose.yml) runs PostgreSQL 18 on port 55432, which the
script connects to by default:

```shell
docker compose -f examples/docker-compose.yml up --detach --wait
elixir examples/postgres_backfill.exs
docker compose -f examples/docker-compose.yml down --volumes
```

Any other PostgreSQL works too: point `DATABASE_URL` at it, for example
`DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres`.

The script creates two tables if they do not exist: `earthquakes`, keyed by `event_id`, and
`earthquake_rejects`. The output looks like this; snapshot, order and timings vary:

```text
Reading pubdata.environment.earthquakes
Snapshot: 5964070490105136742

Slice 0, batch 1: 5000 rows, 5000 written, 0 rejected
Slice 0, batch 0: 5000 rows, 5000 written, 0 rejected
...

Completed.
Rows read: 188490
Batches processed: 40
Failed batches: 0
Elapsed: 2.722s
Rows in earthquakes: 188490
Rows in earthquake_rejects: 0
```

Run it again and every batch reports `0 written`.

### How it writes

Each batch becomes one statement:

```sql
INSERT INTO earthquakes (event_id, event_time, ...)
SELECT * FROM unnest($1::text[], $2::timestamptz[], ...)
ON CONFLICT (event_id) DO UPDATE SET event_time = excluded.event_time, ...
WHERE excluded.updated_at > earthquakes.updated_at
```

A row only replaces one with an older `updated_at`. Writing the same rows again changes
nothing, and the order the batches arrive in does not matter. That makes the scan safe to
replay after a failure, and safe to run alongside another pipeline that writes newer
versions of the same rows, such as one reading Pulsar.

Validation works on one row at a time, so each batch is turned into maps with
`QuackDB.Columns.to_maps/1` first. A pipeline with no per-row logic can skip that and pass
each column to the statement directly, with `QuackDB.Columns.get/3`. If it does not need
Elixir at all, DuckDB's `postgres` extension can write to PostgreSQL without the rows passing
through the BEAM.

### Failures

- A row that fails validation goes to `earthquake_rejects`, with the reason and the row as
  JSON. Its batch still succeeds. The current table passes every check, so the rejects table
  stays empty.
- A batch that PostgreSQL fails to write is a failed message. The script reports it and exits
  with status 1. Running it again is safe, and writes what is missing.
- If PostgreSQL cannot be reached at startup, for example because the Compose service is not
  running, the script exits with status 1 before reading anything.

## Checkpointed backfill

[`checkpointed_backfill.exs`](checkpointed_backfill.exs) reads `places.admin_areas` of the same
public catalog, a table of three data files, with a checkpointed scan. Every data file makes a
slice, and every finished slice is recorded in
[fake-gcs-server](https://github.com/fsouza/fake-gcs-server), a GCS emulator, so a run that is
stopped midway is resumed by the next one.

It demonstrates:

- A single producer using the same sliced execution model as a cluster.
- `OffBroadway.Iceberg.Checkpoint.GCS` pointed at an emulator, without authentication.
- A job named after the snapshot it reads, which it pins.
- The `[:off_broadway_iceberg, :checkpoint, ...]` telemetry events.

### Running it

[`docker-compose.yml`](docker-compose.yml) runs the emulator on port 4443, where the script
looks for it, and the script creates the bucket. `--crash-after 1` halts the script once it has
recorded one slice, as a crash would:

```shell
docker compose -f examples/docker-compose.yml up --detach --wait fake-gcs
elixir examples/checkpointed_backfill.exs --crash-after 1
elixir examples/checkpointed_backfill.exs
elixir examples/checkpointed_backfill.exs
docker compose -f examples/docker-compose.yml down --volumes
```

The first run reads the first slice, records it, and halts:

```text
Job admin-areas-7578015534121896607, snapshot 7578015534121896607
0 of 3 slices finished by earlier runs

Slice 0, batch 0: 1000 rows
Slice 0, batch 1: 1000 rows
Slice 0, batch 2: 48 rows
Recorded slice 0: 3 batches, 2048 rows

Halting, as a crash would. Run the script again to resume.
```

The second reads the other two, and the third finds every slice recorded and reads nothing:

```text
Job admin-areas-7578015534121896607, snapshot 7578015534121896607
3 of 3 slices finished by earlier runs

Completed.
Slices recorded by this run: 0
Rows of the snapshot: 5210
Failed batches: 0
```

The checkpoints are objects of the emulator's `checkpoints` bucket:

```shell
curl -s "http://localhost:4443/storage/v1/b/checkpoints/o?prefix=examples/"
```

The emulator keeps them in memory, so stopping it starts every job over. When the catalog
publishes a new snapshot of the table, the script starts a new job, named after it.

To record checkpoints in a real bucket instead, drop `:endpoint` and authenticate with Goth:
`auth: {:goth, MyApp.Goth}`. See `OffBroadway.Iceberg.Checkpoint.GCS`.
