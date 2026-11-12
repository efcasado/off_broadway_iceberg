# Examples

## Public Iceberg catalog

[`public_catalog.exs`](public_catalog.exs) reads a table of a real Iceberg catalog with a
Broadway pipeline and exits when the scan is done. It needs Elixir and network access,
nothing else: no credentials, no Docker, and no DuckDB to install or start.

It demonstrates:

- DuckDB managed by QuackDB under the script's own supervision tree.
- Attaching an Iceberg REST catalog before the pipeline starts.
- Reading the table's current snapshot, pinned for the whole scan.
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
Connecting to the public Iceberg catalog...
Catalog attached.

Reading pubdata.environment.earthquakes
Snapshot: 5964070490105136742

Batch 0: 5000 rows, strongest M8.0 2000 New Ireland Earthquake
Batch 1: 5000 rows, strongest M8.4 6 km SSW of Atico, Peru
...
Batch 39: 4650 rows, strongest M7.8 25 km SW of Kablalan, Philippines

Completed.
Rows processed: 188490
Batches processed: 40
Failed batches: 0
Elapsed: 2.105s
```

### How it works

`QuackDB.Server.child_specs/1` starts DuckDB as a local process and a QuackDB pool connected
to it. The pipeline is the next child. Before starting Broadway, it installs and loads the
extensions and attaches the catalog as `pubdata`. Stopping the supervisor stops the pipeline,
then the pool, then DuckDB.

The producer resolves the snapshot the table's `main` branch points at, prints it in
`Snapshot:`, and reads every row at that snapshot. If the provider publishes new data while
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

- **`Could not start` while attaching the catalog.** Check that the catalog answers:
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
