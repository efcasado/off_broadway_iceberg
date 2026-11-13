# Broadway Producer for Apache Iceberg

A [Broadway](https://github.com/dashbitco/broadway) producer that reads a snapshot of an
[Apache Iceberg](https://iceberg.apache.org/) table through [DuckDB](https://duckdb.org/),
using [QuackDB](https://hex.pm/packages/quackdb). It is meant for backfills and other jobs
that process a table as it was at one point in time.

The producer streams a fixed snapshot as Broadway messages, one per batch of rows,
at the pace Broadway asks for them. Commits made while the scan runs are not read.

New to Iceberg or DuckDB? [How Iceberg and DuckDB work here](guides/iceberg-and-duckdb.md)
explains snapshots, data and delete files, slices and checkpoints, and how this library
reads a table, in about 15 minutes.

> #### Experimental {: .warning}
>
> This is an early release. It reads one snapshot from start to end. It does not poll for
> new snapshots or read incremental changes, and unless it is checkpointed, a scan that is
> interrupted starts again from the beginning. See [Failures and replays](#failures-and-replays)
> and [Checkpoints](#checkpoints).

## Installation

Add `:off_broadway_iceberg` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_iceberg, "~> 0.1.0"}
  ]
end
```

## Requirements

- DuckDB 2.0 with the `quack` and `iceberg` extensions. QuackDB speaks DuckDB's Quack
  protocol and can run DuckDB for you, as below. It downloads the DuckDB binary on first
  start, and DuckDB downloads the extensions the first time they are installed.
- An Iceberg catalog DuckDB can attach, such as an Iceberg REST catalog. Tables read with
  `iceberg_scan(...)` from a path are not supported.

## Usage

Run DuckDB under your application's supervision tree with QuackDB, next to a QuackDB
connection pool:

```elixir
children =
  QuackDB.Server.child_specs(
    server: [name: MyApp.DuckDB, duckdb: :managed],
    client: [name: MyApp.QuackDB, pool_size: 2]
  ) ++ [MyApp.IcebergBackfill]

Supervisor.start_link(children, strategy: :rest_for_one)
```

Then load the `iceberg` extension, attach the catalog, and start the pipeline. Extensions
and attached catalogs belong to the DuckDB server, so this runs again whenever a restarted
server restarts the pipeline:

```elixir
defmodule MyApp.IcebergBackfill do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    QuackDB.query!(MyApp.QuackDB, "INSTALL iceberg")
    QuackDB.query!(MyApp.QuackDB, "LOAD iceberg")

    QuackDB.query!(
      MyApp.QuackDB,
      "ATTACH IF NOT EXISTS 'warehouse' AS lake " <>
        "(TYPE iceberg, URI 'http://localhost:8181', AUTHORIZATION_TYPE 'none')"
    )

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {OffBroadway.Iceberg.Producer,
           connection: MyApp.QuackDB,
           table: "lake.betting.events",
           snapshot_id: {:as_of, ~U[2026-11-12 00:00:00Z]},
           fetch_size: 10_000},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 4, max_demand: 1]
      ]
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: batch} = message, _context) do
    ids = QuackDB.Columns.get(batch, "id")
    IO.puts("#{batch.num_rows} rows of snapshot #{message.metadata.snapshot_id}: #{inspect(Enum.take(ids, 3))}...")
    message
  end
end
```

`AUTHORIZATION_TYPE 'none'` suits a catalog without authentication. See DuckDB's
[Iceberg documentation](https://duckdb.org/docs/stable/extensions/iceberg/overview) for
catalogs that need credentials and for object storage secrets.

Every scan is sliced and coordinated, even with one producer on one node. Increase
producer concurrency or add connected app instances to share the work. Size each local
QuackDB pool for its concurrent producer stages. `slice_rows: 1_000_000` is the default;
set it directly in the producer options to change the target.

### One DuckDB per pipeline

Give each pipeline its own DuckDB server and QuackDB pool, under one `:rest_for_one`
supervisor, as above. The pipeline then owns that DuckDB instance's extensions and attached
catalogs, which no other code changes. DuckDB's memory and threads are freed when the
pipeline stops, and a failing DuckDB only takes down its own pipeline.

DuckDB uses every core and up to 80% of the machine's memory by default. When several
pipelines run on one machine, give each its share:

```elixir
server: [name: MyApp.DuckDB, duckdb: :managed, settings: [threads: 4, memory_limit: "4GB"]]
```

Run the setup in the pipeline's `start_link`, before `Broadway.start_link/2`, as above. It
runs once, it fails the startup with the actual error, and it runs again whenever DuckDB
restarts, because the supervisor then restarts the pipeline too. Two other places look
natural but are not reliable:

- The pool's `:after_connect` option runs on every connection, at the same time. DuckDB
  rejects all but one of the concurrent `ATTACH IF NOT EXISTS` with "already exists", and
  DBConnection still hands out a connection whose setup failed. Scans then fail with errors
  that do not name the cause.
- A `Task` started before the pipeline does not wait for the setup to finish, so the scan
  can start before the catalog is attached.

### Selecting columns and rows

`:columns` and `:where` read only part of the snapshot:

```elixir
{OffBroadway.Iceberg.Producer,
 connection: MyApp.QuackDB,
 table: "lake.betting.events",
 snapshot_id: 123456789,
 columns: ["event_id", "occurred_at", "amount"],
 where: {"occurred_at >= ? AND occurred_at < ?", [~U[2026-10-01 00:00:00Z], ~U[2026-11-01 00:00:00Z]]}}
```

DuckDB pushes both into its Iceberg scan, so it reads only those columns, and can skip the
parts of data files whose statistics rule the condition out. Delete files still apply.
QuackDB writes the params into the condition as SQL literals. The condition itself is SQL,
run as written: build it in your code, never from user input.

`:where` must select the same rows on every node and every run of a checkpointed job.
Pass a fixed cutoff as a parameter, rather than calling `now()` or computing a new
`DateTime.utc_now()` at startup. The producer fingerprints the condition's text, so it
cannot detect a condition whose result changes on its own. See "Selecting columns and
rows" in `OffBroadway.Iceberg.Producer` for the full requirements.

### Distributed scans

> #### Experimental {: .warning}
>
> Distributed scans are new. Their behavior may change.

When the application runs on several connected nodes, for example a Phoenix application
clustered with `DNSCluster` on Kubernetes, the producers of a pipeline can share one scan,
and keep sharing it as an autoscaler adds and removes nodes:

```elixir
{OffBroadway.Iceberg.Producer,
 connection: MyApp.QuackDB,
 table: "lake.betting.events",
 snapshot_id: {:as_of, ~U[2026-11-12 00:00:00Z]},
 slice_rows: 1_000_000}
```

Each producer plans the snapshot's data files into slices of at least a million rows,
except the last (`slice_rows: rows` changes the target). Files are never
split, so a table stored in one large file can only use one producer. Members find each
other through OTP's `:pg`, assign slices with rendezvous hashing, and share progress.

The nodes must be connected and use the same pipeline name, table, selection and fixed
snapshot. `:current` is not supported. Membership changes and network partitions
can cause slices to be read more than once; a full cluster restart loses progress unless
the scan is checkpointed. See "Distributed scans" in `OffBroadway.Iceberg.Producer` for
membership and completion guarantees.

[How producers share a scan](guides/distribution.md) explains local stages, app replicas,
`:pg` membership, slice ownership, checkpoints, and failures with a worked example.

### Checkpoints

> #### Experimental {: .warning}
>
> Checkpoints are new. Their behavior may change.

A scan can record the slices it finishes in Google Cloud Storage, so that a
scan started again, after a crash, a redeploy or a restart of every node, only reads the
slices no earlier run finished:

```elixir
{OffBroadway.Iceberg.Producer,
 connection: MyApp.QuackDB,
 table: "lake.betting.events",
 snapshot_id: 123456789,
 checkpoint: [
   job_id: "events-backfill-2026-11",
   backend:
     {OffBroadway.Iceberg.Checkpoint.GCS,
      bucket: "my-checkpoints", prefix: "iceberg", auth: {:goth, MyApp.Goth}}
 ]}
```

Add `{:req, "~> 0.7.5"}` and, for `auth: {:goth, name}`, `{:goth, "~> 1.4"}` to your
dependencies. Your application starts the Goth server, which finds Application Default
Credentials, including GKE Workload Identity.

Keep the job id, table name, snapshot, slice target, columns and condition fixed across
runs. A changed job fails before reading any rows. A slice is recorded only after all
its batches succeed; failed slices are left for a later run. Storage errors fail the scan.

See "Checkpoints" in `OffBroadway.Iceberg.Producer` for job and retry semantics,
`OffBroadway.Iceberg.Checkpoint.GCS` for storage and authentication, and
`OffBroadway.Iceberg.Checkpoint` for implementing another backend. For a local GCS
emulator, see [the checkpointed example](examples/checkpointed_backfill.exs).

### Options

See `OffBroadway.Iceberg.Producer.start_link/1` for the full option reference. `:connection`
and `:table` are required, along with a fixed `:snapshot_id`. The table can be a dotted
name such as `"lake.betting.events"`, or a list such as `["lake", "betting", "my-events"]`
for names containing punctuation.

## Snapshots

`:snapshot_id` is required. Pass an integer id or a fixed `{:as_of, datetime}` shared by
all producers and app instances. An as-of timestamp resolves through the table's snapshot
history, including rollbacks. A timestamp before the first snapshot yields no messages.

Choose the value once in your job configuration. `:current` and an omitted snapshot are
rejected, since producers resolving "current" independently might read different versions.
Every slice uses `AT (VERSION => snapshot_id)`, so later commits are never read.

Tables with a physical column named `filename`, regardless of case, are rejected during
planning: that name would shadow DuckDB's virtual column used to select slice files.

## Batches

Each message carries a `QuackDB.Columns` struct in `:data`: decoded Elixir values, one
list per column. Read columns with `QuackDB.Columns.get/3`, or use
`QuackDB.Columns.to_rows/1` and `QuackDB.Columns.to_maps/1` when you need rows.

Messages hold at most `:fetch_size` rows (default `10_000`); this is separate from Broadway's
batcher `:batch_size`, which counts messages. Metadata includes `:table`, `:snapshot_id`,
`:batch_index` and `:slice`. Batch indexes start at zero within
each slice.

## Backpressure

A worker holds one QuackDB connection and fetches only for outstanding Broadway demand.
Memory holds the messages in flight plus at most one buffered round trip from DuckDB.
Use a small processor `max_demand`, such as `1` or `2`, since each message is a batch.

QuackDB decodes strings of 64 bytes or more as slices of the response they arrived in.
Copy them with `:binary.copy/1`, or start the pool with `copy_binaries: true`, if you keep
a few of them for long.

## Completion

The producer releases its connection after reading every row and stays idle once all
batches are acknowledged. It emits these telemetry events:

- `[:off_broadway_iceberg, :scan, :start]` once the snapshot is resolved.
- `[:off_broadway_iceberg, :scan, :stop]` when the scan is complete, with `:batches`,
  `:rows`, `:failed_batches` and `:local_failed_batches`.
- `[:off_broadway_iceberg, :scan, :exception]` when the scan fails.

`failed_batches: 0` on `:stop` means every row was processed successfully. The counts
cover one copy of each slice; `:local_failed_batches` includes failures in other
copies this producer emitted. The producer reference defines the measurements and exact
completion guarantees.

Attach handlers before starting the pipeline. Both completion and failure leave the
producer idle, so persist the outcome if you need it later. To shut down on completion,
forward the `:stop` event to another process and call `Broadway.stop/1` there.

## Failures and replays

Processing is at least once. Make downstream writes idempotent, for example keyed by the
snapshot id and the row's key. Failed batches are logged and counted without retrying.
A query or connection error emits `:exception` and leaves the producer idle.

Without [checkpoints](#checkpoints), a restarted producer scans from the beginning.
Checkpointed scans skip successfully recorded slices, but can replay work processed before
a crash. See "Completion, failure and restarts" and "Checkpoints" in
`OffBroadway.Iceberg.Producer` for the full contract.

## Examples

- [`examples/public_catalog.exs`](examples/public_catalog.exs) reads a table of a public
  Iceberg REST catalog end to end, with DuckDB managed by QuackDB and nothing else to set up.
- [`examples/postgres_backfill.exs`](examples/postgres_backfill.exs) backfills the same data
  into PostgreSQL, with validation, a rejects table and idempotent, version-aware upserts.
- [`examples/checkpointed_backfill.exs`](examples/checkpointed_backfill.exs) checkpoints a scan
  in a GCS emulator, and resumes it after a crash.

```shell
elixir examples/public_catalog.exs

docker compose -f examples/docker-compose.yml up --detach --wait
elixir examples/postgres_backfill.exs
elixir examples/checkpointed_backfill.exs --crash-after 1
elixir examples/checkpointed_backfill.exs
```

See [`examples/README.md`](examples/README.md).

## Development

The toolchain is pinned in `mise.toml`:

```shell
mise install
mix deps.get
```

Unit tests run without DuckDB or Docker:

```shell
mix test
```

Integration tests start an Iceberg REST catalog and fake-gcs-server with Docker Compose,
and a managed DuckDB 2.0 server through QuackDB, which downloads DuckDB and the `iceberg`
extension the first time:

```shell
mix test --only integration
```
