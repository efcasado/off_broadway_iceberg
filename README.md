# Broadway Producer for Apache Iceberg

A [Broadway](https://github.com/dashbitco/broadway) producer that reads a snapshot of an
[Apache Iceberg](https://iceberg.apache.org/) table through [DuckDB](https://duckdb.org/),
using [QuackDB](https://hex.pm/packages/quackdb). It is meant for backfills and other jobs
that process a table as it was at one point in time.

The producer runs one query against a fixed snapshot and emits the result as Broadway
messages, one per batch of rows, at the pace Broadway asks for them. Commits made to the
table while the scan runs are not read.

> #### Experimental {: .warning}
>
> This is an early release. It reads one snapshot from start to end. It does not poll for
> new snapshots, read incremental changes, or persist progress: a scan that is interrupted
> starts again from the beginning. See [Failures and replays](#failures-and-replays).

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
           snapshot_id: :current,
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

The producer requires `producer: [concurrency: 1]`, since every producer stage would scan
the whole snapshot. Scale out with the processors instead.

### Options

- `:connection` (required) - The QuackDB pool: the pid or registered name of a pool started
  with `QuackDB.start_link/1` or `QuackDB.Server.child_specs/1`. The producer checks out one
  connection for the length of the scan. The pool stays yours: the producer never stops it.
  An `Ecto.Repo` is not accepted.
- `:table` (required) - The table, as `"catalog.schema.table"` made of plain identifiers, or
  as a list of name parts such as `["lake", "betting", "my-events"]` for any other names.
  Every part is quoted before it reaches SQL.
- `:snapshot_id` - `:current` (the default) or a snapshot id.
- `:fetch_size` - The maximum number of rows in one message (default `10_000`). It is not
  Broadway's batcher `:batch_size`, which counts messages.

See `OffBroadway.Iceberg.Producer` for the full reference.

## Snapshots

With `snapshot_id: :current`, the producer resolves the snapshot the table's `main` branch
points at, once, before the first fetch, and reads every row with
`AT (VERSION => snapshot_id)`. That is the table's current snapshot, which after a rollback
is not necessarily the newest one. With an explicit id, the producer reads that snapshot.

Either way the scan is pinned: rows committed while it runs are not read. A table with no
snapshots yields no messages.

## Batches

Each message carries a `QuackDB.Columns` struct in `:data`, holding the rows of the batch as
one list per column, already decoded into Elixir terms. It is not Arrow and not zero-copy.
Read columns with `QuackDB.Columns.get/3`, or convert with `QuackDB.Columns.to_rows/1` and
`QuackDB.Columns.to_maps/1` when you need rows.

A message holds at most `:fetch_size` rows, and fewer at the end of the scan and wherever a
round trip to DuckDB ends. DuckDB sends results in whole vectors of 2,048 rows, up to about
`quack_target_batch_bytes` per round trip (a DuckDB setting, 32 MiB by default).

The message metadata holds the `:table`, the `:snapshot_id` and the `:batch_index`, the
position of the batch in the scan, starting at 0.

## Backpressure

The producer fetches a batch only for demand Broadway has already sent, and holds no rows
itself. A worker process owns the QuackDB cursor, fetches one batch at a time, and waits
whenever there is no demand, so a slow pipeline pauses the scan. It never repeats a query
with `LIMIT` and `OFFSET`.

The BEAM holds the messages in flight, plus at most one round trip from DuckDB that has not
been handed out yet. Messages in flight are bounded by Broadway's demand, up to `max_demand`
per processor, so prefer a small `max_demand` such as `1` or `2` for batch messages.

QuackDB decodes strings of 64 bytes or more as slices of the response they arrived in.
Copy them with `:binary.copy/1`, or start the pool with `copy_binaries: true`, if you keep
a few of them for long.

## Acknowledgements

Messages are acknowledged back to the producer with `Broadway.CallerAcknowledger`. An
acknowledgement tells the producer that a batch was processed, nothing more: it is not
stored, and DuckDB and Iceberg have no notion of it. Failed batches are logged and counted,
not retried.

## Completion

Once every row has been read, the producer releases its connection. The scan is complete
when every batch has also been acknowledged. The producer then stays idle, and never reads
the snapshot again. It emits telemetry along the way:

- `[:off_broadway_iceberg, :scan, :start]` once the snapshot is resolved.
- `[:off_broadway_iceberg, :scan, :stop]` when the scan is complete, with the number of
  `:batches`, `:rows` and `:failed_batches`.
- `[:off_broadway_iceberg, :scan, :exception]` when the scan fails.

A `:stop` event with `failed_batches: 0` means every batch was processed successfully.
With `failed_batches` above 0, the whole snapshot was read, but that many batches failed
downstream. A scan that could not read the snapshot emits `:exception` instead, never
`:stop`.

Attach telemetry handlers before starting the pipeline, since a scan that fails early
emits `:exception` right away. The events are the only record of the outcome: the producer
stays idle whether the scan completed or failed. If you need the outcome later, for
example to resume a job, persist it yourself.

Broadway has no way for a producer to stop its own pipeline. To stop it when the scan
completes, handle the `:stop` event and call `Broadway.stop/1` from another process.

## Failures and replays

A query or connection error fails the scan: the producer logs it, emits the `:exception`
event and stays idle. It does not start the scan again, so the batches it emitted are not
delivered twice.

Progress is not persisted, though. If the producer crashes, Broadway restarts it and it
scans the snapshot again from the start, and so does a pipeline you start again after a
failure. Make downstream writes idempotent, for example keyed by the snapshot id and the
row's key. This producer does not provide exactly-once processing.

When the pipeline shuts down mid-scan, the producer stops its worker. The connection the
worker held is disconnected, which cancels the query on the DuckDB server, and the pool
reconnects it.

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

Integration tests start an Iceberg REST catalog with Docker Compose and a managed DuckDB
2.0 server through QuackDB, which downloads DuckDB and the `iceberg` extension the first
time:

```shell
mix test --only integration
```
