#!/usr/bin/env elixir

# Iceberg to PostgreSQL backfill example
#
# Reads the earthquakes of LakehouseBox's public Iceberg catalog
# (https://lakehousebox.com/docs/sample-data/) with a Broadway pipeline, validates every
# row, and upserts the valid ones into a PostgreSQL table, one statement per batch.
# Running it again changes nothing: rows are keyed by event id, and a row is only
# overwritten by a newer version of itself.
#
# Usage, from the repository root:
#   docker compose -f examples/docker-compose.yml up --detach --wait
#   elixir examples/postgres_backfill.exs
#
# It connects to the PostgreSQL in examples/docker-compose.yml. Set DATABASE_URL to use
# another one.
#
# The script exits on its own: with status 0 once every batch has been written, and with
# status 1 if the scan failed or any batch did. See examples/README.md.

Mix.install([
  {:off_broadway_iceberg, path: Path.join(__DIR__, "..")},
  {:postgrex, "~> 0.22.4"}
])

Logger.configure(level: :warning)

defmodule Earthquakes do
  @moduledoc """
  The application's own logic for earthquakes, independent of where they come from. A
  pipeline reading them from Pulsar would call the same functions.
  """

  @columns [
    event_id: "text",
    event_time: "timestamptz",
    latitude: "float8",
    longitude: "float8",
    depth_km: "float8",
    magnitude: "float8",
    magnitude_type: "text",
    place: "text",
    event_type: "text",
    status: "text",
    network: "text",
    updated_at: "timestamptz"
  ]

  @names Enum.map(@columns, fn {name, _type} -> Atom.to_string(name) end)

  def create_tables!(postgres) do
    definitions = Enum.map_join(@columns, ", ", fn {name, type} -> "#{name} #{type}" end)
    Postgrex.query!(postgres, "CREATE TABLE IF NOT EXISTS earthquakes (#{definitions}, PRIMARY KEY (event_id))", [])

    Postgrex.query!(
      postgres,
      "CREATE TABLE IF NOT EXISTS earthquake_rejects (event_id text PRIMARY KEY, reason text NOT NULL, row jsonb NOT NULL)",
      []
    )
  end

  @doc """
  Checks one earthquake, given as a map with the columns above as string keys.
  """
  def validate(%{"event_id" => id}) when id in [nil, ""], do: {:error, "missing event id"}
  def validate(%{"updated_at" => nil}), do: {:error, "missing updated_at"}
  def validate(%{"magnitude" => m}) when not is_number(m) or m < -2 or m > 10, do: {:error, "implausible magnitude"}
  def validate(%{"latitude" => lat}) when not is_number(lat) or lat < -90 or lat > 90, do: {:error, "invalid latitude"}

  def validate(%{"longitude" => lon}) when not is_number(lon) or lon < -180 or lon > 180,
    do: {:error, "invalid longitude"}

  def validate(%{}), do: :ok

  @doc """
  Upserts earthquakes in one statement and returns how many rows changed. Each column
  becomes one array parameter. A row only replaces one with an older `updated_at`, so
  writing the same earthquakes again, or in any order, changes nothing.
  """
  def store!(_postgres, []), do: 0

  def store!(postgres, earthquakes) do
    names = Enum.join(@names, ", ")
    arrays = @columns |> Enum.with_index(1) |> Enum.map_join(", ", fn {{_name, type}, i} -> "$#{i}::#{type}[]" end)
    updates = Enum.map_join(@names -- ["event_id"], ", ", &"#{&1} = excluded.#{&1}")

    statement = """
    INSERT INTO earthquakes (#{names})
    SELECT * FROM unnest(#{arrays})
    ON CONFLICT (event_id) DO UPDATE SET #{updates}
    WHERE excluded.updated_at > earthquakes.updated_at
    """

    params = Enum.map(@names, fn name -> Enum.map(earthquakes, & &1[name]) end)
    Postgrex.query!(postgres, statement, params).num_rows
  end

  @doc """
  Records earthquakes that failed validation, with the reason, instead of failing the
  batch they came in.
  """
  def reject!(_postgres, []), do: 0

  def reject!(postgres, rejects) do
    Postgrex.query!(
      postgres,
      """
      INSERT INTO earthquake_rejects (event_id, reason, row)
      SELECT * FROM unnest($1::text[], $2::text[], $3::text[]::jsonb[])
      ON CONFLICT (event_id) DO UPDATE SET reason = excluded.reason, row = excluded.row
      """,
      [
        Enum.map(rejects, fn {earthquake, _reason} -> earthquake["event_id"] end),
        Enum.map(rejects, fn {_earthquake, reason} -> reason end),
        # Postgrex encodes jsonb with Jason, which this script does not depend on.
        Enum.map(rejects, fn {earthquake, _reason} -> JSON.encode!(earthquake) end)
      ]
    ).num_rows
  end
end

defmodule PostgresBackfill.Pipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message

  def start_link(opts) do
    postgres = Keyword.fetch!(opts, :postgres)

    Earthquakes.create_tables!(postgres)
    IO.puts("PostgreSQL tables ready.\n")

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {OffBroadway.Iceberg.Producer,
           managed: [
             server: [
               # Proposed upstream QuackDB option; unavailable in 0.7.x.
               init_sql: """
               INSTALL iceberg;
               LOAD iceberg;
               INSTALL httpfs;
               LOAD httpfs;
               ATTACH 'commons/pubdata' AS pubdata (
                 TYPE iceberg,
                 URI 'https://public-catalog.lakehousebox.com',
                 AUTHORIZATION_TYPE 'none'
               );
               """
             ],
             client: [pool_size: 2]
           ]
           table: "pubdata.environment.earthquakes",
           snapshot_id: Keyword.fetch!(opts, :snapshot_id),
           fetch_size: 5_000},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 4, max_demand: 1]
      ],
      context: %{postgres: postgres}
    )
  end

  # Invalid rows are data: they go to the rejects table and the batch still succeeds. A
  # batch only fails when PostgreSQL does, and then the producer counts it as failed.
  @impl true
  def handle_message(_processor, %Message{data: batch, metadata: metadata} = message, context) do
    checked = batch |> QuackDB.Columns.to_maps() |> Enum.map(&{&1, Earthquakes.validate(&1)})
    valid = for {earthquake, :ok} <- checked, do: earthquake
    rejected = for {earthquake, {:error, reason}} <- checked, do: {earthquake, reason}

    written = Earthquakes.store!(context.postgres, valid)
    Earthquakes.reject!(context.postgres, rejected)

    IO.puts(
      "Slice #{metadata.slice}, batch #{metadata.batch_index}: #{batch.num_rows} rows, #{written} written, #{length(rejected)} rejected"
    )

    message
  end
end

defmodule PostgresBackfill do
  @moduledoc false

  @events [
    [:off_broadway_iceberg, :scan, :start],
    [:off_broadway_iceberg, :scan, :stop],
    [:off_broadway_iceberg, :scan, :exception]
  ]

  def run(database_url) do
    # Before anything starts, so neither a quick completion nor an early failure is
    # missed. The handler runs inside the producer, so it only tells this process.
    :telemetry.attach_many("postgres-backfill", @events, &__MODULE__.forward/4, self())

    # This process owns the run. Trapping exits turns a supervision tree that fails to
    # start, or dies, into a message to report.
    Process.flag(:trap_exit, true)

    IO.puts("Starting DuckDB (the first run downloads it)...")

    children =
      [
          # Both pools are DBConnection pools, whose child ids would clash.
          Supervisor.child_spec(
            {Postgrex, postgres_options(database_url) ++ [name: PostgresBackfill.Postgres, pool_size: 4]},
            id: PostgresBackfill.Postgres
          ),
          {PostgresBackfill.Pipeline,
           postgres: PostgresBackfill.Postgres,
           snapshot_id: {:as_of, DateTime.utc_now()}}
        ]

    case Supervisor.start_link(children, strategy: :rest_for_one) do
      {:ok, supervisor} ->
        status = await(supervisor)
        if status == 0, do: report_tables()
        # Stops the pipeline, the PostgreSQL pool, the QuackDB pool, then DuckDB itself.
        if Process.alive?(supervisor), do: Supervisor.stop(supervisor)
        status

      {:error, reason} ->
        IO.puts(:stderr, "Could not start: #{Exception.format_exit(reason)}")
        1
    end
  end

  def forward([:off_broadway_iceberg, :scan, event], measurements, metadata, owner) do
    send(owner, {:scan, event, measurements, metadata})
  end

  defp await(supervisor) do
    receive do
      {:scan, :start, _measurements, metadata} ->
        IO.puts("Reading #{metadata.table}")
        IO.puts("Snapshot: #{metadata.snapshot_id || "none, the table has no snapshots"}\n")
        await(supervisor)

      {:scan, :stop, %{failed_batches: 0} = measurements, _metadata} ->
        report("Completed.", measurements)
        0

      {:scan, :stop, measurements, _metadata} ->
        report("Completed, but some batches failed to be written.", measurements)
        1

      {:scan, :exception, _measurements, metadata} ->
        IO.puts(:stderr, "\nScan failed: #{format(metadata.reason)}")
        1

      {:EXIT, ^supervisor, reason} ->
        IO.puts(:stderr, "\nStopped unexpectedly: #{Exception.format_exit(reason)}")
        1
    end
  end

  defp report(outcome, measurements) do
    elapsed = System.convert_time_unit(measurements.duration, :native, :millisecond) / 1000

    IO.puts("""

    #{outcome}
    Rows read: #{measurements.rows}
    Batches processed: #{measurements.batches}
    Failed batches: #{measurements.failed_batches}
    Elapsed: #{elapsed}s\
    """)
  end

  defp report_tables do
    %{rows: [[earthquakes, rejects]]} =
      Postgrex.query!(
        PostgresBackfill.Postgres,
        "SELECT (SELECT count(*) FROM earthquakes), (SELECT count(*) FROM earthquake_rejects)",
        []
      )

    IO.puts("Rows in earthquakes: #{earthquakes}")
    IO.puts("Rows in earthquake_rejects: #{rejects}")
  end

  defp postgres_options(url) do
    uri = URI.parse(url)
    [username, password] = String.split(uri.userinfo || ":", ":", parts: 2)

    [
      hostname: uri.host,
      port: uri.port || 5432,
      username: URI.decode(username),
      password: URI.decode(password),
      database: String.trim_leading(uri.path || "/postgres", "/")
    ]
  end

  defp format(exception) when is_exception(exception), do: Exception.message(exception)
  defp format(reason), do: Exception.format_exit(reason)
end

# The PostgreSQL of examples/docker-compose.yml, unless DATABASE_URL says otherwise.
"DATABASE_URL"
|> System.get_env("postgres://postgres:postgres@localhost:55432/postgres")
|> PostgresBackfill.run()
|> System.halt()
