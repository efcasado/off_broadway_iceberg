#!/usr/bin/env elixir

# Checkpointed backfill example
#
# Reads places.admin_areas of LakehouseBox's public Iceberg catalog, a table of three data
# files, with a checkpointed scan that records each finished slice in a GCS emulator. Stop
# it midway and run it again: it only reads the slices that no earlier run finished.
#
# Usage, from the repository root:
#   docker compose -f examples/docker-compose.yml up --detach --wait fake-gcs
#   elixir examples/checkpointed_backfill.exs --crash-after 1   # halts once a slice is recorded
#   elixir examples/checkpointed_backfill.exs                   # reads the other slices
#   elixir examples/checkpointed_backfill.exs                   # reads nothing: the job is done
#
# CHECKPOINT_ENDPOINT changes the emulator's URL (default http://localhost:4443). The job
# is named after the table's current snapshot, so it starts over whenever the catalog
# publishes a new one. See examples/README.md.

Mix.install([
  {:off_broadway_iceberg, path: Path.join(__DIR__, "..")},
  {:req, "~> 0.7.5"}
])

Logger.configure(level: :warning)

defmodule CheckpointedBackfill.Pipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message

  @table ["pubdata", "places", "admin_areas"]

  def start_link(opts) do
    connection = Keyword.fetch!(opts, :connection)

    IO.puts("Connecting to the public Iceberg catalog...")

    for extension <- ["iceberg", "httpfs"] do
      QuackDB.query!(connection, "INSTALL #{extension}")
      QuackDB.query!(connection, "LOAD #{extension}")
    end

    QuackDB.query!(
      connection,
      "ATTACH IF NOT EXISTS 'commons/pubdata' AS pubdata " <>
        "(TYPE iceberg, URI 'https://public-catalog.lakehousebox.com', AUTHORIZATION_TYPE 'none')"
    )

    # A checkpointed job reads one snapshot on every run, so pin the current one, and
    # name the job after it. With a :slice_rows of 1, every data file makes a slice.
    snapshot_id = current_snapshot(connection)

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {OffBroadway.Iceberg.Producer,
           connection: connection,
           table: @table,
           snapshot_id: snapshot_id,
           fetch_size: 1_000,
           distribute: [slice_rows: 1],
           checkpoint: [
             job_id: "admin-areas-#{snapshot_id}",
             backend:
               {OffBroadway.Iceberg.Checkpoint.GCS,
                bucket: Keyword.fetch!(opts, :bucket),
                prefix: "examples",
                endpoint: Keyword.fetch!(opts, :endpoint),
                auth: :none}
           ]},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 2, max_demand: 1]
      ]
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: batch, metadata: metadata} = message, _context) do
    IO.puts("Slice #{metadata.slice}, batch #{metadata.batch_index}: #{batch.num_rows} rows")
    message
  end

  defp current_snapshot(connection) do
    table = Enum.map_join(@table, ".", &~s("#{&1}"))
    QuackDB.query!(connection, "SELECT * FROM #{table} LIMIT 0")

    %{rows: [[snapshot_id]]} =
      QuackDB.query!(connection, "SELECT snapshot_id FROM iceberg_refs(?) WHERE name = 'main'", [table])

    snapshot_id
  end
end

defmodule CheckpointedBackfill do
  @moduledoc false

  @bucket "checkpoints"
  @events [
    [:off_broadway_iceberg, :scan, :stop],
    [:off_broadway_iceberg, :scan, :exception],
    [:off_broadway_iceberg, :checkpoint, :load],
    [:off_broadway_iceberg, :checkpoint, :write]
  ]

  def run(args) do
    {opts, _args} = OptionParser.parse!(args, strict: [crash_after: :integer])
    endpoint = System.get_env("CHECKPOINT_ENDPOINT", "http://localhost:4443")

    with :ok <- create_bucket(endpoint) do
      # The handlers run inside the producer, so they only tell this process.
      :telemetry.attach_many("checkpointed-backfill", @events, &__MODULE__.forward/4, self())
      Process.flag(:trap_exit, true)

      IO.puts("Starting DuckDB (the first run downloads it)...")

      children =
        QuackDB.Server.child_specs(
          server: [name: CheckpointedBackfill.DuckDB, duckdb: :managed],
          client: [name: CheckpointedBackfill.QuackDB, pool_size: 2]
        ) ++
          [
            {CheckpointedBackfill.Pipeline, connection: CheckpointedBackfill.QuackDB, bucket: @bucket, endpoint: endpoint}
          ]

      case Supervisor.start_link(children, strategy: :rest_for_one) do
        {:ok, supervisor} ->
          status = await(supervisor, Keyword.get(opts, :crash_after), 0)
          if Process.alive?(supervisor), do: Supervisor.stop(supervisor)
          status

        {:error, reason} ->
          IO.puts(:stderr, "Could not start: #{Exception.format_exit(reason)}")
          1
      end
    end
  end

  def forward([:off_broadway_iceberg, kind, event], measurements, metadata, owner) do
    send(owner, {kind, event, measurements, metadata})
  end

  # The emulator starts without buckets. 409 means the bucket exists already.
  defp create_bucket(endpoint) do
    case Req.post("#{endpoint}/storage/v1/b", json: %{name: @bucket}, retry: false) do
      {:ok, %{status: status}} when status in [200, 409] ->
        :ok

      other ->
        IO.puts(:stderr, """
        Could not reach the GCS emulator at #{endpoint}: #{inspect(other)}
        Start it with: docker compose -f examples/docker-compose.yml up --detach --wait fake-gcs\
        """)

        1
    end
  end

  defp await(supervisor, crash_after, recorded) do
    receive do
      {:checkpoint, :load, %{slices: slices, finished: finished}, metadata} ->
        IO.puts("""

        Job #{metadata.job_id}, snapshot #{metadata.snapshot_id}
        #{finished} of #{slices} slices finished by earlier runs
        """)

        await(supervisor, crash_after, recorded)

      {:checkpoint, :write, measurements, metadata} ->
        IO.puts("Recorded slice #{metadata.slice}: #{measurements.batches} batches, #{measurements.rows} rows")

        if recorded + 1 == crash_after do
          IO.puts("\nHalting, as a crash would. Run the script again to resume.")
          System.halt(1)
        end

        await(supervisor, crash_after, recorded + 1)

      {:scan, :stop, measurements, _metadata} ->
        IO.puts("""

        Completed.
        Slices recorded by this run: #{recorded}
        Rows of the snapshot: #{measurements.rows}
        Failed batches: #{measurements.failed_batches}\
        """)

        if measurements.failed_batches == 0, do: 0, else: 1

      {:scan, :exception, _measurements, metadata} ->
        IO.puts(:stderr, "\nScan failed: #{format(metadata.reason)}")
        1

      {:EXIT, ^supervisor, reason} ->
        IO.puts(:stderr, "\nStopped unexpectedly: #{Exception.format_exit(reason)}")
        1
    end
  end

  defp format(exception) when is_exception(exception), do: Exception.message(exception)
  defp format(reason), do: Exception.format_exit(reason)
end

System.argv()
|> CheckpointedBackfill.run()
|> System.halt()
