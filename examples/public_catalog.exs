#!/usr/bin/env elixir

# Public Iceberg catalog example
#
# Reads one table of LakehouseBox's public Iceberg REST catalog
# (https://lakehousebox.com/docs/sample-data/) with a Broadway pipeline. QuackDB runs
# DuckDB under the script's own supervision tree, so there is nothing to install or
# start besides Elixir: no credentials, no Docker, no DuckDB server.
#
# Usage, from the repository root:
#   elixir examples/public_catalog.exs                       # environment.earthquakes
#   elixir examples/public_catalog.exs reference.countries   # any table of the catalog
#
# The script exits on its own: with status 0 once every batch has been processed, and
# with status 1 if the scan failed or any batch did. See examples/README.md.

Mix.install([
  {:off_broadway_iceberg, path: Path.join(__DIR__, "..")}
])

Logger.configure(level: :warning)

defmodule PublicCatalog.Backfill do
  @moduledoc false

  use Broadway

  alias Broadway.Message

  def start_link(opts) do
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
           ],
           table: "pubdata." <> Keyword.fetch!(opts, :table),
           snapshot_id: Keyword.fetch!(opts, :snapshot_id),
           fetch_size: 5_000},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 2, max_demand: 1]
      ]
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: batch, metadata: metadata} = message, _context) do
    IO.puts(
      "Slice #{metadata.slice}, batch #{metadata.batch_index}: #{batch.num_rows} rows#{strongest_earthquake(batch)}"
    )

    message
  end

  # The batch is a QuackDB.Columns: one list per column. Read the two columns needed,
  # without turning rows into maps. Tables without them get no detail.
  defp strongest_earthquake(batch) do
    with {:ok, magnitudes} <- QuackDB.Columns.fetch(batch, "magnitude"),
         {:ok, places} <- QuackDB.Columns.fetch(batch, "place"),
         [_ | _] = earthquakes <- magnitudes |> Enum.zip(places) |> Enum.reject(&is_nil(elem(&1, 0))) do
      {magnitude, place} = Enum.max_by(earthquakes, &elem(&1, 0))
      ", strongest M#{magnitude} #{place}"
    else
      _no_earthquakes -> ""
    end
  end
end

defmodule PublicCatalog do
  @moduledoc false

  @events [
    [:off_broadway_iceberg, :scan, :start],
    [:off_broadway_iceberg, :scan, :stop],
    [:off_broadway_iceberg, :scan, :exception]
  ]

  def run(table) do
    # Before anything starts, so neither a quick completion nor an early failure is
    # missed. The handler runs inside the producer, so it only tells this process.
    :telemetry.attach_many("public-catalog", @events, &__MODULE__.forward/4, self())

    # This process owns the run. Trapping exits turns a supervision tree that fails to
    # start, or dies, into a message to report.
    Process.flag(:trap_exit, true)

    IO.puts("Starting DuckDB (the first run downloads it)...")

    children =
      [
          {PublicCatalog.Backfill,
           table: table, snapshot_id: {:as_of, DateTime.utc_now()}}
        ]

    case Supervisor.start_link(children, strategy: :rest_for_one) do
      {:ok, supervisor} ->
        status = await(supervisor)
        # Stopping Broadway also stops its managed QuackDB pool and DuckDB server.
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
        report("Completed, but some batches failed downstream.", measurements)
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
    Rows processed: #{measurements.rows}
    Batches processed: #{measurements.batches}
    Failed batches: #{measurements.failed_batches}
    Elapsed: #{elapsed}s\
    """)
  end

  defp format(exception) when is_exception(exception), do: Exception.message(exception)
  defp format(reason), do: Exception.format_exit(reason)
end

System.argv()
|> List.first("environment.earthquakes")
|> PublicCatalog.run()
|> System.halt()
