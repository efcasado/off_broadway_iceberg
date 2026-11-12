defmodule OffBroadway.Iceberg.Producer do
  @moduledoc """
  A Broadway producer that reads one Apache Iceberg snapshot through DuckDB.

  The producer runs `SELECT * FROM table AT (VERSION => snapshot_id)` on a
  [QuackDB](https://hexdocs.pm/quackdb) connection and emits the result as
  `Broadway.Message`s, one message per fetched batch of rows. The snapshot is
  fixed for the whole scan, so commits made to the table while the scan runs
  are not read.

  An Iceberg snapshot is finite: once every row has been emitted, the producer
  stops fetching and stays idle. It does not poll for new snapshots.

  See `start_link/1` for the configuration options.
  """

  @behaviour Broadway.Producer

  use GenStage

  alias Broadway.CallerAcknowledger
  alias Broadway.Message
  alias OffBroadway.Iceberg.Scan

  require Logger

  @max_snapshot_id 0x7FFFFFFFFFFFFFFF

  @opts_schema [
    connection: [
      type: {:custom, __MODULE__, :validate_connection, []},
      required: true,
      doc: """
      The QuackDB connection pool to read through: the pid or registered name of a pool
      started with `QuackDB.start_link/1` or `QuackDB.Server.child_specs/1`. The pool
      belongs to your application; the producer checks out one connection for the length
      of the scan and never stops the pool. An `Ecto.Repo` is not accepted.
      """
    ],
    table: [
      type: {:custom, __MODULE__, :validate_table, []},
      required: true,
      doc: """
      The Iceberg table to read, as a dot-separated name such as
      `"lake.betting.events"` (catalog, schema and table) or a list of name parts such as
      `["lake", "betting", "events"]`. The dotted form accepts plain identifiers only
      (letters, digits and underscores, not starting with a digit); use the list form for
      names that contain other characters. Every part is quoted before it reaches SQL.
      """
    ],
    snapshot_id: [
      type: {:custom, __MODULE__, :validate_snapshot_id, []},
      default: :current,
      doc: """
      The snapshot to read. `:current` resolves the snapshot the table's `main` branch
      points at, once, when the scan starts. An integer reads that snapshot.
      """
    ],
    fetch_size: [
      type: :pos_integer,
      default: 10_000,
      doc: """
      The maximum number of rows in one message. See "Batches" below. This is not
      Broadway's batcher `:batch_size`, which counts messages.
      """
    ]
  ]

  @doc """
  Starts an `OffBroadway.Iceberg` producer process linked to the current process.

  ## Configuration

  #{NimbleOptions.docs(@opts_schema)}

  Options are validated in `c:Broadway.Producer.prepare_for_start/2`, so
  `Broadway.start_link/2` fails with the configuration error before any stage starts.
  The producer only works
  with `producer: [concurrency: 1]`: each producer stage would scan the whole snapshot,
  so more than one would deliver every row more than once.

  ## DuckDB setup

  DuckDB must have the `iceberg` extension loaded and the catalog attached before the
  scan starts. Both apply to the whole DuckDB instance, so running them on any
  connection of the pool is enough:

      QuackDB.query!(MyApp.QuackDB, "INSTALL iceberg")
      QuackDB.query!(MyApp.QuackDB, "LOAD iceberg")

      QuackDB.query!(
        MyApp.QuackDB,
        "ATTACH IF NOT EXISTS 'warehouse' AS lake " <>
          "(TYPE iceberg, URI 'http://localhost:8181', AUTHORIZATION_TYPE 'none')"
      )

  `AUTHORIZATION_TYPE 'none'` suits a catalog without authentication. See DuckDB's
  Iceberg documentation for catalogs that need credentials.

  Only tables of an attached Iceberg catalog are supported. Paths read with
  `iceberg_scan(...)` are not.

  ## Snapshots

  With `snapshot_id: :current`, the scan reads `iceberg_refs(table)` and pins the
  snapshot of the `main` branch. That is the table's current snapshot, which after a
  rollback is not necessarily the newest one. A table with no snapshots yields no
  messages. With an explicit id, a snapshot that does not exist fails the scan.

  The snapshot is resolved before the first fetch and every row is read with
  `AT (VERSION => snapshot_id)`, so the scan never sees later commits.

  ## Batches

  Each message carries one `QuackDB.Columns` struct in `:data`: the rows of the batch
  as one list per column. Use `batch.num_rows`, `QuackDB.Columns.get/3`, or
  `QuackDB.Columns.to_rows/1` and `QuackDB.Columns.to_maps/1` when you need rows. The
  values are already decoded Elixir terms, not Arrow buffers.

  `:fetch_size` is passed to QuackDB as `:max_rows`. A message holds at most
  `:fetch_size` rows and may hold fewer: at the end of the scan, and wherever a round
  trip to DuckDB ends. DuckDB sends results in whole vectors of 2,048 rows. The response
  to the query carries at least one vector, and each later round trip carries vectors up
  to about `quack_target_batch_bytes` (a DuckDB setting, 32 MiB by default), and at
  least one. QuackDB keeps the rows of a round trip that do not fit in one message on the
  connection, and hands them out on later fetches without asking DuckDB again. Batches
  with no rows are not emitted.

  Each message has the following `:metadata`:

  - `:table` - The `:table` option, as given
  - `:snapshot_id` - The snapshot the scan reads
  - `:batch_index` - The position of the batch in the scan, starting at 0

  ## Backpressure

  The producer fetches a batch only for demand Broadway has already sent, and holds no
  rows itself. One worker process per producer owns the QuackDB cursor and fetches one
  batch at a time. It waits whenever there is no outstanding demand, so a slow pipeline
  pauses the scan.

  The BEAM holds the messages in flight, plus at most one round trip from DuckDB that has
  not been handed out yet. The messages in flight are bounded by Broadway's demand: up
  to `max_demand` messages per processor, each with up to `:fetch_size` rows. Batch
  messages are large, so consider a small `max_demand` for the processors, such as `1`
  or `2`. While the scan waits, QuackDB keeps the connection's lease alive, so a paused
  scan does not expire.

  ## Acknowledgements

  Messages are acknowledged back to the producer with `Broadway.CallerAcknowledger`.
  An acknowledgement only tells the producer that a batch was processed. It is not
  stored anywhere, and neither DuckDB nor Iceberg take part in it:

  - Failed batches are logged as warnings and counted in the `:stop` event below. They
    are not retried.
  - Once every batch is acknowledged, successfully or not, the scan is complete.

  ## Completion, failure and restarts

  The worker notices the end of the snapshot on the fetch after the last batch, which it
  makes once Broadway asks for more messages, as processors do after handling theirs. It
  then releases its connection. The scan is complete once every emitted batch has also
  been acknowledged. The producer then stays idle: further demand is ignored and the
  snapshot is never scanned again. Broadway has no way for a producer
  to stop its own pipeline, so stop it yourself, for example from a handler of the
  `[:off_broadway_iceberg, :scan, :stop]` event below. Call `Broadway.stop/1` from
  another process, not from the handler itself, which runs inside the producer.

  A query or connection error fails the scan. The producer logs the error, emits
  `[:off_broadway_iceberg, :scan, :exception]` and stays idle, without starting over.
  The batches emitted before the error are still processed and acknowledged.

  Progress is not persisted. A producer that crashes is restarted by Broadway and scans
  the snapshot again from the start, and so does a pipeline started again after a
  failure. Make downstream writes idempotent, keyed for example by the snapshot id and
  the row's own key, so a replay is harmless. This producer does not provide
  exactly-once processing.

  When the pipeline shuts down mid-scan, the producer stops the worker. DBConnection
  then disconnects the connection it held, which cancels the query on the DuckDB
  server, and the pool reconnects it.

  ## Telemetry

  The producer emits the following events:

  - `[:off_broadway_iceberg, :scan, :start]` - The snapshot is resolved and the scan
    begins. Measurements: `:system_time`. Metadata: `:name`, `:table`, `:snapshot_id`.
  - `[:off_broadway_iceberg, :scan, :stop]` - Every batch has been emitted and
    acknowledged. Measurements: `:duration`, `:batches`, `:rows`, `:failed_batches`.
    Metadata: `:name`, `:table`, `:snapshot_id`.
  - `[:off_broadway_iceberg, :scan, :exception]` - The scan failed. Measurements:
    `:duration`, `:batches`, `:rows`. Metadata: `:name`, `:table`, `:snapshot_id`
    (`nil` if the scan failed before resolving it), `:kind` and `:reason`.

  `:name` is the Broadway pipeline name, or `nil` outside Broadway. Durations are in
  `:native` time units.

  A `:stop` event with `failed_batches: 0` means every emitted batch was processed
  successfully. With `failed_batches` above 0, the scan read the whole snapshot, but that
  many batches failed downstream. A scan that could not read the snapshot emits
  `:exception` instead, never `:stop`.

  Attach handlers before starting the pipeline. The scan starts with the producer, and
  one that fails early emits `:exception` right away. The events are the only record of
  a scan's outcome: the producer stays idle whether the scan completed or failed. If you
  need the outcome later, persist it yourself.

  QuackDB emits its own `[:quackdb, :query, ...]` and `[:quackdb, :fetch, ...]` events.
  """
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl Broadway.Producer
  def prepare_for_start(_module, broadway_opts) do
    producer = Keyword.fetch!(broadway_opts, :producer)
    {_producer_module, producer_opts} = Keyword.fetch!(producer, :module)

    validate_options!(producer_opts)

    case Keyword.get(producer, :concurrency, 1) do
      1 ->
        {[], broadway_opts}

      concurrency ->
        raise ArgumentError,
              "#{inspect(__MODULE__)} requires producer concurrency 1, got: #{inspect(concurrency)}. " <>
                "Every producer stage would scan the whole snapshot."
    end
  end

  @impl GenStage
  def init(opts) do
    name = opts |> Keyword.get(:broadway, []) |> Keyword.get(:name)
    opts = validate_options!(opts)
    ref = make_ref()

    state = %{
      ref: ref,
      scan: Scan.start_link(self(), ref, opts),
      name: name,
      table: Keyword.fetch!(opts, :table),
      snapshot_id: nil,
      # :resolving -> :scanning -> :exhausted -> :completed, or :failed / :cancelled.
      status: :resolving,
      started_at: System.monotonic_time(),
      # Demand Broadway has sent and batches the worker has been asked for, neither yet
      # met with a batch. The worker is never asked for more than the demand.
      demand: 0,
      requested: 0,
      emitted: 0,
      rows: 0,
      acked: 0,
      failed: 0
    }

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    {:noreply, [], request_batches(%{state | demand: state.demand + incoming_demand})}
  end

  @impl GenStage
  def handle_info({Scan, ref, {:snapshot, snapshot_id}}, %{ref: ref, status: :resolving} = state) do
    Logger.debug("Scanning #{inspect(state.table)} at snapshot #{inspect(snapshot_id)}")

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :start],
      %{system_time: System.system_time()},
      event_metadata(%{state | snapshot_id: snapshot_id})
    )

    {:noreply, [], %{state | status: :scanning, snapshot_id: snapshot_id}}
  end

  def handle_info({Scan, ref, {:batch, batch}}, %{ref: ref, status: :scanning} = state) do
    message = %Message{
      data: batch,
      metadata: %{table: state.table, snapshot_id: state.snapshot_id, batch_index: state.emitted},
      acknowledger: CallerAcknowledger.init({self(), ref}, state.emitted)
    }

    state = %{
      state
      | demand: state.demand - 1,
        requested: state.requested - 1,
        emitted: state.emitted + 1,
        rows: state.rows + batch.num_rows
    }

    {:noreply, [message], state}
  end

  def handle_info({Scan, ref, :done}, %{ref: ref, status: :scanning} = state) do
    Logger.debug("Read every row of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)}")

    {:noreply, [], maybe_complete(%{state | status: :exhausted})}
  end

  def handle_info({Scan, ref, {:error, reason}}, %{ref: ref, status: status} = state)
      when status in [:resolving, :scanning] do
    Logger.error(
      "Scan of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)} failed after " <>
        "#{state.emitted} batches and will not be retried: #{format_reason(reason)}"
    )

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :exception],
      %{duration: duration(state), batches: state.emitted, rows: state.rows},
      Map.merge(event_metadata(state), %{kind: :error, reason: reason})
    )

    {:noreply, [], %{state | status: :failed}}
  end

  # A cancelled scan's worker may have sent a batch before it was stopped.
  def handle_info({Scan, ref, _event}, %{ref: ref} = state), do: {:noreply, [], state}

  def handle_info({:ack, ref, successful, failed}, %{ref: ref} = state) do
    if failed != [] do
      indexes = Enum.map(failed, & &1.metadata.batch_index)

      Logger.warning(
        "#{length(failed)} batches of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)} " <>
          "failed and will not be retried: #{inspect(indexes, charlists: :as_lists)}"
      )
    end

    state = %{state | acked: state.acked + length(successful), failed: state.failed + length(failed)}

    {:noreply, [], maybe_complete(state)}
  end

  def handle_info(message, state) do
    Logger.warning("Producer received an unexpected message: #{inspect(message)}")

    {:noreply, [], state}
  end

  # Broadway stops asking for messages and drains the ones it has, so stop fetching too.
  @impl Broadway.Producer
  def prepare_for_draining(%{status: status} = state) when status in [:resolving, :scanning] do
    Scan.cancel(state.scan)

    {:noreply, [], %{state | status: :cancelled}}
  end

  def prepare_for_draining(state), do: {:noreply, [], state}

  # A supervisor shutdown takes the worker down through the link. A normal stop does not
  # propagate through a link, so stop the worker explicitly.
  @impl GenStage
  def terminate(_reason, state) do
    Scan.cancel(state.scan)
  end

  @doc false
  def validate_connection(connection)
      when is_pid(connection) or (is_atom(connection) and connection not in [nil, true, false]), do: {:ok, connection}

  def validate_connection({:global, _name} = connection), do: {:ok, connection}
  def validate_connection({:via, module, _name} = connection) when is_atom(module), do: {:ok, connection}

  def validate_connection(connection) do
    {:error, "expected a QuackDB pool pid or registered name, got: #{inspect(connection)}"}
  end

  @doc false
  def validate_table(table) do
    if Scan.table_parts(table) == :error do
      {:error,
       "expected a table name such as \"catalog.schema.table\" made of plain identifiers, " <>
         "or a list of one to three non-empty name parts, got: #{inspect(table)}"}
    else
      {:ok, table}
    end
  end

  @doc false
  def validate_snapshot_id(:current), do: {:ok, :current}

  def validate_snapshot_id(snapshot_id) when is_integer(snapshot_id) and snapshot_id in 0..@max_snapshot_id,
    do: {:ok, snapshot_id}

  def validate_snapshot_id(snapshot_id) do
    {:error, "expected :current or a snapshot id between 0 and #{@max_snapshot_id}, got: #{inspect(snapshot_id)}"}
  end

  defp validate_options!(opts) do
    # Broadway injects :broadway (pipeline name and stage index) into the producer's options.
    {_broadway, opts} = Keyword.pop(opts, :broadway)

    NimbleOptions.validate!(opts, @opts_schema)
  end

  defp request_batches(%{status: status, demand: demand, requested: requested} = state)
       when status in [:resolving, :scanning] and demand > requested do
    Scan.fetch(state.scan, state.ref, demand - requested)

    %{state | requested: demand}
  end

  defp request_batches(state), do: state

  defp maybe_complete(%{status: :exhausted, emitted: emitted, acked: acked, failed: failed} = state)
       when acked + failed == emitted do
    Logger.debug("Scan of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)} completed")

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :stop],
      %{duration: duration(state), batches: emitted, rows: state.rows, failed_batches: failed},
      event_metadata(state)
    )

    %{state | status: :completed}
  end

  defp maybe_complete(state), do: state

  defp event_metadata(state) do
    %{name: state.name, table: state.table, snapshot_id: state.snapshot_id}
  end

  defp duration(state), do: System.monotonic_time() - state.started_at

  defp format_reason(exception) when is_exception(exception), do: Exception.message(exception)
  defp format_reason(reason), do: inspect(reason)
end
