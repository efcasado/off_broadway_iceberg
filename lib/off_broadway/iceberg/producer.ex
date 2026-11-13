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

  With `:distribute`, the producers of the same pipeline on every connected node
  share one scan between them. See "Distributed scans" in `start_link/1`.

  See `start_link/1` for the configuration options.
  """

  @behaviour Broadway.Producer

  use GenStage

  alias Broadway.CallerAcknowledger
  alias Broadway.Message
  alias OffBroadway.Iceberg.Distribution
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
      belongs to your application; the producer checks out one connection at a time and
      never stops the pool. An `Ecto.Repo` is not accepted.
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
      `{:as_of, datetime}` reads the snapshot that was the table's current one at that
      time.
      """
    ],
    fetch_size: [
      type: :pos_integer,
      default: 10_000,
      doc: """
      The maximum number of rows in one message. See "Batches" below. This is not
      Broadway's batcher `:batch_size`, which counts messages.
      """
    ],
    distribute: [
      type: :keyword_list,
      keys: [
        key: [
          type: {:custom, __MODULE__, :validate_key, []},
          required: true,
          doc: "The column, or list of columns, that rows are assigned to slices by."
        ],
        slices: [
          type: :pos_integer,
          default: 64,
          doc: "The number of slices the snapshot is divided into."
        ]
      ],
      doc: """
      Shares the scan between the producers of this pipeline on every connected node.
      See "Distributed scans" below. Requires an integer or `{:as_of, datetime}`
      `:snapshot_id`.
      """
    ]
  ]

  @doc """
  Starts an `OffBroadway.Iceberg` producer process linked to the current process.

  ## Configuration

  #{NimbleOptions.docs(@opts_schema)}

  Options are validated in `c:Broadway.Producer.prepare_for_start/2`, so
  `Broadway.start_link/2` fails with the configuration error before any stage starts.
  Without `:distribute`, the producer only works with `producer: [concurrency: 1]`:
  each producer stage would scan the whole snapshot, so more than one would deliver
  every row more than once.

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

  With `{:as_of, datetime}`, the scan reads `iceberg_history(table)`, the log of which
  snapshot was current when, and pins the one that was current at `datetime`. Every
  node resolves it to the same snapshot. Before the table's first snapshot, there is no
  snapshot to read and the scan yields no messages.

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
  - `:batch_index` - The position of the batch in the scan, or in its slice, starting
    at 0
  - `:slice` - With `:distribute` only, the slice the batch belongs to

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

  ## Distributed scans

  > #### Experimental {: .warning}
  >
  > Distributed scans are new. Their behavior may change.

  With `:distribute`, the snapshot is divided into slices by hashing the key columns:
  slice `i` of `n` holds the rows where `hash(key) % n = i`. Every producer stage of the
  pipeline, on every connected node, joins a process group (OTP's `:pg`) for the scan
  and reads the slices it owns, so `producer: [concurrency: 2]` on three nodes makes six
  members. A slice belongs to the member that wins rendezvous hashing over the group's
  current members. When nodes join or leave, for example as an autoscaler adds or
  removes pods, the slices of the members that came or went move to others, and the
  rest stay where they are.

  The nodes must be connected to each other, as Phoenix's `DNSCluster` and `libcluster`
  connect them. A node only sees the members on the nodes it is connected to, and a
  member that sees no others reads every slice itself.

  A member that finishes a slice, with every batch acknowledged, tells the others, and
  members tell every member that joins what they know. When every slice is finished,
  every member emits `:stop`, with the counts of the whole scan. A member that joins
  after that emits `:stop` without reading anything.

  The members agree on the scan without any shared storage, which comes with trade-offs:

  - All producers must use the same `:table`, `:snapshot_id` and `:distribute`, and the
    same pipeline name. Those make up the group, so producers that differ in any of
    them scan separately.
  - `:snapshot_id` must be an integer or `{:as_of, datetime}`. Producers resolving
    `:current` at different times could read different snapshots.
  - Every slice scans the whole snapshot in DuckDB, which filters the rows of the other
    slices. What the slices divide is the transfer and decoding of rows, and their
    processing.
  - A slice can be read more than once. A member keeps reading a slice it no longer
    owns, and stops when another member finishes it first. A member that fails, or
    stops, leaves the group, and the slices it had not finished are read by others from
    the start. Writes must be idempotent.
  - Progress lives in the members. If every member stops at once, for example in a
    full redeploy, the next members scan the snapshot again.
  - Every node must run the same DuckDB version, because `hash/1` must agree everywhere.

  A failed slice fails only the member that read it: it emits `:exception`, leaves the
  group, and stays idle. The slice is read again by another member.

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
  `:native` time units. In a distributed scan, the counts of `:stop` cover every slice,
  wherever it was read, and those of `:exception` the batches this producer emitted.

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

    distributed? = producer_opts |> validate_options!() |> Keyword.get(:distribute) != nil

    case Keyword.get(producer, :concurrency, 1) do
      1 ->
        {[], broadway_opts}

      _concurrency when distributed? ->
        {[], broadway_opts}

      concurrency ->
        raise ArgumentError,
              "#{inspect(__MODULE__)} requires producer concurrency 1 without :distribute, " <>
                "got: #{inspect(concurrency)}. Every producer stage would scan the whole snapshot."
    end
  end

  @impl GenStage
  def init(opts) do
    name = opts |> Keyword.get(:broadway, []) |> Keyword.get(:name)
    {distribute, scan_opts} = opts |> validate_options!() |> Keyword.pop(:distribute)

    state = %{
      name: name,
      table: Keyword.fetch!(scan_opts, :table),
      scan_opts: scan_opts,
      snapshot_id: nil,
      started?: false,
      # :running -> :completed, or :failed / :cancelled.
      status: :running,
      started_at: System.monotonic_time(),
      # Demand Broadway has sent and batches the current worker has been asked for,
      # neither yet met with a batch. The worker is never asked for more than the demand.
      demand: 0,
      requested: 0,
      # The worker reading a slice, if any: %{pid, ref, slice}.
      scan: nil,
      # Slices this producer has read whose batches are not all acknowledged yet, by the
      # reference their messages are acknowledged with.
      reads: %{},
      emitted: 0,
      rows: 0,
      # A scan without :distribute is a single slice that only this producer reads.
      key: nil,
      slices: 1,
      group: nil,
      monitor: nil,
      members: [self()],
      done: %{}
    }

    {:producer, state |> join(distribute) |> next_slice()}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    {:noreply, [], request_batches(%{state | demand: state.demand + incoming_demand})}
  end

  @impl GenStage
  def handle_info({Scan, ref, {:snapshot, snapshot_id}}, %{scan: %{ref: ref}} = state) do
    {:noreply, [], start_scan(state, snapshot_id)}
  end

  def handle_info({Scan, ref, {:batch, batch}}, %{scan: %{ref: ref, slice: slice}} = state) do
    read = Map.fetch!(state.reads, ref)
    metadata = %{table: state.table, snapshot_id: state.snapshot_id, batch_index: read.emitted}
    metadata = if state.group, do: Map.put(metadata, :slice, slice), else: metadata

    message = %Message{
      data: batch,
      metadata: metadata,
      acknowledger: CallerAcknowledger.init({self(), ref}, read.emitted)
    }

    read = %{read | emitted: read.emitted + 1, rows: read.rows + batch.num_rows}

    state = %{
      state
      | reads: Map.put(state.reads, ref, read),
        demand: state.demand - 1,
        requested: state.requested - 1,
        emitted: state.emitted + 1,
        rows: state.rows + batch.num_rows
    }

    {:noreply, [message], state}
  end

  def handle_info({Scan, ref, :done}, %{scan: %{ref: ref}} = state) do
    reads = Map.update!(state.reads, ref, &%{&1 | exhausted?: true})
    state = %{state | scan: nil, requested: 0, reads: reads}

    {:noreply, [], state |> finish_read(ref) |> next_slice() |> maybe_complete()}
  end

  def handle_info({Scan, ref, {:error, reason}}, %{scan: %{ref: ref, slice: slice}} = state) do
    Logger.error(
      "Scan of #{describe(state, slice)} failed after #{state.emitted} batches and will not be " <>
        "retried#{if state.group, do: " by this producer", else: ""}: #{format_reason(reason)}"
    )

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :exception],
      %{duration: duration(state), batches: state.emitted, rows: state.rows},
      Map.merge(event_metadata(state), %{kind: :error, reason: reason})
    )

    {:noreply, [], leave(%{state | scan: nil, requested: 0, status: :failed})}
  end

  # A worker that was stopped may have sent messages before it stopped.
  def handle_info({Scan, _ref, _event}, state), do: {:noreply, [], state}

  def handle_info({:ack, ref, successful, failed}, state) when is_map_key(state.reads, ref) do
    read = Map.fetch!(state.reads, ref)

    if failed != [] do
      indexes = Enum.map(failed, & &1.metadata.batch_index)

      Logger.warning(
        "#{length(failed)} batches of #{describe(state, read.slice)} failed and will not be retried: " <>
          inspect(indexes, charlists: :as_lists)
      )
    end

    read = %{read | acked: read.acked + length(successful), failed: read.failed + length(failed)}
    state = %{state | reads: Map.put(state.reads, ref, read)}

    {:noreply, [], state |> finish_read(ref) |> next_slice() |> maybe_complete()}
  end

  def handle_info({Distribution, group, {:done, done}}, %{group: group} = state) do
    state = %{state | done: Distribution.merge(state.done, done)}

    {:noreply, [], state |> drop_finished_slice() |> next_slice() |> maybe_complete()}
  end

  def handle_info({monitor, :join, group, pids}, %{monitor: monitor, group: group} = state) do
    # Every member tells those that join what it knows.
    if state.done != %{}, do: Distribution.announce(pids, group, state.done)

    {:noreply, [], next_slice(%{state | members: Enum.uniq(state.members ++ pids)})}
  end

  def handle_info({monitor, :leave, group, pids}, %{monitor: monitor, group: group} = state) do
    {:noreply, [], next_slice(%{state | members: state.members -- pids})}
  end

  def handle_info(message, state) do
    Logger.warning("Producer received an unexpected message: #{inspect(message)}")

    {:noreply, [], state}
  end

  # Broadway stops asking for messages and drains the ones it has, so stop fetching too.
  # A distributed producer also leaves its group, so the others take its slices over.
  @impl Broadway.Producer
  def prepare_for_draining(%{status: :running} = state) do
    {:noreply, [], state |> stop_scan() |> leave() |> Map.put(:status, :cancelled)}
  end

  def prepare_for_draining(state), do: {:noreply, [], state}

  # A supervisor shutdown takes the worker down through the link. A normal stop does not
  # propagate through a link, so stop the worker explicitly.
  @impl GenStage
  def terminate(_reason, state) do
    stop_scan(state)
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

  def validate_snapshot_id({:as_of, %DateTime{}} = as_of), do: {:ok, as_of}

  def validate_snapshot_id(snapshot_id) do
    {:error,
     "expected :current, {:as_of, datetime} or a snapshot id between 0 and #{@max_snapshot_id}, " <>
       "got: #{inspect(snapshot_id)}"}
  end

  @doc false
  def validate_key(key) do
    case key do
      column when is_binary(column) -> validate_key([column])
      [_ | _] = columns -> if Enum.all?(columns, &Scan.name_part?/1), do: {:ok, columns}, else: invalid_key(key)
      _other -> invalid_key(key)
    end
  end

  defp invalid_key(key), do: {:error, "expected a column name or a non-empty list of them, got: #{inspect(key)}"}

  defp validate_options!(opts) do
    # Broadway injects :broadway (pipeline name and stage index) into the producer's options.
    {_broadway, opts} = Keyword.pop(opts, :broadway)
    opts = NimbleOptions.validate!(opts, @opts_schema)

    if opts[:distribute] && opts[:snapshot_id] == :current do
      raise ArgumentError,
            "#{inspect(__MODULE__)} requires an integer or {:as_of, datetime} :snapshot_id with :distribute, " <>
              "so that every producer reads the same snapshot"
    end

    opts
  end

  defp join(state, nil), do: state

  defp join(state, distribute) do
    key = Keyword.fetch!(distribute, :key)
    slices = Keyword.fetch!(distribute, :slices)
    snapshot_id = Keyword.fetch!(state.scan_opts, :snapshot_id)
    group = {state.name, Scan.table_parts(state.table), snapshot_id, key, slices}
    {monitor, members} = Distribution.join(group)

    %{state | key: key, slices: slices, group: group, monitor: monitor, members: members}
  end

  defp leave(%{group: nil} = state), do: state

  defp leave(state) do
    Distribution.leave(state.group, state.monitor)
    %{state | monitor: nil}
  end

  # Starts reading the first slice this producer owns that is neither finished nor read
  # by it already, if it is not reading one.
  defp next_slice(%{status: :running, scan: nil} = state) do
    read = MapSet.new(Map.values(state.reads), & &1.slice)

    slice =
      Enum.find(0..(state.slices - 1)//1, fn slice ->
        not is_map_key(state.done, slice) and not MapSet.member?(read, slice) and owner?(state, slice)
      end)

    if slice, do: read_slice(state, slice), else: state
  end

  defp next_slice(state), do: state

  defp owner?(%{group: nil}, _slice), do: true
  defp owner?(state, slice), do: Distribution.owner(state.group, slice, state.members) == self()

  defp read_slice(state, slice) do
    ref = make_ref()
    filter = if state.group, do: {state.key, state.slices, slice}
    # Once resolved, later slices read the same snapshot without resolving it again.
    snapshot_id = if is_integer(state.snapshot_id), do: state.snapshot_id, else: state.scan_opts[:snapshot_id]

    opts = Keyword.merge(state.scan_opts, snapshot_id: snapshot_id, slice: filter)
    read = %{slice: slice, emitted: 0, acked: 0, failed: 0, rows: 0, exhausted?: false}

    request_batches(%{
      state
      | scan: %{pid: Scan.start_link(self(), ref, opts), ref: ref, slice: slice},
        reads: Map.put(state.reads, ref, read),
        requested: 0
    })
  end

  defp request_batches(%{scan: %{pid: pid, ref: ref}, demand: demand, requested: requested} = state)
       when demand > requested do
    Scan.fetch(pid, ref, demand - requested)

    %{state | requested: demand}
  end

  defp request_batches(state), do: state

  defp start_scan(%{started?: true} = state, _snapshot_id), do: state

  defp start_scan(state, snapshot_id) do
    state = %{state | started?: true, snapshot_id: snapshot_id}
    Logger.debug("Scanning #{inspect(state.table)} at snapshot #{inspect(snapshot_id)}")

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :start],
      %{system_time: System.system_time()},
      event_metadata(state)
    )

    state
  end

  # A slice is finished once it has been read to the end and every batch acknowledged.
  defp finish_read(state, ref) do
    case Map.fetch!(state.reads, ref) do
      %{exhausted?: true, emitted: emitted, acked: acked, failed: failed} = read when acked + failed == emitted ->
        finished = %{read.slice => {read.failed, read.emitted, read.rows}}
        if state.group, do: Distribution.announce(state.members, state.group, finished)

        %{state | reads: Map.delete(state.reads, ref), done: Distribution.merge(state.done, finished)}

      _unfinished ->
        state
    end
  end

  # Another member finished the slice this producer is reading, so stop reading it. Its
  # emitted batches are still processed and acknowledged.
  defp drop_finished_slice(%{scan: %{slice: slice}} = state) when is_map_key(state.done, slice) do
    stop_scan(state)
  end

  defp drop_finished_slice(state), do: state

  defp stop_scan(%{scan: nil} = state), do: state

  defp stop_scan(state) do
    Scan.cancel(state.scan.pid)
    %{state | scan: nil, requested: 0}
  end

  defp maybe_complete(%{status: :running, slices: slices, done: done} = state) when map_size(done) == slices do
    {failed, batches, rows} =
      Enum.reduce(Map.values(done), {0, 0, 0}, fn {f, b, r}, {failed, batches, rows} ->
        {failed + f, batches + b, rows + r}
      end)

    Logger.debug("Scan of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)} completed")

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :stop],
      %{duration: duration(state), batches: batches, rows: rows, failed_batches: failed},
      event_metadata(state)
    )

    %{state | status: :completed}
  end

  defp maybe_complete(state), do: state

  defp describe(%{group: nil} = state, _slice) do
    "#{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)}"
  end

  defp describe(state, slice) do
    "slice #{slice} of #{state.slices} of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)}"
  end

  defp event_metadata(state) do
    %{name: state.name, table: state.table, snapshot_id: state.snapshot_id}
  end

  defp duration(state), do: System.monotonic_time() - state.started_at

  defp format_reason(exception) when is_exception(exception), do: Exception.message(exception)
  defp format_reason(reason), do: inspect(reason)
end
