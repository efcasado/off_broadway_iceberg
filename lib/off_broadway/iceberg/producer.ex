defmodule OffBroadway.Iceberg.Producer do
  @moduledoc """
  A Broadway producer that reads one Apache Iceberg snapshot through DuckDB.

  The producer runs `SELECT * FROM table AT (VERSION => snapshot_id)` on a
  [QuackDB](https://hexdocs.pm/quackdb) connection, or reads only the columns and rows
  that `:columns` and `:where` select, and emits the result as `Broadway.Message`s, one
  message per fetched batch of rows. The snapshot is fixed for the whole scan, so commits
  made to the table while the scan runs are not read.

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
  alias OffBroadway.Iceberg.Checkpoint
  alias OffBroadway.Iceberg.Distribution
  alias OffBroadway.Iceberg.Options
  alias OffBroadway.Iceberg.Scan

  require Logger

  @doc """
  Starts an `OffBroadway.Iceberg` producer process linked to the current process.

  ## Configuration

  #{Options.docs()}

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

  ## Selecting columns and rows

  `:columns` and `:where` narrow what the scan reads:

      columns: ["event_id", "occurred_at", "amount"],
      where: {"occurred_at >= ? AND occurred_at < ?", [~U[2026-10-01 00:00:00Z], ~U[2026-11-01 00:00:00Z]]}

  The scan then runs:

      SELECT "event_id", "occurred_at", "amount"
      FROM (SELECT * FROM table AT (VERSION => snapshot_id))
      WHERE (
      occurred_at >= TIMESTAMPTZ '2026-10-01T00:00:00Z' AND occurred_at < TIMESTAMPTZ '2026-11-01T00:00:00Z'
      )

  DuckDB pushes both into its Iceberg scan: it reads only the columns named, and can skip
  the parts of data files whose statistics rule the condition out. The table's delete
  files still apply. Each message holds the columns in the order `:columns` names them.

  QuackDB writes the params into the condition as SQL literals, quoting strings, as it
  does for any query. It takes the same values as `QuackDB.query/3`, and an option error
  names one it cannot write. The condition itself is SQL, run as written: build it in your
  code, never from user input, and pass values as params.

  > #### The condition must not change {: .warning}
  >
  > `:where` must select the same rows on every node, and on every run of a checkpointed
  > job, for as long as the scan or job lasts. A distributed scan runs it once per slice,
  > on several nodes, and the producer cannot check what it selects: the plan's
  > fingerprint covers the condition's text, not its rows. With `"occurred_at < now()"`,
  > members would read their slices up to different times, and a checkpointed job would
  > resume with its recorded slices read up to an earlier time than the rest, so the rows
  > in between are never read.
  >
  > Pass fixed values as params instead, such as `{"occurred_at < ?", [cutoff]}`, with a
  > `cutoff` that is the same on every node and every run, for example from your
  > configuration, next to the job id. A value computed as the pipeline starts, such as
  > `DateTime.utc_now()`, differs between nodes and runs: those producers then scan
  > apart, and a checkpointed job fails as incompatible when it runs again. Nothing that
  > changes on its own, such as `current_date` or `random()`, belongs in the condition.

  In a distributed scan, the condition only filters the rows of each slice. Slices are
  still made of whole data files, sized by the rows of their files before the condition
  applies, so with a selective condition a slice can hold far fewer rows than
  `:slice_rows`, or none. The plan binds the columns and the condition once, without
  reading any rows, so that a column that does not exist, or a condition DuckDB cannot
  run, fails the scan before any slice is read.

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

  Without `:checkpoint`, progress is not persisted. A producer that crashes is restarted
  by Broadway and scans the snapshot again from the start, and so does a pipeline started
  again after a failure. Make downstream writes idempotent, keyed for example by the
  snapshot id and the row's own key, so a replay is harmless. This producer does not
  provide exactly-once processing. See "Checkpoints" below for scans that resume.

  When the pipeline shuts down mid-scan, the producer stops the worker. DBConnection
  then disconnects the connection it held, which cancels the query on the DuckDB
  server, and the pool reconnects it.

  ## Distributed scans

  > #### Experimental {: .warning}
  >
  > Distributed scans are new. Their behavior may change.

  With `:distribute`, the producers of this pipeline on every connected node share one
  scan. Every producer stage is a member, so `producer: [concurrency: 2]` on three nodes
  makes six.

  ### Slices

  Each member plans the scan the same way. It resolves the snapshot and lists its data
  files from the table's manifests, through `iceberg_files`, in path order. Consecutive
  files make a slice once they hold `:slice_rows` rows between them. Files are never
  split, even when one exceeds that target. A slice is read with
  `SELECT * FROM table AT (VERSION => snapshot_id) WHERE filename IN (...)`, so DuckDB
  opens only those files, and still applies the table's delete files. `:columns` and
  `:where` then select from the rows of the slice, as above.

  Every data file is read by one slice, so the snapshot is read once in all. A scan
  cannot be shared by more members than it has slices, though: a table stored in a
  single large file is read by a single member.

  ### Members

  Once planned, a member joins a process group (OTP's `:pg`) named after the pipeline,
  the table, the snapshot and the plan. Only members that planned the same slices, and
  select the same columns and rows from them, share a scan. A slice belongs to the
  member that wins rendezvous hashing over the group's current members. When nodes join
  or leave, for example as an autoscaler adds or removes pods, only the slices of the
  members that came or went move.

  The nodes must be connected to each other, as Phoenix's `DNSCluster` and `libcluster`
  connect them. A member only sees the members on nodes it is connected to, and one that
  sees no others reads every slice itself.

  A member that finishes a slice tells the others, and every member tells those that
  join which slices are finished. Without `:checkpoint`, nothing is stored anywhere else:
  if every member stops at once, for example in a full redeploy, the members that start
  next scan again. With an integer or `{:as_of, datetime}` `:snapshot_id`, they read the
  same rows again, so idempotent writes make that harmless, if wasteful. With
  `:checkpoint`, they only read the slices that no earlier run finished. See
  "Checkpoints" below.

  ### What `:stop` means

  A slice is finished once a member has read it to the end and every one of its
  batches has been acknowledged. Members only ever tell each other about finished
  slices. That news can arrive late, or not at all, but it never names a slice that was
  not processed.

  A member emits `:stop` once it knows that every slice is finished and every batch it
  emitted itself has been acknowledged. So `:stop` means that every row of the snapshot
  was processed at least once, by this member or by one it heard from. A member that
  joins after the scan finished emits `:stop` without reading anything.

  The `:batches`, `:rows` and `:failed_batches` of `:stop` count each slice once, from
  the copy of it with the fewest failed batches. So `failed_batches: 0` means that every
  slice was processed without failures by some member: every row landed. Failures in
  other copies of a slice do not count there. `:local_failed_batches` counts every batch
  this member emitted that failed, in any copy, so a failure anywhere shows up in the
  `:stop` of the member that saw it, as well as in its logs.

  `:stop` does not mean that each row was processed exactly once, or that other members
  are done. A slice can be processed more than once:

  - A member keeps reading a slice it stopped owning, until another member finishes it.
  - A member that fails or stops leaves the group, and the slices it had not finished
    are read again, from the start, by the members that own them now.
  - Members that cannot reach each other, for example on either side of a network
    partition, each scan on their own. Each side can emit `:stop`, and what `:stop`
    means still holds on each side, but both sides may process the same slices.

  Writes must be idempotent, as they must for any scan of this producer.

  ### Requirements

  - All members must use the same pipeline name, `:table`, `:snapshot_id`, `:columns`,
    `:where` and `:distribute`. Producers that differ in any of them scan separately. With
    `:checkpoint`, the same `:job_id`, recorded in the same place, takes the place of
    the pipeline name. See "Checkpoints" below.
  - `:snapshot_id` must be an integer or `{:as_of, datetime}`. Members resolving
    `:current` at different times could read different snapshots.

  A member whose slice or plan fails emits `:exception`, leaves the group and stays
  idle. The other members read its slices.

  ## Checkpoints

  > #### Experimental {: .warning}
  >
  > Checkpoints are new. Their behavior may change.

  With `:checkpoint`, a distributed scan records every slice it finishes in a backend,
  such as Google Cloud Storage with `OffBroadway.Iceberg.Checkpoint.GCS`. A scan started
  again, after a producer crashed, a node was replaced or every node restarted at once,
  does not read the recorded slices again:

      {OffBroadway.Iceberg.Producer,
       connection: MyApp.QuackDB,
       table: "lake.betting.events",
       snapshot_id: 123456789,
       distribute: true,
       checkpoint: [
         job_id: "events-backfill-2026-11",
         backend:
           {OffBroadway.Iceberg.Checkpoint.GCS,
            bucket: "my-checkpoints", prefix: "iceberg", auth: {:goth, MyApp.Goth}}
       ]}

  `:checkpoint` requires `:distribute`, since what it records are slices. A single
  producer on a single node is a distributed scan of one member.

  ### Jobs

  A job is one scan of one snapshot, named by `:job_id`. The first run of a job records
  it: its id, its table, its snapshot, the fingerprint of its plan and its number of
  slices. Every later run checks that all of them are the same before it reads a row,
  and fails its scan with an `OffBroadway.Iceberg.Checkpoint.Error` if any is not,
  changing nothing. The plan covers the columns and rows the scan selects, so a different
  `:slice_rows`, `:columns` or `:where` makes a different plan. Keep them for the life of
  a job, and use a new job id to scan again from the start, or to select something else.

  The plan covers the text of `:where`, not the rows it selects, so a job only resumes
  correctly if its condition selects the same rows on every run. See "The condition
  must not change" in "Selecting columns and rows" above.

  The table is recorded as its name parts, so `"lake.betting.events"` and
  `["lake", "betting", "events"]` name the same table, but `"events"` names another one,
  even where DuckDB resolves it to the same table. Name it the same way on every run.

  `{:as_of, datetime}` resolves to the same snapshot on every run while the table's
  snapshot log keeps it. A snapshot that was expired cannot be read again: the job then
  fails, rather than reading another snapshot.

  Producers share a job's scan while it runs if they have the same job id and record it
  in the same place: for `OffBroadway.Iceberg.Checkpoint.GCS`, the same endpoint, bucket
  and prefix. Producers that record the same job id elsewhere scan it apart, since one
  would not find what the other recorded after a restart.

  ### What is recorded

  A slice is recorded once it has been read to the end and every one of its batches was
  acknowledged as successful. Only then do the other members hear that it is finished,
  and only then does it count towards `:stop`. A run of the job loads the recorded slices
  before it reads anything, and reads the others.

  A slice with failed batches is not recorded. The run still counts it finished, as it
  would without checkpoints, so `:stop` reports its failed batches, and no member starts
  reading it again: it is not retried. Members that were reading it already go on,
  though, unlike without checkpoints. Only a copy that succeeded, which is recorded, ends
  the others, and it replaces the failed one in what the members know.

  The next run of the job reads every slice not recorded, those with failed batches
  included. To retry the failed batches of a job, start it again once it has stopped.
  Members that keep running, as in a rolling redeploy, remember the slice as finished,
  so only a run that starts with no member running reads it again.

  A producer emits `:stop` only once its own writes have returned, so a pipeline stopped
  on `:stop` does not cut one short.

  ### Failures

  A producer that cannot load its job, or cannot record a slice it finished, fails its
  scan with `:exception`, as a failed query does, and the other members read its slices.
  A backend that cannot be reached is never taken for a job without finished slices, and
  a slice whose write was not confirmed is never counted as recorded.

  A producer that stops while it records a slice, before its `:stop`, leaves the slice
  to be read again.

  ### Guarantees

  Checkpoints spare a restarted scan the work that was finished. They do not make
  processing exactly-once: a producer can process every batch of a slice and stop before
  recording it, and members that read a slice at the same time each process it (see
  "What `:stop` means"). Writes must be idempotent, as they must for any scan of this
  producer.

  ## Telemetry

  The producer emits the following events:

  - `[:off_broadway_iceberg, :scan, :start]` - The snapshot is resolved and the scan
    begins. Measurements: `:system_time`. Metadata: `:name`, `:table`, `:snapshot_id`.
  - `[:off_broadway_iceberg, :scan, :stop]` - Every batch has been emitted and
    acknowledged. Measurements: `:duration`, `:batches`, `:rows`, `:failed_batches`,
    `:local_failed_batches`. Metadata: `:name`, `:table`, `:snapshot_id`.
  - `[:off_broadway_iceberg, :scan, :exception]` - The scan failed. Measurements:
    `:duration`, `:batches`, `:rows`. Metadata: `:name`, `:table`, `:snapshot_id`
    (`nil` if the scan failed before resolving it), `:kind` and `:reason`.
  - `[:off_broadway_iceberg, :checkpoint, :load]` - A checkpointed scan loaded its job.
    Measurements: `:duration`, `:slices`, the number of slices in the plan, and
    `:finished`, the number of them earlier runs finished. Metadata: `:name`, `:table`,
    `:snapshot_id`, `:job_id`.
  - `[:off_broadway_iceberg, :checkpoint, :write]` - A slice was recorded. Measurements:
    `:duration`, `:batches`, `:rows`. Metadata: `:name`, `:table`, `:snapshot_id`,
    `:job_id`, `:slice`.

  The scan events of a checkpointed scan have its `:job_id` in their metadata too. A
  scan that fails because a checkpoint could not be loaded or written has an
  `OffBroadway.Iceberg.Checkpoint.Error` as its `:reason`.

  `:name` is the Broadway pipeline name, or `nil` outside Broadway. Durations are in
  `:native` time units. In a distributed scan, the counts of `:stop` cover every slice
  once, wherever it was read, and those of `:exception` the batches this producer
  emitted. See "What `:stop` means" above.

  See "Completion, failure and restarts" and "What `:stop` means" above for how to
  interpret these outcomes, including failures in duplicate copies of a slice.

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

    distributed? = producer_opts |> Options.validate!() |> Keyword.get(:distribute) != nil

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
    {distribute, scan_opts} = opts |> Options.validate!() |> Keyword.pop(:distribute)
    {checkpoint, scan_opts} = Keyword.pop(scan_opts, :checkpoint)

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
      # reference their messages are acknowledged with. The scan is not complete while
      # there are any, even when other members finished every slice.
      reads: %{},
      # This producer's own counts, of every slice it read, whichever copy of a slice
      # counts for the scan.
      emitted: 0,
      rows: 0,
      failed: 0,
      # A scan without :distribute is a single slice that only this producer reads. A
      # distributed one has as many slices as its plan, and joins its group once planned.
      distributed?: distribute != nil,
      slices: if(distribute, do: nil, else: 1),
      units: nil,
      plan: nil,
      planner: nil,
      group: nil,
      monitor: nil,
      members: [self()],
      done: %{},
      # With :checkpoint, the job and its backend. The scan reads no slice before the
      # loader has returned the finished ones, and a slice it finishes is only announced
      # once its write, one of :writes by reference, has stored it.
      checkpoint: checkpoint && checkpoint_state(checkpoint),
      loader: nil,
      writes: %{}
    }

    state = if distribute, do: plan(state, distribute), else: state

    {:producer, next_slice(state)}
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
    metadata = if state.distributed?, do: Map.put(metadata, :slice, slice), else: metadata

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
    {:noreply, [], fail(%{state | scan: nil, requested: 0}, describe(state, slice), reason)}
  end

  def handle_info({Scan, ref, {:plan, snapshot_id, units}}, %{planner: %{ref: ref}} = state) do
    plan = Distribution.fingerprint(units, state.scan_opts[:columns], state.scan_opts[:where])
    state = %{state | planner: nil, units: List.to_tuple(units), plan: plan}

    {:noreply, [], state |> start_scan(snapshot_id) |> load_checkpoints()}
  end

  def handle_info({Scan, ref, {:error, reason}}, %{planner: %{ref: ref}} = state) do
    {:noreply, [], fail(%{state | planner: nil}, "the plan of #{inspect(state.table)}", reason)}
  end

  # A worker that was stopped may have sent messages before it stopped.
  def handle_info({Scan, _ref, _event}, state), do: {:noreply, [], state}

  def handle_info({Checkpoint, ref, {result, duration}}, %{loader: %{ref: ref}} = state) do
    state = %{state | loader: nil}

    with {:ok, finished} <- result,
         {:ok, done} <- finished_slices(finished, tuple_size(state.units)) do
      :telemetry.execute(
        [:off_broadway_iceberg, :checkpoint, :load],
        %{duration: duration, slices: tuple_size(state.units), finished: map_size(done)},
        event_metadata(state)
      )

      Logger.debug(
        "Loaded job #{inspect(state.checkpoint.job_id)}: #{map_size(done)} of #{tuple_size(state.units)} " <>
          "slices finished"
      )

      {:noreply, [], start_slices(%{state | done: Distribution.merge(state.done, done)})}
    else
      error -> {:noreply, [], fail(state, describe(state, nil), error_reason(error))}
    end
  end

  def handle_info({Checkpoint, ref, {result, duration}}, state) when is_map_key(state.writes, ref) do
    {write, writes} = Map.pop!(state.writes, ref)

    {:noreply, [], checkpointed(%{state | writes: writes}, write, result, duration)}
  end

  # A loader or write that was stopped may have sent its result before it stopped.
  def handle_info({Checkpoint, _ref, _result}, state), do: {:noreply, [], state}

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
    state = %{state | reads: Map.put(state.reads, ref, read), failed: state.failed + length(failed)}

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
  # Checkpoint writes go on, so that slices whose last batches are acknowledged while
  # draining are still recorded, if the pipeline waits for them.
  def prepare_for_draining(%{status: :running} = state) do
    {:noreply, [], state |> stop_planner() |> stop_loader() |> stop_scan() |> leave() |> Map.put(:status, :cancelled)}
  end

  def prepare_for_draining(state), do: {:noreply, [], state}

  # A supervisor shutdown takes the worker down through the link. A normal stop does not
  # propagate through a link, so stop the worker explicitly. A checkpoint write stopped
  # midway leaves its slice to be read again, as one that was never written does.
  @impl GenStage
  def terminate(_reason, state) do
    state |> stop_planner() |> stop_loader() |> stop_scan() |> stop_writes()
  end

  defp plan(state, distribute) do
    ref = make_ref()
    opts = Keyword.put(state.scan_opts, :slice_rows, Keyword.fetch!(distribute, :slice_rows))
    {:ok, pid} = Scan.plan(self(), ref, opts)

    %{state | planner: %{pid: pid, ref: ref}}
  end

  defp checkpoint_state(checkpoint) do
    {backend, config} = Keyword.fetch!(checkpoint, :backend)

    %{
      job_id: Keyword.fetch!(checkpoint, :job_id),
      backend: {backend, config},
      namespace: backend.namespace(config),
      job: nil
    }
  end

  # A checkpointed scan reads no slice before it knows which ones a run of its job
  # finished. Before the table's first snapshot, there is nothing to read or record.
  defp load_checkpoints(%{checkpoint: %{backend: {backend, config}}, snapshot_id: snapshot_id} = state)
       when snapshot_id != nil do
    job = %{
      id: state.checkpoint.job_id,
      table: Scan.table_parts(state.table),
      snapshot_id: snapshot_id,
      plan: state.plan,
      slices: tuple_size(state.units)
    }

    loader = run_checkpoint(fn -> backend.load(config, job) end)
    %{state | loader: loader, checkpoint: %{state.checkpoint | job: job}}
  end

  defp load_checkpoints(state), do: start_slices(state)

  defp start_slices(state) do
    %{state | slices: tuple_size(state.units)} |> join() |> next_slice() |> maybe_complete()
  end

  # A backend may only return slices of the plan.
  defp finished_slices(finished, slices) when is_map(finished) do
    Enum.reduce_while(finished, {:ok, %{}}, fn
      {slice, %{batches: batches, rows: rows}}, {:ok, done}
      when is_integer(slice) and slice >= 0 and slice < slices and is_integer(batches) and batches >= 0 and
             is_integer(rows) and rows >= 0 ->
        {:cont, {:ok, Map.put(done, slice, {0, batches, rows})}}

      {slice, result}, _done ->
        message =
          "the checkpoint backend loaded #{inspect(result)} for slice #{inspect(slice)}, " <>
            "which is not a finished slice of a plan of #{slices} slices"

        {:halt, {:error, %Checkpoint.Error{reason: :invalid, message: message}}}
    end)
  end

  defp finished_slices(finished, _slices), do: {:error, unexpected_return({:ok, finished})}

  defp error_reason({:error, reason}), do: reason
  defp error_reason(other), do: unexpected_return(other)

  defp unexpected_return(other) do
    %Checkpoint.Error{reason: :invalid, message: "the checkpoint backend returned #{inspect(other, limit: 5)}"}
  end

  # Runs a backend call in a process linked to the producer, which then gets
  # `{Checkpoint, ref, {result, duration}}`. A backend that raises or exits returns an
  # error instead.
  defp run_checkpoint(fun) do
    producer = self()
    ref = make_ref()

    {:ok, pid} =
      Task.start_link(fn ->
        started = System.monotonic_time()

        result =
          try do
            fun.()
          rescue
            exception -> {:error, exception}
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        send(producer, {Checkpoint, ref, {result, System.monotonic_time() - started}})
      end)

    %{pid: pid, ref: ref}
  end

  # Producers only share progress if they planned the same slices, because slices are
  # known to each other by their position in the plan. Checkpointed producers share it
  # with the producers that record the same job in the same place, whatever their
  # pipeline, so that what they hear is what a restart would load.
  defp join(state) do
    owner =
      case state.checkpoint do
        nil -> state.name
        %{backend: {backend, _config}, namespace: namespace, job_id: id} -> {:checkpoint, backend, namespace, id}
      end

    group = {owner, Scan.table_parts(state.table), state.snapshot_id, state.plan}
    {monitor, members} = Distribution.join(group)

    Logger.debug(
      "Joined the scan of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)} with plan " <>
        "#{state.plan |> Base.encode16(case: :lower) |> binary_part(0, 12)}: #{state.slices} slices, " <>
        "#{length(members)} members"
    )

    %{state | group: group, monitor: monitor, members: members}
  end

  defp fail(state, what, reason) do
    Logger.error(
      "Scan of #{what} failed after #{state.emitted} batches and will not be " <>
        "retried#{if state.distributed?, do: " by this producer", else: ""}: #{format_reason(reason)}"
    )

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :exception],
      %{duration: duration(state), batches: state.emitted, rows: state.rows},
      Map.merge(event_metadata(state), %{kind: :error, reason: reason})
    )

    leave(%{state | status: :failed})
  end

  defp leave(%{group: nil} = state), do: state

  defp leave(state) do
    Distribution.leave(state.group, state.monitor)
    %{state | monitor: nil}
  end

  # Starts reading the first slice this producer owns that is neither finished nor read
  # or being checkpointed by it already, if it is not reading one.
  defp next_slice(%{status: :running, scan: nil, slices: slices} = state) when is_integer(slices) do
    busy = MapSet.new(Map.values(state.reads) ++ Map.values(state.writes), & &1.slice)

    slice =
      Enum.find(0..(state.slices - 1)//1, fn slice ->
        not is_map_key(state.done, slice) and not MapSet.member?(busy, slice) and owner?(state, slice)
      end)

    if slice, do: read_slice(state, slice), else: state
  end

  defp next_slice(state), do: state

  defp owner?(%{group: nil}, _slice), do: true
  defp owner?(state, slice), do: Distribution.owner(state.group, slice, state.members) == self()

  defp read_slice(state, slice) do
    ref = make_ref()
    filter = if state.units, do: {:files, elem(state.units, slice)}
    # A distributed scan read its snapshot when planning.
    snapshot_id = if state.units, do: state.snapshot_id, else: state.scan_opts[:snapshot_id]

    opts = Keyword.merge(state.scan_opts, snapshot_id: snapshot_id, slice: filter)
    read = %{slice: slice, emitted: 0, acked: 0, failed: 0, rows: 0, exhausted?: false, dropped?: false}
    {:ok, pid} = Scan.start_link(self(), ref, opts)

    request_batches(%{
      state
      | scan: %{pid: pid, ref: ref, slice: slice},
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

  # A slice is finished once it has been read to the end and every batch acknowledged. A
  # read dropped because another member finished its slice only waits for its batches.
  defp finish_read(state, ref) do
    case Map.fetch!(state.reads, ref) do
      %{dropped?: true, emitted: emitted, acked: acked, failed: failed} when acked + failed == emitted ->
        %{state | reads: Map.delete(state.reads, ref)}

      %{exhausted?: true, emitted: emitted, acked: acked, failed: failed} = read when acked + failed == emitted ->
        finish_read_slice(%{state | reads: Map.delete(state.reads, ref)}, read)

      _unfinished ->
        state
    end
  end

  # With checkpoints, a slice whose batches all succeeded is only announced once it is
  # recorded, unless another member recorded it already. One with failed batches is not
  # recorded, so that the next run of the job reads it again, but this run counts it
  # finished, as it would without checkpoints.
  defp finish_read_slice(%{checkpoint: nil} = state, read) do
    finish_slice(state, read.slice, {read.failed, read.emitted, read.rows})
  end

  defp finish_read_slice(state, %{failed: 0} = read) do
    if succeeded?(state.done[read.slice]),
      do: finish_slice(state, read.slice, {0, read.emitted, read.rows}),
      else: write_checkpoint(state, read)
  end

  defp finish_read_slice(state, read) do
    Logger.warning(
      "Not checkpointing #{describe(state, read.slice)}, since #{read.failed} of its batches failed. " <>
        "The next run of job #{inspect(state.checkpoint.job_id)} reads it again"
    )

    finish_slice(state, read.slice, {read.failed, read.emitted, read.rows})
  end

  defp finish_slice(state, slice, counts) do
    finished = %{slice => counts}
    if state.group, do: Distribution.announce(state.members, state.group, finished)

    %{state | done: Distribution.merge(state.done, finished)}
  end

  defp write_checkpoint(%{checkpoint: %{backend: {backend, config}, job: job}} = state, read) do
    result = %{batches: read.emitted, rows: read.rows}
    write = run_checkpoint(fn -> backend.complete(config, job, read.slice, result) end)

    %{state | writes: Map.put(state.writes, write.ref, Map.merge(write, %{slice: read.slice, result: result}))}
  end

  defp checkpointed(state, %{slice: slice, result: result}, :ok, duration) do
    :telemetry.execute(
      [:off_broadway_iceberg, :checkpoint, :write],
      %{duration: duration, batches: result.batches, rows: result.rows},
      Map.put(event_metadata(state), :slice, slice)
    )

    state |> finish_slice(slice, {0, result.batches, result.rows}) |> next_slice() |> maybe_complete()
  end

  # A slice that cannot be recorded is not finished. The producer fails, and the members
  # that own its slices now, this one included, read them again.
  defp checkpointed(%{status: :running} = state, write, error, _duration) do
    state |> stop_scan() |> fail(describe(state, write.slice), error_reason(error))
  end

  defp checkpointed(state, write, error, _duration) do
    Logger.warning(
      "Could not checkpoint #{describe(state, write.slice)}, which the next run of job " <>
        "#{inspect(state.checkpoint.job_id)} reads again: #{format_reason(error_reason(error))}"
    )

    state
  end

  # Another member finished the slice this producer is reading, so stop reading it. Its
  # emitted batches are still processed and acknowledged. In a checkpointed scan, only a
  # copy that succeeded, which is recorded, ends the others: one with failed batches ends
  # the slice for this run, so nobody starts reading it again, but the copies already
  # being read go on, since one of them may succeed and be recorded.
  defp drop_finished_slice(%{scan: %{slice: slice, ref: ref}} = state) when is_map_key(state.done, slice) do
    if state.checkpoint && not succeeded?(state.done[slice]) do
      state
    else
      state = stop_scan(state)
      finish_read(%{state | reads: Map.update!(state.reads, ref, &%{&1 | dropped?: true})}, ref)
    end
  end

  defp drop_finished_slice(state), do: state

  # A finished slice is a copy that succeeded, or one with failed batches. Merging keeps
  # the one with the fewest failed batches, so a success replaces a failure.
  defp succeeded?({0, _batches, _rows}), do: true
  defp succeeded?(_finished), do: false

  defp stop_planner(%{planner: nil} = state), do: state

  defp stop_planner(state) do
    Scan.cancel(state.planner.pid)
    %{state | planner: nil}
  end

  defp stop_loader(%{loader: nil} = state), do: state

  defp stop_loader(state) do
    stop_task(state.loader.pid)
    %{state | loader: nil}
  end

  defp stop_writes(state) do
    Enum.each(state.writes, fn {_ref, write} -> stop_task(write.pid) end)
    %{state | writes: %{}}
  end

  defp stop_task(pid) do
    Process.unlink(pid)
    Process.exit(pid, :shutdown)
  end

  defp stop_scan(%{scan: nil} = state), do: state

  defp stop_scan(state) do
    Scan.cancel(state.scan.pid)
    %{state | scan: nil, requested: 0}
  end

  # A checkpointed producer also waits for its own writes, so that its :stop counts the
  # slices it recorded, and a pipeline stopped on :stop does not cut a write short.
  defp maybe_complete(%{status: :running, slices: slices, done: done, reads: reads, writes: writes} = state)
       when map_size(done) == slices and map_size(reads) == 0 and map_size(writes) == 0 do
    {failed, batches, rows} =
      Enum.reduce(Map.values(done), {0, 0, 0}, fn {f, b, r}, {failed, batches, rows} ->
        {failed + f, batches + b, rows + r}
      end)

    Logger.debug("Scan of #{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)} completed")

    :telemetry.execute(
      [:off_broadway_iceberg, :scan, :stop],
      %{
        duration: duration(state),
        batches: batches,
        rows: rows,
        failed_batches: failed,
        local_failed_batches: state.failed
      },
      event_metadata(state)
    )

    %{state | status: :completed}
  end

  defp maybe_complete(state), do: state

  defp describe(%{units: units} = state, slice) when units == nil or slice == nil do
    "#{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)}"
  end

  defp describe(state, slice) do
    "slice #{slice} of #{tuple_size(state.units)} (#{length(elem(state.units, slice))} data files) of " <>
      "#{inspect(state.table)} at snapshot #{inspect(state.snapshot_id)}"
  end

  defp event_metadata(%{checkpoint: nil} = state) do
    %{name: state.name, table: state.table, snapshot_id: state.snapshot_id}
  end

  defp event_metadata(state) do
    %{name: state.name, table: state.table, snapshot_id: state.snapshot_id, job_id: state.checkpoint.job_id}
  end

  defp duration(state), do: System.monotonic_time() - state.started_at

  defp format_reason(exception) when is_exception(exception), do: Exception.message(exception)
  defp format_reason(reason), do: inspect(reason)
end
