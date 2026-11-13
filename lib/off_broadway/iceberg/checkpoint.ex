defmodule OffBroadway.Iceberg.Checkpoint do
  @moduledoc """
  A place to record the finished slices of a distributed scan, so that a scan started
  again, after its producers restart or the whole cluster does, does not read them again.

  `OffBroadway.Iceberg.Checkpoint.GCS` records them in Google Cloud Storage. See
  "Checkpoints" in `OffBroadway.Iceberg.Producer` for how the producer uses a backend.

  ## Implementing a backend

  A backend is a module that implements this behaviour. The producer calls `c:init/1`
  and `c:namespace/1` when it starts, and `c:load/2` and `c:complete/4` from worker processes,
  so it remains responsive while waiting for storage. Several producers, on several
  nodes, call them for the same job at the same time.

  Producers only share a scan's progress while it runs if they record it in the same
  place, as `c:namespace/1` tells.

  A backend keeps two kinds of records per job, both written once and never changed:

  - The job: its id, table, snapshot and plan. `c:load/2` creates it, or checks that the
    one already recorded is the same job.
  - A finished slice: its index and counts. `c:complete/4` creates it. A slice already
    recorded with the same rows is the same slice, finished again, which is not an error.

  A backend returns `:ok` or `{:ok, ...}` only once the record is stored, and never
  reports a failure to reach its storage as an empty job. When a write fails in a way that
  may still have stored it, such as a timeout, it must be safe to call `c:complete/4`
  again, and `c:load/2` must see the record if it was stored.
  """

  @typedoc """
  A checkpointed scan:

  - `:id` - The `:job_id` the producer was given.
  - `:table` - The table, as its name parts.
  - `:snapshot_id` - The snapshot the scan reads.
  - `:plan` - The SHA-256 of the scan's plan, which changes with its slices and their
    data files, and with the columns and rows the scan selects.
  - `:slices` - The number of slices in the plan.

  Every field is immutable: a run of a job loads it only if every field is the same as
  the first run recorded.
  """
  @type job :: %{
          id: String.t(),
          table: [String.t()],
          snapshot_id: non_neg_integer(),
          plan: <<_::256>>,
          slices: non_neg_integer()
        }

  @typedoc "The index of a slice in the plan, starting at 0."
  @type slice :: non_neg_integer()

  @typedoc """
  The counts of a finished slice. `:rows` is the same every time the slice is read.
  `:batches` depends on `:fetch_size` and on DuckDB, so it can differ between reads.
  """
  @type result :: %{batches: non_neg_integer(), rows: non_neg_integer()}

  @doc """
  Validates the backend's options and returns the configuration the other callbacks get.
  It runs whenever the producer validates its options, so it must not do any I/O.
  """
  @callback init(opts :: keyword()) :: {:ok, config :: term()} | {:error, String.t()}

  @doc """
  Identifies where the backend records jobs, such as a bucket and a prefix. Producers
  with the same job id share a scan only if their namespaces are equal, so that one never
  skips a slice that only another storage recorded.

  It must be equal on every node exactly when the backends read and write the same
  records. When unsure, tell storages apart: producers that do not share a scan only read
  slices twice. Build it from the options that name the storage, not from all of them.
  """
  @callback namespace(config :: term()) :: term()

  @doc """
  Records the job, unless it is recorded already, and returns its finished slices.

  Returns an error, and no slices, if a job is recorded under the same id with any field
  that differs, or if the backend cannot tell which slices are finished.
  """
  @callback load(config :: term(), job()) :: {:ok, %{slice() => result()}} | {:error, term()}

  @doc """
  Records that `slice` of `job` is finished, every one of its batches processed
  successfully.

  Returns `:ok` once the record is stored, or if the slice was already recorded with the
  same `:rows`. Returns an error if it was recorded with other rows, or if the backend
  cannot confirm that the record is stored.
  """
  @callback complete(config :: term(), job(), slice(), result()) :: :ok | {:error, term()}
end
