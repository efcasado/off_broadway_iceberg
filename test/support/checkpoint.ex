defmodule OffBroadway.Iceberg.Test.Checkpoint do
  @moduledoc """
  A checkpoint backend for tests that keeps its records in a process the test controls.

  Start a store with `start_link/1` and pass it to producers as
  `backend: {OffBroadway.Iceberg.Test.Checkpoint, store: store}`. The store keeps records
  as a real backend does: a job id is bound to the first job it is loaded with, every
  field of it, and a slice is recorded once. The store is the backend's namespace. It
  reports every call to the test as `{:checkpoint, event}`:

  - `{:load, job}`
  - `{:complete, job_id, slice, result}` - when the write starts

  `hold/1` makes writes wait until `release/1`, and `fail/3` makes calls fail until it is
  called again with `nil`. The `:fail_complete` option makes the writes of one producer
  fail with that reason, without the store seeing them.
  """

  @behaviour OffBroadway.Iceberg.Checkpoint

  alias OffBroadway.Iceberg.Checkpoint.Error
  alias OffBroadway.Iceberg.Test.Checkpoint.Store

  @impl true
  def init(opts) do
    case Keyword.fetch(opts, :store) do
      {:ok, store} -> {:ok, %{store: store, fail_complete: Keyword.get(opts, :fail_complete)}}
      :error -> {:error, "required :store option not found"}
    end
  end

  @impl true
  def namespace(%{store: store}), do: store

  @impl true
  def load(%{store: store}, job), do: GenServer.call(store, {:load, job}, :infinity)

  @impl true
  def complete(%{fail_complete: nil, store: store}, job, slice, result) do
    GenServer.call(store, {:complete, job, slice, result}, :infinity)
  end

  def complete(%{fail_complete: reason}, _job, _slice, _result), do: {:error, reason}

  @doc "Starts a store that reports to the caller, or to `:test`."
  def start_link(opts \\ []), do: GenServer.start_link(Store, Keyword.put_new(opts, :test, self()))

  @doc "Records `slices`, a map of slice to result, for `job_id`, as an earlier run would."
  def put_slices(store, job_id, slices), do: GenServer.call(store, {:put_slices, job_id, slices})

  @doc "Records `job`, as an earlier run would."
  def put_job(store, job), do: GenServer.call(store, {:put_job, job})

  @doc "The recorded slices of `job_id`."
  def slices(store, job_id), do: GenServer.call(store, {:slices, job_id})

  @doc "Makes writes wait until `release/1`."
  def hold(store), do: GenServer.call(store, :hold)

  @doc "Stores the writes that wait, answers them, and lets later writes through."
  def release(store), do: GenServer.call(store, :release)

  @doc "Makes `:load` or `:complete` calls return `{:error, reason}`, or succeed again with `nil`."
  def fail(store, call, reason), do: GenServer.call(store, {:fail, call, reason})

  defmodule Store do
    @moduledoc false

    use GenServer

    @impl true
    def init(opts) do
      {:ok, %{test: Keyword.fetch!(opts, :test), jobs: %{}, slices: %{}, held: nil, fail: %{}}}
    end

    @impl true
    def handle_call({:load, job}, _from, state) do
      send(state.test, {:checkpoint, {:load, job}})

      cond do
        reason = state.fail[:load] ->
          {:reply, {:error, reason}, state}

        recorded = state.jobs[job.id] ->
          if recorded == job,
            do: {:reply, {:ok, Map.get(state.slices, job.id, %{})}, state},
            else: {:reply, {:error, %Error{reason: :incompatible, message: "another job"}}, state}

        true ->
          state = put_in(state.jobs[job.id], job)
          {:reply, {:ok, Map.get(state.slices, job.id, %{})}, state}
      end
    end

    def handle_call({:complete, job, slice, result} = write, from, state) do
      send(state.test, {:checkpoint, {:complete, job.id, slice, result}})

      cond do
        reason = state.fail[:complete] -> {:reply, {:error, reason}, state}
        state.held -> {:noreply, %{state | held: [{from, write} | state.held]}}
        true -> store(state, write)
      end
    end

    def handle_call({:put_slices, job_id, slices}, _from, state) do
      {:reply, :ok, update_in(state.slices, &Map.update(&1, job_id, slices, fn old -> Map.merge(old, slices) end))}
    end

    def handle_call({:put_job, job}, _from, state), do: {:reply, :ok, put_in(state.jobs[job.id], job)}

    def handle_call({:slices, job_id}, _from, state), do: {:reply, Map.get(state.slices, job_id, %{}), state}
    def handle_call(:hold, _from, state), do: {:reply, :ok, %{state | held: state.held || []}}

    def handle_call(:release, _from, state) do
      state =
        state.held
        |> List.wrap()
        |> Enum.reverse()
        |> Enum.reduce(%{state | held: nil}, fn {from, write}, state ->
          {:reply, reply, state} = store(state, write)
          GenServer.reply(from, reply)
          state
        end)

      {:reply, :ok, state}
    end

    def handle_call({:fail, call, reason}, _from, state), do: {:reply, :ok, put_in(state.fail[call], reason)}

    defp store(state, {:complete, job, slice, result}) do
      case get_in(state.slices, [Access.key(job.id, %{}), slice]) do
        nil ->
          {:reply, :ok,
           update_in(state.slices, &Map.update(&1, job.id, %{slice => result}, fn s -> Map.put(s, slice, result) end))}

        %{rows: rows} when rows == result.rows ->
          {:reply, :ok, state}

        _other ->
          {:reply, {:error, %Error{reason: :incompatible, message: "another slice"}}, state}
      end
    end
  end
end
