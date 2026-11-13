defmodule OffBroadway.Iceberg.Test.FakeConnection do
  @moduledoc """
  A DBConnection driver that answers QuackDB's `query/4` and `columnar_batches/4` from
  a script instead of a DuckDB server, so the producer runs its real code path
  without one.

  Every call is reported to the test process as `{:fake, event}`:

  - `{:connect}`, `{:disconnect, error}`
  - `{:query, statement, params}` - a `QuackDB.query/4`
  - `{:declare, statement, params, max_rows}` - a stream started
  - `{:fetch, index, row_count}` - a stream batch fetched, `index` starting at 1
  - `{:deallocate}` - a stream closed

  Script keys:

  - `:refs` - rows returned by the `iceberg_refs` query (default `[[42]]`)
  - `:history` - rows returned by the `iceberg_history` query (default `[[42]]`)
  - `:rows` - rows of the scan, as one-element lists (default `[]`). A scan of slice `i`
    of `n` gets the rows whose first value is `i` modulo `n`, standing in for DuckDB's
    `hash/1`
  - `:fail` - `{:bind | :refs | :declare, exception}` or `{{:fetch, n}, exception}`
    makes that call fail; `{{:fetch, n}, {:disconnect, exception}}` also disconnects
  """

  use DBConnection

  def child_spec(opts) do
    %{id: Keyword.get(opts, :id, __MODULE__), start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) do
    DBConnection.start_link(__MODULE__, Keyword.put_new(opts, :pool_size, 1))
  end

  @impl true
  def connect(opts) do
    state = %{test: Keyword.fetch!(opts, :test), script: Map.new(Keyword.get(opts, :script, [])), cursors: %{}}
    report(state, {:connect})
    {:ok, state}
  end

  @impl true
  def disconnect(error, state) do
    report(state, {:disconnect, error})
    :ok
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def ping(state), do: {:ok, state}

  @impl true
  def handle_status(_opts, state), do: {:idle, state}

  @impl true
  def handle_begin(_opts, state), do: {:idle, state}

  @impl true
  def handle_commit(_opts, state), do: {:idle, state}

  @impl true
  def handle_rollback(_opts, state), do: {:idle, state}

  @impl true
  def handle_prepare(query, _opts, state), do: {:ok, query, state}

  @impl true
  def handle_close(_query, _opts, state), do: {:ok, result([], []), state}

  @impl true
  def handle_execute(query, params, _opts, state) do
    report(state, {:query, query.statement, params})

    response =
      cond do
        String.contains?(query.statement, "LIMIT 0") ->
          respond(state, :bind, result(["id"], []))

        String.contains?(query.statement, "iceberg_refs") ->
          respond(state, :refs, result(["snapshot_id"], Map.get(state.script, :refs, [[42]])))

        String.contains?(query.statement, "iceberg_history") ->
          respond(state, :history, result(["snapshot_id"], Map.get(state.script, :history, [[42]])))

        true ->
          {:ok, result([], []), state}
      end

    case response do
      {:ok, result, state} -> {:ok, query, result, state}
      error -> error
    end
  end

  @impl true
  def handle_declare(query, params, opts, state) do
    report(state, {:declare, query.statement, params, Keyword.get(opts, :max_rows)})
    cursor = make_ref()

    case respond(state, :declare, :ok) do
      {:ok, :ok, state} ->
        rows = state.script |> Map.get(:rows, []) |> slice(params)
        {:ok, query, cursor, put_in(state.cursors[cursor], %{rows: rows, fetched: 0})}

      error ->
        error
    end
  end

  @impl true
  def handle_fetch(_query, cursor, opts, state) do
    %{rows: rows, fetched: fetched} = Map.fetch!(state.cursors, cursor)
    index = fetched + 1
    {batch, rest} = Enum.split(rows, Keyword.get(opts, :max_rows, 500))
    report(state, {:fetch, index, length(batch)})
    state = put_in(state.cursors[cursor], %{rows: rest, fetched: index})

    case respond(state, {:fetch, index}, result(["id"], batch)) do
      {:ok, result, state} when rest == [] -> {:halt, result, state}
      {:ok, result, state} -> {:cont, result, state}
      error -> error
    end
  end

  @impl true
  def handle_deallocate(_query, cursor, _opts, state) do
    report(state, {:deallocate})
    {:ok, result([], []), %{state | cursors: Map.delete(state.cursors, cursor)}}
  end

  defp slice(rows, [_snapshot_id, count, index]), do: Enum.filter(rows, fn [id | _] -> rem(id, count) == index end)
  defp slice(rows, [_snapshot_id]), do: rows

  defp respond(state, call, ok) do
    case Map.get(state.script, :fail) do
      {^call, {:disconnect, exception}} -> {:disconnect, exception, state}
      {^call, exception} -> {:error, exception, state}
      _other -> {:ok, ok, state}
    end
  end

  defp result(columns, rows) do
    %QuackDB.Result{command: :select, columns: columns, rows: rows, num_rows: length(rows)}
  end

  defp report(state, event), do: send(state.test, {:fake, event})
end
