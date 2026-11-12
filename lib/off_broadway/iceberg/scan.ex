defmodule OffBroadway.Iceberg.Scan do
  @moduledoc false

  # The process that owns a producer's DuckDB cursor.
  #
  # A QuackDB stream can only be read inside `DBConnection.run/3`, by the process that
  # checked the connection out, so the cursor cannot live in the producer without
  # blocking it. This process checks out one connection for the whole scan and reads the
  # stream, fetching a batch only once the producer has asked for one.
  #
  # Messages to the producer, in order: `{:snapshot, id | nil}`, any number of
  # `{:batch, %QuackDB.Columns{}}`, then `:done` or `{:error, reason}`. Each is sent as
  # `{OffBroadway.Iceberg.Scan, ref, event}`.

  alias QuackDB.Columns

  @plain_identifier ~r/\A[A-Za-z_][A-Za-z0-9_]*\z/

  @doc """
  Starts the scan for `producer`, linked to the caller.
  """
  @spec start_link(pid(), reference(), keyword()) :: pid()
  def start_link(producer, ref, opts) do
    {:ok, scan} = Task.start_link(fn -> run(producer, ref, opts) end)
    scan
  end

  @doc """
  Allows the scan to fetch `count` more batches.
  """
  @spec fetch(pid(), reference(), pos_integer()) :: :ok
  def fetch(scan, ref, count) do
    send(scan, {__MODULE__, ref, {:fetch, count}})
    :ok
  end

  @doc """
  Stops the scan. The scan dies while holding its connection, so DBConnection
  disconnects that connection, which cancels the query on the DuckDB server.
  """
  @spec cancel(pid()) :: :ok
  def cancel(scan) do
    Process.unlink(scan)
    Process.exit(scan, :shutdown)
    :ok
  end

  @doc """
  Splits a `:table` option into its name parts, or returns `:error`.
  """
  @spec table_parts(term()) :: [String.t()] | :error
  def table_parts(table) when is_binary(table) do
    parts = String.split(table, ".")

    if length(parts) <= 3 and Enum.all?(parts, &Regex.match?(@plain_identifier, &1)),
      do: parts,
      else: :error
  end

  def table_parts(table) when is_list(table) and length(table) in 1..3 do
    if Enum.all?(table, &name_part?/1), do: table, else: :error
  end

  def table_parts(_table), do: :error

  defp name_part?(part) do
    is_binary(part) and part != "" and String.valid?(part) and not String.contains?(part, <<0>>)
  end

  defp run(producer, ref, opts) do
    result =
      try do
        DBConnection.run(
          Keyword.fetch!(opts, :connection),
          &scan(&1, producer, ref, opts),
          timeout: :infinity
        )
      rescue
        exception -> {:error, exception}
      catch
        :exit, reason -> {:error, reason}
      end

    send(producer, {__MODULE__, ref, result})
  end

  defp scan(conn, producer, ref, opts) do
    table =
      opts
      |> Keyword.fetch!(:table)
      |> table_parts()
      |> Enum.map_intersperse(".", &QuackDB.Type.quote_identifier/1)
      |> IO.iodata_to_binary()

    snapshot_id = resolve_snapshot(conn, table, Keyword.fetch!(opts, :snapshot_id))
    send(producer, {__MODULE__, ref, {:snapshot, snapshot_id}})

    if snapshot_id do
      # Running the query already fetches the first batch, so it waits for demand too.
      credit = await_credit(ref, 0)
      statement = ["SELECT * FROM ", table, " AT (VERSION => ?)"]

      _unused_credit =
        conn
        |> QuackDB.columnar_batches(statement, [snapshot_id], max_rows: Keyword.fetch!(opts, :fetch_size))
        |> Enum.reduce(credit, &deliver(&1, &2, producer, ref))
    end

    :done
  end

  defp resolve_snapshot(_conn, _table, snapshot_id) when is_integer(snapshot_id), do: snapshot_id

  defp resolve_snapshot(conn, table, :current) do
    # iceberg_refs/1 reads a name it cannot resolve as a file path, and the error it then
    # raises hides the actual problem. Binding the table first reports a missing table.
    QuackDB.query!(conn, ["SELECT * FROM ", table, " LIMIT 0"])

    # The main branch is the table's current snapshot. A table with no snapshots has none.
    case QuackDB.query!(conn, "SELECT snapshot_id FROM iceberg_refs(?) WHERE name = 'main'", [table]) do
      %{rows: [[snapshot_id]]} -> snapshot_id
      %{rows: []} -> nil
    end
  end

  # The stream fetches its next batch as soon as this returns, so it only returns with
  # credit left.
  defp deliver(%Columns{num_rows: 0}, credit, _producer, _ref), do: credit

  defp deliver(%Columns{} = batch, credit, producer, ref) do
    send(producer, {__MODULE__, ref, {:batch, batch}})
    await_credit(ref, credit - 1)
  end

  defp await_credit(ref, 0) do
    receive do
      {__MODULE__, ^ref, {:fetch, count}} -> count
    end
  end

  defp await_credit(_ref, credit), do: credit
end
