defmodule OffBroadway.Iceberg.Scan do
  @moduledoc false

  # The process that owns a producer's DuckDB cursor.
  #
  # A QuackDB stream can only be read inside `DBConnection.run/3`, by the process that
  # checked the connection out, so the cursor cannot live in the producer without
  # blocking it. This process checks out one connection for the whole scan and reads the
  # stream, fetching a batch only once the producer has asked for one.
  #
  # A scan reads either the whole snapshot or, with a `:slice` of `{:files, paths}`, the
  # rows of those data files, and selects `:columns` and the rows that meet `:where` from
  # them.
  #
  # Messages to the producer, in order: `{:snapshot, id | nil}`, any number of
  # `{:batch, %QuackDB.Columns{}}`, then `:done` or `{:error, reason}`. Each is sent as
  # `{OffBroadway.Iceberg.Scan, ref, event}`.
  #
  # `plan/3` starts a process of its own, which resolves the snapshot and divides its
  # data files into slices, and sends `{:plan, snapshot_id, slices}` or `{:error, reason}`.

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
  Plans a distributed scan for `producer`, linked to the caller: resolves the snapshot,
  and divides its data files into slices of whole files, in path order, each with at
  least `:slice_rows` rows but the last. Files are never split.
  """
  @spec plan(pid(), reference(), keyword()) :: pid()
  def plan(producer, ref, opts) do
    {:ok, planner} =
      Task.start_link(fn ->
        send(producer, {__MODULE__, ref, with_connection(opts, &plan_slices(&1, opts))})
      end)

    planner
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

  @doc """
  Quotes name parts and joins them with dots.
  """
  @spec quote_name([String.t()]) :: String.t()
  def quote_name(parts) do
    parts |> Enum.map_intersperse(".", &QuackDB.Type.quote_identifier/1) |> IO.iodata_to_binary()
  end

  @doc """
  Whether `part` can be quoted as an identifier: a non-empty UTF-8 string without NUL.
  """
  @spec name_part?(term()) :: boolean()
  def name_part?(part) do
    is_binary(part) and part != "" and String.valid?(part) and not String.contains?(part, <<0>>)
  end

  defp run(producer, ref, opts) do
    send(producer, {__MODULE__, ref, with_connection(opts, &scan(&1, producer, ref, opts))})
  end

  defp with_connection(opts, fun) do
    DBConnection.run(Keyword.fetch!(opts, :connection), fun, timeout: :infinity)
  rescue
    exception -> {:error, exception}
  catch
    :exit, reason -> {:error, reason}
  end

  defp plan_slices(conn, opts) do
    table = opts |> Keyword.fetch!(:table) |> table_parts() |> quote_name()

    case resolve_snapshot(conn, table, Keyword.fetch!(opts, :snapshot_id)) do
      nil ->
        {:plan, nil, []}

      snapshot_id ->
        check_selection(conn, table, snapshot_id, opts)

        # The live data files of the snapshot, from its manifests. Reading them needs only
        # the catalog, unlike the files' own metadata, which may need storage credentials
        # that only the Iceberg scan has.
        %{rows: files} =
          QuackDB.query!(
            conn,
            "SELECT file_path, record_count FROM iceberg_files(?, snapshot_from_id => ?) " <>
              "WHERE content = 0 ORDER BY file_path",
            [table, snapshot_id]
          )

        {:plan, snapshot_id, slices(files, Keyword.fetch!(opts, :slice_rows))}
    end
  end

  # A column or condition DuckDB cannot read fails the plan, before any slice is read or
  # its job recorded. With LIMIT 0, DuckDB binds the query without reading any rows.
  defp check_selection(conn, table, snapshot_id, opts) do
    if Keyword.get(opts, :columns) || Keyword.get(opts, :where) do
      {statement, params} = select(table, opts)
      QuackDB.query!(conn, [statement, " LIMIT 0"], [snapshot_id | params])
    end
  end

  defp slices(files, slice_rows) do
    Enum.chunk_while(
      files,
      {[], 0},
      fn [path, rows], {paths, total} ->
        paths = [path | paths]

        if total + rows >= slice_rows,
          do: {:cont, Enum.reverse(paths), {[], 0}},
          else: {:cont, {paths, total + rows}}
      end,
      fn
        {[], 0} -> {:cont, {[], 0}}
        {paths, _total} -> {:cont, Enum.reverse(paths), {[], 0}}
      end
    )
  end

  defp scan(conn, producer, ref, opts) do
    table = opts |> Keyword.fetch!(:table) |> table_parts() |> quote_name()
    snapshot_id = resolve_snapshot(conn, table, Keyword.fetch!(opts, :snapshot_id))
    send(producer, {__MODULE__, ref, {:snapshot, snapshot_id}})

    if snapshot_id do
      # Running the query already fetches the first batch, so it waits for demand too.
      credit = await_credit(ref, 0)
      {statement, params} = select(table, opts)

      _unused_credit =
        conn
        |> QuackDB.columnar_batches(statement, [snapshot_id | params], max_rows: Keyword.fetch!(opts, :fetch_size))
        |> Enum.reduce(credit, &deliver(&1, &2, producer, ref))
    end

    :done
  end

  # The query of a scan, and its parameters but the snapshot id. Columns and a condition
  # select from the rows of the slice in a query of their own, so that no condition can
  # change which files the slice reads. DuckDB still pushes both into its Iceberg scan.
  # The condition already has its parameters written in.
  defp select(table, opts) do
    {filter, params} = slice_filter(Keyword.get(opts, :slice))
    rows = ["SELECT * FROM ", table, " AT (VERSION => ?)", filter]

    statement =
      case {Keyword.get(opts, :columns), Keyword.get(opts, :where)} do
        {nil, nil} -> rows
        {columns, where} -> ["SELECT ", projection(columns), " FROM (", rows, ")", condition(where)]
      end

    {statement, params}
  end

  defp projection(nil), do: "*"
  defp projection(columns), do: Enum.map_intersperse(columns, ", ", &QuackDB.Type.quote_identifier/1)

  defp condition(nil), do: []
  # On lines of its own, so that a comment at the end of the condition ends with it.
  defp condition(where), do: [" WHERE (\n", where, "\n)"]

  defp slice_filter(nil), do: {[], []}

  # DuckDB reads only the files named, and still applies their delete files.
  defp slice_filter({:files, paths}) do
    {[" WHERE filename IN (", Enum.map_intersperse(paths, ", ", fn _path -> "?" end), ")"], paths}
  end

  defp resolve_snapshot(_conn, _table, snapshot_id) when is_integer(snapshot_id), do: snapshot_id

  # The snapshot log records when each snapshot became the table's current one, so this
  # resolves to the same snapshot wherever it runs. Before the first snapshot, there is
  # none. Iceberg records the times in UTC.
  defp resolve_snapshot(conn, table, {:as_of, %DateTime{} = as_of}) do
    QuackDB.query!(conn, ["SELECT * FROM ", table, " LIMIT 0"])
    as_of = as_of |> DateTime.to_unix(:microsecond) |> DateTime.from_unix!(:microsecond) |> DateTime.to_naive()

    case QuackDB.query!(
           conn,
           "SELECT snapshot_id FROM iceberg_history(?) WHERE made_current_at <= ? ORDER BY made_current_at DESC LIMIT 1",
           [table, as_of]
         ) do
      %{rows: [[snapshot_id]]} -> snapshot_id
      %{rows: []} -> nil
    end
  end

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
