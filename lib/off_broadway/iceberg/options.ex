defmodule OffBroadway.Iceberg.Options do
  @moduledoc false

  alias OffBroadway.Iceberg.Checkpoint
  alias OffBroadway.Iceberg.Scan

  @max_snapshot_id 0x7FFFFFFFFFFFFFFF
  @slice_rows 1_000_000
  @max_job_id_bytes 128

  @opts_schema [
    connection: [
      type: {:custom, __MODULE__, :validate_connection, []},
      doc: """
      An existing QuackDB connection pool, as a pid or registered name. Use this to
      manage DuckDB yourself. Supply exactly one of `:connection` or `:managed`.
      """
    ],
    managed: [
      type: {:custom, __MODULE__, :validate_managed, []},
      doc: """
      Start a managed DuckDB server and QuackDB pool as children of the Broadway
      topology, once per pipeline (not once per producer). Accepts
      `[server: [...], client: [...]]`, where server options are passed to
      `QuackDB.Server` and client options to the QuackDB pool. Names default to
      `<Broadway name>.DuckDB` and `<Broadway name>.QuackDB`. Set a distinct
      `:endpoint` in server options for each managed DuckDB on the same host.
      `:init_sql` in server options assumes the proposed upstream QuackDB
      extension; current QuackDB 0.7 does not support it yet.
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
      required: true,
      doc: """
      The fixed snapshot to read: an integer id, or `{:as_of, datetime}` for the snapshot
      that was current at that time. Use the same value on every node. `:current` is not
      accepted, since producers starting at different times could resolve different snapshots.
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
    columns: [
      type: {:custom, __MODULE__, :validate_columns, []},
      doc: """
      The columns to read, as a list of names such as `["id", "amount"]`, in the order the
      messages hold them. Every name is quoted before it reaches SQL and names a top-level
      column; a struct column is read whole. Reads every column by default. See
      "Selecting columns and rows" below.
      """
    ],
    where: [
      type: {:custom, __MODULE__, :validate_where, []},
      doc: """
      A SQL condition the rows to read must meet, such as `"amount > 100"`, or
      `{condition, params}` with a `?` in the condition for each of `params`, such as
      `{"ts >= ? AND ts < ?", [from, to]}`. Reads every row by default. It must select the
      same rows on every node and every run: pass fixed values as params rather than
      calling `now()`. See "Selecting columns and rows" below.
      """
    ],
    slice_rows: [
      type: :pos_integer,
      default: @slice_rows,
      doc: """
      The target number of rows per slice. Consecutive data files are grouped until their
      record counts reach this target; the final slice may be smaller. Files are never
      split, so a slice can exceed the target. The count is before `:where` and deletes
      apply; this is separate from `:fetch_size` and does not bound memory.
      """
    ],
    checkpoint: [
      type: :keyword_list,
      keys: [
        job_id: [
          type: {:custom, __MODULE__, :validate_job_id, []},
          required: true,
          doc: """
          Names the job, the same on every node and every run, such as
          `"events-backfill-2026-11"`. Runs with the same job id, recorded in the same
          place, resume each other's progress. A string of 1 to #{@max_job_id_bytes} bytes, other than `"."` and `".."`.
          """
        ],
        backend: [
          type: {:custom, __MODULE__, :validate_backend, []},
          required: true,
          doc: """
          `{module, options}`, a module that implements `OffBroadway.Iceberg.Checkpoint`,
          such as `OffBroadway.Iceberg.Checkpoint.GCS`, and its options.
          """
        ]
      ],
      doc: """
      Records every slice finished without failed batches, so that the scan, started
      again with the same `:job_id`, does not read it again. See "Checkpoints" below.
      """
    ]
  ]

  def docs, do: NimbleOptions.docs(@opts_schema)

  @spec validate!(keyword()) :: keyword()
  def validate!(opts) do
    # Broadway injects :broadway (pipeline name and stage index) into the producer's options.
    {_broadway, opts} = Keyword.pop(opts, :broadway)
    validated = NimbleOptions.validate!(opts, @opts_schema)

    case {Keyword.has_key?(validated, :connection), Keyword.has_key?(validated, :managed)} do
      {true, false} -> validated
      {false, true} -> validated
      _ -> raise ArgumentError, "expected exactly one of :connection or :managed"
    end
  end

  @doc false
  def validate_managed(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- [:server, :client] == [] and
         Keyword.has_key?(opts, :server) and
         is_list(opts[:server]) and Keyword.keyword?(opts[:server]) and
         is_list(Keyword.get(opts, :client, [])) and Keyword.keyword?(Keyword.get(opts, :client, [])) do
      {:ok, opts}
    else
      {:error, "expected [server: keyword(), client: keyword()] (client is optional)"}
    end
  end

  def validate_managed(_opts),
    do: {:error, "expected [server: keyword(), client: keyword()] (client is optional)"}

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
  def validate_snapshot_id(snapshot_id) when is_integer(snapshot_id) and snapshot_id in 0..@max_snapshot_id,
    do: {:ok, snapshot_id}

  def validate_snapshot_id({:as_of, %DateTime{}} = as_of), do: {:ok, as_of}

  def validate_snapshot_id(snapshot_id) do
    {:error,
     "expected {:as_of, datetime} or a snapshot id between 0 and #{@max_snapshot_id}, " <>
       "got: #{inspect(snapshot_id)}"}
  end

  @doc false
  def validate_columns(columns) do
    cond do
      not (is_list(columns) and columns != [] and Enum.all?(columns, &Scan.name_part?/1)) ->
        {:error, "expected a non-empty list of column names, each a non-empty string, got: #{inspect(columns)}"}

      Enum.uniq_by(columns, &String.downcase/1) != columns ->
        {:error, "expected every column once, as DuckDB matches names regardless of case, got: #{inspect(columns)}"}

      true ->
        {:ok, columns}
    end
  end

  # QuackDB writes the params into the condition as SQL literals, as it does for any
  # query, so the scan runs a condition without placeholders, and the plan's fingerprint
  # covers exactly that condition. QuackDB only counts placeholders when there are params,
  # so one more, after the condition, makes it count them always. It also tells a string
  # or comment left open, which would hide that placeholder.
  @doc false
  def validate_where(condition) when is_binary(condition), do: format_where(condition, [], condition)

  def validate_where({condition, params} = where) when is_binary(condition) and is_list(params),
    do: format_where(condition, params, where)

  def validate_where(where) do
    {:error, "expected a SQL condition, or {condition, params} with a list of params, got: #{inspect(where)}"}
  end

  defp format_where(condition, params, where) do
    with true <- String.valid?(condition) and String.trim(condition) != "",
         {:ok, formatted} <- QuackDB.SQL.format([condition, "\n?"], params ++ [nil]) do
      {:ok, String.replace_suffix(formatted, "\nNULL", "")}
    else
      false ->
        {:error, "expected a SQL condition, not an empty one, got: #{inspect(where)}"}

      {:error, %QuackDB.Error{code: :parameter_count_mismatch}} ->
        {:error,
         "expected a ? in the condition for each of its #{length(params)} params, outside strings and " <>
           "comments, and every string and comment closed, got: #{inspect(where)}"}

      {:error, error} ->
        {:error, "#{Exception.message(error)}, in: #{inspect(where)}"}
    end
  rescue
    exception -> {:error, "#{Exception.message(exception)}, in: #{inspect(where)}"}
  end

  @doc false
  def validate_job_id(job_id)
      when is_binary(job_id) and byte_size(job_id) in 1..@max_job_id_bytes and job_id not in [".", ".."] do
    if String.valid?(job_id) and not String.match?(job_id, ~r/[\x00-\x1F\x7F]/),
      do: {:ok, job_id},
      else: validate_job_id(nil)
  end

  def validate_job_id(job_id) do
    {:error,
     "expected a job id of 1 to #{@max_job_id_bytes} bytes of UTF-8 without control characters, " <>
       "other than \".\" and \"..\", got: #{inspect(job_id)}"}
  end

  @doc false
  def validate_backend({module, opts}) when is_atom(module) and is_list(opts) do
    if Code.ensure_loaded?(module) and function_exported?(module, :init, 1) and
         function_exported?(module, :namespace, 1) and function_exported?(module, :load, 2) and
         function_exported?(module, :complete, 4) do
      case module.init(opts) do
        {:ok, config} -> {:ok, {module, config}}
        {:error, message} -> {:error, "invalid options for #{inspect(module)}: #{message}"}
      end
    else
      {:error, "expected a module that implements #{inspect(Checkpoint)}, got: #{inspect(module)}"}
    end
  end

  def validate_backend(_backend) do
    {:error, "expected {module, options}, with a module that implements #{inspect(Checkpoint)}"}
  end
end
