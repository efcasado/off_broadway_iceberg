defmodule OffBroadway.Iceberg.Checkpoint.GCS do
  @opts_schema [
    bucket: [
      type: {:custom, __MODULE__, :validate_bucket, []},
      required: true,
      doc: "The bucket to store checkpoints in. It must exist."
    ],
    prefix: [
      type: {:custom, __MODULE__, :validate_prefix, []},
      default: "",
      doc: """
      The folder of the bucket to store them under, such as `"iceberg"` or
      `"backfills/iceberg"`. Its segments are letters, digits and `._~-`.
      """
    ],
    auth: [
      type: {:custom, __MODULE__, :validate_auth, []},
      required: true,
      doc: """
      `{:goth, name}` to authenticate with the tokens of the Goth server `name`, a function
      that returns `{:ok, token}` or `{:error, reason}`, or `:none` for an emulator.
      """
    ],
    endpoint: [
      type: {:custom, __MODULE__, :validate_endpoint, []},
      default: "https://storage.googleapis.com",
      doc: "The base URL of the JSON API. Change it for an emulator."
    ],
    req_options: [
      type: :keyword_list,
      default: [],
      doc: """
      Options for Req, such as `:receive_timeout`, `:max_retries`, `:retry` or
      `:finch`. They override the adapter's own.
      """
    ]
  ]

  @moduledoc """
  Records checkpoints in a Google Cloud Storage bucket, through its JSON API.

      checkpoint: [
        job_id: "events-backfill-2026-11",
        backend:
          {OffBroadway.Iceberg.Checkpoint.GCS,
           bucket: "my-checkpoints", prefix: "iceberg", auth: {:goth, MyApp.Goth}}
      ]

  It needs [Req](https://hex.pm/packages/req), and [Goth](https://hex.pm/packages/goth)
  for `auth: {:goth, name}`. Both are optional dependencies of this library, so add them
  to your own:

      {:req, "~> 0.7.5"},
      {:goth, "~> 1.4"}

  ## Options

  #{NimbleOptions.docs(@opts_schema)}

  ## Objects

  Every record is an object of its own, created once and never changed:

  ```text
  gs://my-checkpoints/iceberg/jobs/events-backfill-2026-11/job.json
  gs://my-checkpoints/iceberg/jobs/events-backfill-2026-11/slices/000000.json
  gs://my-checkpoints/iceberg/jobs/events-backfill-2026-11/slices/000001.json
  ```

  `job.json` records the job:

  ```json
  {"version": 1, "job_id": "events-backfill-2026-11", "table": ["lake", "betting", "events"],
   "snapshot_id": "123456789", "plan_fingerprint": "9f86d0...", "slices": 42}
  ```

  and each slice that was finished without failed batches has an object of its own:

  ```json
  {"version": 1, "slice": 17, "batches": 24, "rows": 240000}
  ```

  The snapshot id is a string, so that no JSON reader rounds it. The job id is
  percent-encoded in object names, so any job id makes a name of its own, under the
  prefix.

  Each object holds its record twice: as its content, for people to read, and as its
  custom metadata, all strings. The adapter only reads the metadata, which a listing
  returns too, so loading a job takes one listing for every thousand finished slices
  instead of one read per slice.

  ## Writes

  Objects are created with the precondition `ifGenerationMatch=0`, so GCS creates one
  only if there is no object by that name, and producers racing to create the same one
  cannot overwrite each other. When the object exists, GCS responds `412 Precondition
  Failed`, and the adapter reads it. If it records the same job, every field the same,
  or the same slice with the same rows, the write succeeds: another producer finished
  that slice too, or an earlier attempt was stored although its response was lost.
  Otherwise it fails with an `:incompatible` `OffBroadway.Iceberg.Checkpoint.Error`.

  A job's table is recorded as the name parts the producer resolved `:table` to, so
  `"lake.betting.events"` and `["lake", "betting", "events"]` are the same table, but
  `"events"` is another one, even where DuckDB resolves it to the same table.

  `namespace/1` is the endpoint, bucket and prefix, so producers only share a scan with
  producers that record it in the same folder of the same bucket.

  Failed requests are retried with Req's `retry: :transient`, POSTs included, since
  creating an object that exists already changes nothing. A request that still fails is
  an error: a write is never reported as stored unless GCS confirmed it, and a job whose
  slices cannot be listed is never loaded as empty. Change the retries, timeouts or Finch
  pool with `:req_options`.

  The adapter relies on GCS's strong consistency: an object that was created is seen by
  every later read and listing.

  ## Permissions

  The adapter creates, reads and lists objects, and never overwrites or deletes them.
  `roles/storage.objectCreator` and `roles/storage.objectViewer` on the bucket are enough.

  Delete a job's objects to make its id start over, or use a bucket lifecycle rule to
  delete old jobs.

  ## Authentication

  `auth: {:goth, name}` fetches tokens from a Goth server your application starts. Goth
  finds Application Default Credentials, which on GKE with Workload Identity are those of
  the pod's Kubernetes service account, so no key file is needed:

      children = [
        {Goth, name: MyApp.Goth},
        MyApp.Backfill
      ]

  A function returning `{:ok, token}` works with any other token source. Tokens never
  appear in the producer's options, logs or telemetry.

  ## Local development

  [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) supports everything the
  adapter needs. Point `:endpoint` at it, without authentication:

      {OffBroadway.Iceberg.Checkpoint.GCS,
       bucket: "checkpoints", endpoint: "http://localhost:4443", auth: :none}

  Start it with `-scheme http`, and create the bucket first. See this repository's
  `docker-compose.yml`.
  """

  @behaviour OffBroadway.Iceberg.Checkpoint

  alias OffBroadway.Iceberg.Checkpoint.Error

  @compile {:no_warn_undefined, [Req, Goth]}

  @version 1
  @segment ~r/\A[A-Za-z0-9._~-]+\z/
  @bucket ~r/\A[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]\z/
  @slice_file ~r/\A(\d+)\.json\z/

  @impl true
  def init(opts) do
    with {:ok, opts} <- validate(opts),
         :ok <- require_module(Req, :req),
         :ok <- require_goth(opts[:auth]) do
      {:ok, Map.new(opts)}
    end
  end

  # Buckets are global, but an emulator has buckets of its own, so the endpoint counts.
  @impl true
  def namespace(config), do: {config.endpoint, config.bucket, config.prefix}

  @impl true
  def load(config, job) do
    job_object = job_dir(config, job) <> "job.json"

    with :ok <- create_or_match(config, job_object, job_record(job), &match_job(&1, config, job_object, job)),
         {:ok, objects} <- list(config, slices_prefix(config, job)) do
      collect_slices(objects, config, job)
    end
  end

  @impl true
  def complete(config, job, slice, result) do
    object = slices_prefix(config, job) <> slice_file(slice)
    create_or_match(config, object, slice_record(slice, result), &match_slice(&1, config, object, slice, result))
  end

  @doc false
  def validate_bucket(bucket) do
    if is_binary(bucket) and Regex.match?(@bucket, bucket),
      do: {:ok, bucket},
      else: {:error, "expected a bucket name such as \"my-checkpoints\", got: #{inspect(bucket)}"}
  end

  @doc false
  def validate_prefix(""), do: {:ok, ""}

  def validate_prefix(prefix) when is_binary(prefix) do
    segments = String.split(prefix, "/")

    if Enum.all?(segments, &(Regex.match?(@segment, &1) and &1 not in [".", ".."])),
      do: {:ok, prefix <> "/"},
      else: validate_prefix(nil)
  end

  def validate_prefix(prefix) do
    {:error,
     "expected a prefix of segments made of letters, digits and ._~- separated by /, " <>
       "such as \"backfills/iceberg\", got: #{inspect(prefix)}"}
  end

  @doc false
  def validate_auth(:none), do: {:ok, :none}
  def validate_auth({:goth, _name} = auth), do: {:ok, auth}
  def validate_auth(fun) when is_function(fun, 0), do: {:ok, fun}

  # The value is not shown, since it may be a token given by mistake.
  def validate_auth(_auth), do: {:error, "expected {:goth, name}, a function of no arguments or :none"}

  @doc false
  def validate_endpoint(endpoint) when is_binary(endpoint) do
    case URI.new(endpoint) do
      {:ok, %URI{scheme: scheme, host: host}} when scheme in ["http", "https"] and host not in [nil, ""] ->
        {:ok, String.trim_trailing(endpoint, "/")}

      _invalid ->
        validate_endpoint(nil)
    end
  end

  def validate_endpoint(endpoint), do: {:error, "expected an http or https URL, got: #{inspect(endpoint)}"}

  defp validate(opts) do
    case NimbleOptions.validate(opts, @opts_schema) do
      {:ok, opts} -> {:ok, opts}
      {:error, error} -> {:error, Exception.message(error)}
    end
  end

  defp require_module(module, app) do
    if Code.ensure_loaded?(module),
      do: :ok,
      else: {:error, "#{inspect(__MODULE__)} needs the #{inspect(app)} dependency, which is not available"}
  end

  defp require_goth({:goth, _name}), do: require_module(Goth, :goth)
  defp require_goth(_auth), do: :ok

  ## Records

  defp job_record(job) do
    %{
      "version" => @version,
      "job_id" => job.id,
      "table" => job.table,
      "snapshot_id" => Integer.to_string(job.snapshot_id),
      "plan_fingerprint" => Base.encode16(job.plan, case: :lower),
      "slices" => job.slices
    }
  end

  defp slice_record(slice, result) do
    %{"version" => @version, "slice" => slice, "batches" => result.batches, "rows" => result.rows}
  end

  # Custom metadata values are strings.
  defp to_metadata(record) do
    Map.new(record, fn
      {key, value} when is_integer(value) -> {key, Integer.to_string(value)}
      {key, value} when is_list(value) -> {key, JSON.encode!(value)}
      {key, value} -> {key, value}
    end)
  end

  # Every field of a job is immutable, so the recorded one must match all of them,
  # and have no others.
  defp match_job(metadata, config, object, job) do
    expected = to_metadata(job_record(job))

    cond do
      metadata["version"] != expected["version"] ->
        {:error, unsupported(config, object, metadata)}

      metadata == expected ->
        :ok

      true ->
        differences =
          for key <- (Map.keys(metadata) ++ Map.keys(expected)) |> Enum.uniq() |> Enum.sort(),
              metadata[key] != expected[key] do
            "#{key} #{describe_field(metadata[key])} (this run: #{describe_field(expected[key])})"
          end

        {:error,
         %Error{
           reason: :incompatible,
           message:
             "#{url(config, object)} records another job under this id, with #{Enum.join(differences, ", ")}. " <>
               "A job id names one table, snapshot and plan: use another job id, or the same table name, " <>
               "snapshot, :slice_rows, :columns and :where as the job's first run"
         }}
    end
  end

  defp describe_field(nil), do: "missing"
  defp describe_field(value), do: inspect(value)

  defp match_slice(metadata, config, object, slice, result) do
    case parse_slice(metadata) do
      {:ok, ^slice, %{rows: rows}} when rows == result.rows ->
        :ok

      {:ok, recorded, %{rows: rows}} ->
        {:error,
         %Error{
           reason: :incompatible,
           message:
             "#{url(config, object)} records slice #{recorded} with #{rows} rows, " <>
               "not slice #{slice} with #{result.rows} rows"
         }}

      :error ->
        {:error, unsupported(config, object, metadata)}
    end
  end

  defp collect_slices(objects, config, job) do
    prefix = slices_prefix(config, job)

    Enum.reduce_while(objects, {:ok, %{}}, fn object, {:ok, slices} ->
      name = object["name"]

      with {:ok, file} <- strip_prefix(name, prefix),
           [index] <- Regex.run(@slice_file, file, capture: :all_but_first),
           slice = String.to_integer(index),
           true <- file == slice_file(slice) and slice < job.slices,
           {:ok, ^slice, result} <- parse_slice(object["metadata"]) do
        {:cont, {:ok, Map.put(slices, slice, result)}}
      else
        _invalid ->
          message =
            "#{url(config, to_string(name))} is not a checkpoint of slice 0 to #{job.slices - 1} of job " <>
              "#{inspect(job.id)}. Only the adapter should write under #{url(config, prefix)}"

          {:halt, {:error, %Error{reason: :invalid, message: message}}}
      end
    end)
  end

  defp strip_prefix(name, prefix) when is_binary(name) do
    if String.starts_with?(name, prefix),
      do: {:ok, binary_part(name, byte_size(prefix), byte_size(name) - byte_size(prefix))},
      else: :error
  end

  defp strip_prefix(_name, _prefix), do: :error

  defp parse_slice(%{"version" => version} = metadata) do
    with ^version <- Integer.to_string(@version),
         {:ok, slice} <- non_neg(metadata["slice"]),
         {:ok, batches} <- non_neg(metadata["batches"]),
         {:ok, rows} <- non_neg(metadata["rows"]) do
      {:ok, slice, %{batches: batches, rows: rows}}
    else
      _invalid -> :error
    end
  end

  defp parse_slice(_metadata), do: :error

  defp non_neg(string) when is_binary(string) do
    case Integer.parse(string) do
      {integer, ""} when integer >= 0 -> {:ok, integer}
      _invalid -> :error
    end
  end

  defp non_neg(_value), do: :error

  defp unsupported(config, object, metadata) do
    %Error{
      reason: :invalid,
      message:
        "#{url(config, object)} is not a checkpoint of version #{@version} " <>
          "(its version is #{inspect(metadata["version"])})"
    }
  end

  ## Names

  defp job_dir(config, job), do: config.prefix <> "jobs/" <> URI.encode(job.id, &URI.char_unreserved?/1) <> "/"

  defp slices_prefix(config, job), do: job_dir(config, job) <> "slices/"

  defp slice_file(slice), do: String.pad_leading(Integer.to_string(slice), 6, "0") <> ".json"

  defp url(config, object), do: "gs://#{config.bucket}/#{object}"

  ## Requests

  # Creates the object only if there is none by that name. One that exists is either the
  # same record, created by another producer or by an earlier attempt whose response was
  # lost, or a different one, which is never overwritten.
  defp create_or_match(config, object, record, match) do
    case create(config, object, record) do
      {:ok, %{status: 200}} ->
        :ok

      {:ok, %{status: 412}} ->
        with {:ok, metadata} <- read_metadata(config, object), do: match.(metadata)

      response ->
        {:error, failure(config, object, response)}
    end
  end

  defp create(config, object, record) do
    boundary = "off_broadway_iceberg_" <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)
    resource = %{"name" => object, "contentType" => "application/json", "metadata" => to_metadata(record)}

    request(config,
      method: :post,
      url: "/upload/storage/v1/b/#{encode(config.bucket)}/o",
      params: [uploadType: "multipart", ifGenerationMatch: 0],
      headers: [{"content-type", "multipart/related; boundary=#{boundary}"}],
      body: [part(boundary, JSON.encode!(resource)), part(boundary, JSON.encode!(record)), "--", boundary, "--\r\n"]
    )
  end

  defp part(boundary, json) do
    ["--", boundary, "\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n", json, "\r\n"]
  end

  defp read_metadata(config, object) do
    response =
      request(config,
        method: :get,
        url: "/storage/v1/b/#{encode(config.bucket)}/o/#{encode(object)}",
        params: [fields: "name,metadata"]
      )

    case decode(response) do
      {:ok, %{"metadata" => metadata}} when is_map(metadata) -> {:ok, metadata}
      {:ok, _object} -> {:ok, %{}}
      _failed -> {:error, failure(config, object, response)}
    end
  end

  defp list(config, prefix, page_token \\ nil, pages \\ []) do
    params = [prefix: prefix, fields: "items(name,metadata),nextPageToken", maxResults: 1_000]
    params = if page_token, do: [pageToken: page_token] ++ params, else: params
    response = request(config, method: :get, url: "/storage/v1/b/#{encode(config.bucket)}/o", params: params)

    case decode(response) do
      {:ok, %{"nextPageToken" => token} = page} when is_binary(token) and token != "" ->
        list(config, prefix, token, [Map.get(page, "items", []) | pages])

      {:ok, %{} = page} ->
        {:ok, [Map.get(page, "items", []) | pages] |> Enum.reverse() |> Enum.concat()}

      _failed ->
        {:error, failure(config, prefix, response)}
    end
  end

  defp decode({:ok, %{status: 200, body: body}}) do
    case JSON.decode(body) do
      {:ok, %{} = decoded} -> {:ok, decoded}
      _invalid -> :error
    end
  end

  defp decode(_response), do: :error

  defp request(config, opts) do
    case authorize(config.auth) do
      {:ok, auth} ->
        # Req merges headers and params by name, so those of :req_options are kept.
        [base_url: config.endpoint, retry: :transient, decode_body: false]
        |> Keyword.merge(auth)
        |> Req.new()
        |> Req.merge(config.req_options)
        |> Req.request(opts)

      {:error, reason} ->
        {:error, {:auth, reason}}
    end
  end

  defp authorize(:none), do: {:ok, []}

  defp authorize({:goth, name}) do
    case Goth.fetch(name) do
      {:ok, %{token: token}} -> {:ok, auth: {:bearer, token}}
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp authorize(fun) do
    case fun.() do
      {:ok, token} when is_binary(token) -> {:ok, auth: {:bearer, token}}
      {:error, reason} -> {:error, reason}
      _other -> {:error, :invalid_token}
    end
  end

  # A 200 response whose body is not what the JSON API returns.
  defp failure(config, object, {:ok, %{status: 200}}) do
    %Error{reason: :invalid, message: "#{url(config, object)}: GCS responded with a body that is not a JSON object"}
  end

  defp failure(config, object, {:ok, %{status: status, body: body}}) do
    reason =
      case status do
        status when status in [401, 403] -> :unauthorized
        404 -> :not_found
        _status -> :unavailable
      end

    %Error{reason: reason, message: "#{url(config, object)}: GCS responded #{status}#{gcs_message(body)}"}
  end

  defp failure(config, object, {:error, {:auth, reason}}) do
    %Error{reason: :unauthorized, message: "#{url(config, object)}: could not fetch an access token: #{describe(reason)}"}
  end

  defp failure(config, object, {:error, reason}) do
    %Error{reason: :unavailable, message: "#{url(config, object)}: #{describe(reason)}"}
  end

  defp gcs_message(body) do
    case JSON.decode(body) do
      {:ok, %{"error" => %{"message" => message}}} when is_binary(message) -> ": " <> message
      _other -> ""
    end
  end

  defp describe(exception) when is_exception(exception), do: Exception.message(exception)
  defp describe(reason), do: inspect(reason)

  defp encode(segment), do: URI.encode(segment, &URI.char_unreserved?/1)
end
