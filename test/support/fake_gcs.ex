defmodule OffBroadway.Iceberg.Test.FakeGCS do
  @moduledoc """
  The parts of the GCS JSON API the checkpoint adapter uses, as a Req adapter, so that
  tests can run the adapter without a network and make GCS fail in ways a real server or
  an emulator will not on demand.

  Pass `req_options(fake)` as the adapter's `:req_options`. It serves the bucket
  `"checkpoints"`:

  - Multipart uploads with `ifGenerationMatch=0`, which answer 412 for an object that
    exists.
  - Object reads, which answer 404 for an object that does not.
  - Listings by prefix, `:page_size` objects per page (default 1_000).

  `inject/3` queues a fault for the next request of a kind (`:create`, `:read` or
  `:list`):

  - `{:status, status}` - answers with that status, and does nothing else.
  - `:lost` - does what was asked, then fails as a connection closed before the response.
  - `:closed` - fails as a connection closed, and does nothing.
  """

  use Agent

  @bucket "checkpoints"

  def start_link(opts \\ []) do
    name = :"#{__MODULE__}.#{System.unique_integer([:positive])}"
    state = %{objects: %{}, faults: [], requests: [], page_size: Keyword.get(opts, :page_size, 1_000)}
    {:ok, _pid} = Agent.start_link(fn -> state end, name: name)
    {:ok, name}
  end

  def bucket, do: @bucket

  @doc "Options for Req that send requests to `fake`, and retry at once."
  def req_options(fake) do
    [adapter: __MODULE__, headers: [{"x-fake-gcs", Atom.to_string(fake)}], retry_delay: fn _count -> 0 end]
  end

  def inject(fake, kind, fault), do: Agent.update(fake, &%{&1 | faults: &1.faults ++ [{kind, fault}]})

  @doc "Stores an object as anything but the adapter would."
  def put(fake, name, metadata), do: Agent.update(fake, &put_in(&1.objects[name], %{metadata: metadata, content: "{}"}))

  @doc "The objects, by name, as `%{metadata: map, content: binary}`."
  def objects(fake), do: Agent.get(fake, & &1.objects)

  @doc "The requests made so far, oldest first, as `{kind, request}`."
  def requests(fake), do: fake |> Agent.get(& &1.requests) |> Enum.reverse()

  # Req adapters are modules, so the fake is named in a header of the request.
  @doc false
  def run(request) do
    [fake] = Req.Request.get_header(request, "x-fake-gcs")
    handle(String.to_existing_atom(fake), request)
  end

  defp handle(fake, request) do
    kind = kind(request)
    fault = Agent.get_and_update(fake, &pop_fault(&1, kind))
    Agent.update(fake, &%{&1 | requests: [{kind, request} | &1.requests]})

    case fault do
      {:status, status} -> {request, error(status)}
      :closed -> {request, %Req.TransportError{reason: :closed}}
      :lost -> {elem(serve(fake, kind, request), 0), %Req.TransportError{reason: :closed}}
      nil -> serve(fake, kind, request)
    end
  end

  defp kind(%{method: :post, url: %{path: "/upload/storage/v1/b/" <> _rest}}), do: :create
  defp kind(%{method: :get, url: %{path: path}}), do: if(String.ends_with?(path, "/o"), do: :list, else: :read)

  defp pop_fault(state, kind) do
    case Enum.split_while(state.faults, fn {fault_kind, _fault} -> fault_kind != kind end) do
      {before, [{^kind, fault} | rest]} -> {fault, %{state | faults: before ++ rest}}
      {_all, []} -> {nil, state}
    end
  end

  defp serve(fake, :create, request) do
    %{"uploadType" => "multipart", "ifGenerationMatch" => "0"} = URI.decode_query(request.url.query)
    "/upload/storage/v1/b/" <> bucket = String.trim_trailing(request.url.path, "/o")
    [resource, content] = parts(request)
    %{"name" => name, "metadata" => metadata} = JSON.decode!(resource)

    Agent.get_and_update(fake, fn state ->
      cond do
        bucket != @bucket ->
          {{request, error(404)}, state}

        is_map_key(state.objects, name) ->
          {{request, error(412)}, state}

        true ->
          {{request, ok(%{"name" => name, "metadata" => metadata})},
           put_in(state.objects[name], %{metadata: metadata, content: content})}
      end
    end)
  end

  defp serve(fake, :read, request) do
    "/storage/v1/b/" <> rest = request.url.path
    [bucket, name] = String.split(rest, "/o/", parts: 2)
    name = URI.decode(name)

    case Agent.get(fake, & &1.objects[name]) do
      %{metadata: metadata} when bucket == @bucket -> {request, ok(%{"name" => name, "metadata" => metadata})}
      _missing -> {request, error(404)}
    end
  end

  defp serve(fake, :list, request) do
    "/storage/v1/b/" <> bucket = String.trim_trailing(request.url.path, "/o")
    query = URI.decode_query(request.url.query)
    prefix = Map.get(query, "prefix", "")
    {objects, page_size} = Agent.get(fake, &{&1.objects, &1.page_size})

    names =
      objects
      |> Map.keys()
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()
      |> Enum.drop_while(&(&1 < Map.get(query, "pageToken", "")))

    {page, rest} = Enum.split(names, page_size)
    items = for name <- page, do: %{"name" => name, "metadata" => objects[name].metadata}
    body = if rest == [], do: %{"items" => items}, else: %{"items" => items, "nextPageToken" => hd(rest)}

    if bucket == @bucket, do: {request, ok(body)}, else: {request, error(404)}
  end

  defp parts(request) do
    [content_type] = Req.Request.get_header(request, "content-type")
    "multipart/related; boundary=" <> boundary = content_type
    body = IO.iodata_to_binary(request.body)

    body
    |> String.split("--" <> boundary)
    |> Enum.slice(1..-2//1)
    |> Enum.map(fn part ->
      [_headers, content] = String.split(part, "\r\n\r\n", parts: 2)
      String.trim_trailing(content, "\r\n")
    end)
  end

  defp ok(body), do: Req.Response.new(status: 200, body: JSON.encode!(body))

  defp error(status) do
    Req.Response.new(
      status: status,
      body: JSON.encode!(%{"error" => %{"code" => status, "message" => "status #{status}"}})
    )
  end
end
