defmodule OffBroadway.Iceberg.Integration.CheckpointTest do
  # Checkpoints in fake-gcs-server (see docker-compose.yml), first on their own, then for
  # scans of a real Iceberg table.
  use ExUnit.Case, async: false

  alias Broadway.Message
  alias OffBroadway.Iceberg.Checkpoint.Error
  alias OffBroadway.Iceberg.Checkpoint.GCS
  alias OffBroadway.Iceberg.Test.Iceberg
  alias OffBroadway.Iceberg.Test.Pipeline
  alias OffBroadway.Iceberg.Test.Telemetry

  @moduletag :integration
  @moduletag :capture_log
  @moduletag timeout: 180_000

  @pool OffBroadway.Iceberg.Integration.CheckpointPool
  @port 19_495
  @endpoint "http://localhost:14443"
  @bucket "checkpoints"

  setup_all do
    :ok = Iceberg.start_catalog!()
    on_exit(fn -> Iceberg.stop_catalog!() end)

    %{status: 200} = Req.post!("#{@endpoint}/storage/v1/b", json: %{name: @bucket}, retry: false)

    start_supervised!(Iceberg.duckdb_spec(@pool, @port))
    Iceberg.attach_catalog!(@pool)

    %{schema: Iceberg.create_schema!(@pool)}
  end

  setup do
    %{backend: [bucket: @bucket, endpoint: @endpoint, auth: :none, prefix: "it-#{System.unique_integer([:positive])}"]}
  end

  describe "the emulator" do
    test "refuses to replace an object created with ifGenerationMatch=0", %{backend: backend} do
      name = backend[:prefix] <> "/object.json"
      first = create(name, ~s({"n":1}))
      second = create(name, ~s({"n":2}))

      assert {first.status, second.status} == {200, 412}
      assert read(name) == ~s({"n":1})
    end
  end

  describe "the GCS adapter" do
    test "records a job and its slices, and loads them back", %{backend: backend} do
      {:ok, config} = GCS.init(backend)
      job = job("events-backfill-2026-11")

      assert {:ok, %{}} = GCS.load(config, job)
      for slice <- [0, 5, 7], do: assert(:ok = GCS.complete(config, job, slice, %{batches: 2, rows: slice}))

      assert GCS.load(config, job) ==
               {:ok, %{0 => %{batches: 2, rows: 0}, 5 => %{batches: 2, rows: 5}, 7 => %{batches: 2, rows: 7}}}

      assert {:error, %Error{reason: :incompatible}} = GCS.load(config, %{job | snapshot_id: 1})
      assert {:error, %Error{reason: :incompatible}} = GCS.complete(config, job, 5, %{batches: 2, rows: 6})
    end

    test "stores every job id under a name of its own", %{backend: backend} do
      {:ok, config} = GCS.init(backend)
      ids = ["a/b", "a%2Fb", "a b", "../x", "ü"]

      for id <- ids do
        job = job(id)
        assert {:ok, %{}} = GCS.load(config, job)
        assert :ok = GCS.complete(config, job, 1, %{batches: 1, rows: 1})
        assert {:ok, %{1 => _result}} = GCS.load(config, job)
      end

      assert length(list(backend[:prefix] <> "/")) == 2 * length(ids)
    end

    test "records a slice once when many producers finish it at the same time", %{backend: backend} do
      {:ok, config} = GCS.init(backend)
      job = job("concurrent")
      {:ok, %{}} = GCS.load(config, job)

      results =
        1..20
        |> Task.async_stream(fn batches -> GCS.complete(config, job, 3, %{batches: batches, rows: 30}) end)
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.uniq(results) == [:ok]
      assert [_one] = list(backend[:prefix] <> "/jobs/concurrent/slices/")
    end

    test "reports a missing bucket as such", %{backend: backend} do
      {:ok, config} = GCS.init(Keyword.put(backend, :bucket, "missing-bucket"))

      assert {:error, %Error{reason: :not_found}} = GCS.load(config, job("job"))
    end
  end

  describe "a checkpointed scan" do
    test "reads only the slices that the previous run of its job did not finish", %{schema: schema, backend: backend} do
      table = Iceberg.create_table!(@pool, schema)
      # 8 inserts make 8 data files of 1,250 rows, and a slice each.
      for file <- 0..7, do: Iceberg.insert!(@pool, table, (file * 1_250)..(file * 1_250 + 1_249))
      snapshot = @pool |> Iceberg.snapshots(table) |> List.last()
      job_id = "it-job-#{System.unique_integer([:positive])}"
      Telemetry.attach([[:off_broadway_iceberg, :scan, :stop]], &(&1[:job_id] == job_id))

      producer = [
        connection: @pool,
        table: table,
        snapshot_id: snapshot,
        fetch_size: 500,
        distribute: [slice_rows: 1_250],
        checkpoint: [job_id: job_id, backend: {GCS, backend}]
      ]

      # The first run fails every batch of slice 3.
      test = self()

      handler = fn message ->
        send(test, {:handled, message})
        if message.metadata.slice == 3, do: Message.failed(message, "boom"), else: message
      end

      first = run(producer, handler, producer_concurrency: 2)
      assert %{rows: 10_000, failed_batches: failed} = first
      assert failed > 0

      prefix = "#{backend[:prefix]}/jobs/#{job_id}/slices/"
      assert list(prefix) == for(slice <- [0, 1, 2, 4, 5, 6, 7], do: prefix <> "00000#{slice}.json")
      flush_handled()

      # The second run reads slice 3 alone, and records it.
      second = run(producer, &send(test, {:handled, &1}), producer_concurrency: 2)
      assert %{rows: 10_000, failed_batches: 0} = second

      messages = flush_handled()
      assert messages |> Enum.map(& &1.metadata.slice) |> Enum.uniq() == [3]
      # Producers that start together can both read a slice.
      ids = messages |> Enum.flat_map(&QuackDB.Columns.get(&1.data, "id")) |> Enum.uniq() |> Enum.sort()
      assert ids == Enum.to_list(3_750..4_999)
      assert length(list(prefix)) == 8
    end
  end

  # Runs a pipeline until each of its producers reports :stop, and returns the
  # measurements of the first report.
  defp run(producer, handler, opts) do
    name = :"pipeline_#{System.unique_integer([:positive])}"
    stages = Keyword.fetch!(opts, :producer_concurrency)

    start_supervised!(
      {Pipeline,
       name: name,
       test: self(),
       handler: handler,
       producer: producer,
       producer_concurrency: stages,
       processor_concurrency: 2,
       max_demand: 1},
      id: name,
      restart: :temporary
    )

    [measurements | _others] =
      for _stage <- 1..stages do
        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, _metadata}, 60_000
        measurements
      end

    stop_supervised!(name)
    measurements
  end

  defp job(id) do
    %{id: id, table: ["lake", "events"], snapshot_id: 42, plan: :crypto.hash(:sha256, "plan"), slices: 8}
  end

  defp create(name, content) do
    Req.post!("#{@endpoint}/upload/storage/v1/b/#{@bucket}/o",
      params: [uploadType: "media", name: name, ifGenerationMatch: 0],
      headers: [{"content-type", "application/json"}],
      body: content,
      retry: false
    )
  end

  defp read(name) do
    Req.get!("#{@endpoint}/storage/v1/b/#{@bucket}/o/#{URI.encode(name, &URI.char_unreserved?/1)}",
      params: [alt: "media"],
      decode_body: false,
      retry: false
    ).body
  end

  defp list(prefix) do
    %{status: 200, body: body} =
      Req.get!("#{@endpoint}/storage/v1/b/#{@bucket}/o", params: [prefix: prefix], retry: false)

    body |> Map.get("items", []) |> Enum.map(& &1["name"]) |> Enum.sort()
  end

  defp flush_handled(messages \\ []) do
    receive do
      {:handled, message} -> flush_handled([message | messages])
    after
      200 -> Enum.reverse(messages)
    end
  end
end
