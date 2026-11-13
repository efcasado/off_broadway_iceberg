defmodule OffBroadway.Iceberg.Checkpoint.GCSTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Iceberg.Checkpoint.Error
  alias OffBroadway.Iceberg.Checkpoint.GCS
  alias OffBroadway.Iceberg.Test.FakeGCS

  # Req logs the requests it retries.
  @moduletag :capture_log

  @job %{
    id: "events-backfill-2026-11",
    table: ["lake", "betting", "events"],
    snapshot_id: 0x7FFFFFFFFFFFFFFF,
    plan: :crypto.hash(:sha256, "plan"),
    slices: 8
  }

  describe "options" do
    test "need a bucket and an authentication, and default the rest" do
      assert {:ok, config} = GCS.init(bucket: "my-checkpoints", auth: :none)
      assert %{bucket: "my-checkpoints", prefix: "", endpoint: "https://storage.googleapis.com"} = config

      assert {:error, message} = GCS.init(auth: :none)
      assert message =~ "required :bucket option not found"

      assert {:error, message} = GCS.init(bucket: "my-checkpoints")
      assert message =~ "required :auth option not found"
    end

    test "take a prefix of plain segments, and a folder" do
      assert {:ok, %{prefix: "iceberg/"}} = GCS.init(bucket: "b-1", auth: :none, prefix: "iceberg")
      assert {:ok, %{prefix: "a/b.c/d_e~f-g/"}} = GCS.init(bucket: "b-1", auth: :none, prefix: "a/b.c/d_e~f-g")

      for prefix <- ["/iceberg", "iceberg/", "a//b", "..", "a/./b", "a b", "a%2Fb", "ü", :iceberg] do
        assert {:error, message} = GCS.init(bucket: "b-1", auth: :none, prefix: prefix)
        assert message =~ ":prefix"
      end
    end

    test "reject buckets, endpoints and authentications that cannot work" do
      for bucket <- ["", "B", "-b", "b/c", "a", :b] do
        assert {:error, message} = GCS.init(bucket: bucket, auth: :none)
        assert message =~ ":bucket"
      end

      for endpoint <- ["localhost:4443", "ftp://localhost", "http://", :local] do
        assert {:error, message} = GCS.init(bucket: "b-1", auth: :none, endpoint: endpoint)
        assert message =~ ":endpoint"
      end

      assert {:ok, %{endpoint: "http://localhost:4443"}} =
               GCS.init(bucket: "b-1", auth: :none, endpoint: "http://localhost:4443/")
    end

    test "never show an authentication that is not valid, which may be a token" do
      assert {:error, message} = GCS.init(bucket: "b-1", auth: "ya29.secret-token")
      assert message =~ ":auth"
      refute message =~ "secret-token"
    end

    test "take a Goth server or a function" do
      assert {:ok, %{auth: {:goth, MyApp.Goth}}} = GCS.init(bucket: "b-1", auth: {:goth, MyApp.Goth})
      assert {:ok, _config} = GCS.init(bucket: "b-1", auth: fn -> {:ok, "token"} end)
    end
  end

  describe "load" do
    test "records a new job and finds no slices" do
      {config, fake} = start(prefix: "iceberg")

      assert {:ok, %{}} = GCS.load(config, @job)

      assert %{"iceberg/jobs/events-backfill-2026-11/job.json" => %{metadata: metadata, content: content}} =
               FakeGCS.objects(fake)

      plan = Base.encode16(@job.plan, case: :lower)

      assert metadata == %{
               "version" => "1",
               "job_id" => "events-backfill-2026-11",
               "table" => ~s(["lake","betting","events"]),
               "snapshot_id" => "9223372036854775807",
               "plan_fingerprint" => plan,
               "slices" => "8"
             }

      # The snapshot id is a string, so that no JSON reader rounds it.
      assert JSON.decode!(content) == %{
               "version" => 1,
               "job_id" => "events-backfill-2026-11",
               "table" => ["lake", "betting", "events"],
               "snapshot_id" => "9223372036854775807",
               "plan_fingerprint" => plan,
               "slices" => 8
             }
    end

    test "finds the slices finished by earlier runs of the job" do
      {config, _fake} = start()

      assert {:ok, %{}} = GCS.load(config, @job)
      assert :ok = GCS.complete(config, @job, 0, %{batches: 3, rows: 30})
      assert :ok = GCS.complete(config, @job, 7, %{batches: 1, rows: 5})

      assert GCS.load(config, @job) == {:ok, %{0 => %{batches: 3, rows: 30}, 7 => %{batches: 1, rows: 5}}}
    end

    test "reads every page of a long listing" do
      {config, fake} = start(page_size: 2)

      for slice <- 0..6, do: :ok = GCS.complete(config, @job, slice, %{batches: 1, rows: slice})

      assert {:ok, slices} = GCS.load(config, @job)
      assert slices == Map.new(0..6, &{&1, %{batches: 1, rows: &1}})
      assert fake |> FakeGCS.requests() |> Enum.count(&match?({:list, _request}, &1)) == 4
    end

    test "refuses a job id recorded for another snapshot or plan, and leaves it as it was" do
      {config, fake} = start()
      assert {:ok, %{}} = GCS.load(config, @job)
      recorded = FakeGCS.objects(fake)

      for other <- [%{@job | snapshot_id: 1}, %{@job | plan: :crypto.hash(:sha256, "other")}] do
        assert {:error, %Error{reason: :incompatible, message: message}} = GCS.load(config, other)
        assert message =~ "gs://checkpoints/jobs/events-backfill-2026-11/job.json records job"
      end

      assert FakeGCS.objects(fake) == recorded
    end

    test "accepts the same job however its table was named" do
      {config, _fake} = start()

      assert {:ok, %{}} = GCS.load(config, @job)
      assert {:ok, %{}} = GCS.load(config, %{@job | table: ["events"]})
    end

    test "keeps every job id apart, under the prefix" do
      {config, fake} = start(prefix: "iceberg")
      ids = ["a/b", "a%2Fb", "a b", "../x", "jobs", "ü"]

      for id <- ids, do: assert({:ok, %{}} = GCS.load(config, %{@job | id: id}))

      assert fake |> FakeGCS.objects() |> Map.keys() |> Enum.sort() == [
               "iceberg/jobs/%C3%BC/job.json",
               "iceberg/jobs/..%2Fx/job.json",
               "iceberg/jobs/a%20b/job.json",
               "iceberg/jobs/a%252Fb/job.json",
               "iceberg/jobs/a%2Fb/job.json",
               "iceberg/jobs/jobs/job.json"
             ]
    end

    test "fails on objects among the slices that the adapter did not write" do
      prefix = "jobs/events-backfill-2026-11/slices/"
      valid = %{"version" => "1", "slice" => "1", "batches" => "1", "rows" => "1"}

      for {name, metadata} <- [
            {"1.json", valid},
            {"000001.txt", valid},
            {"000008.json", %{valid | "slice" => "8"}},
            {"000001.json", %{valid | "slice" => "2"}},
            {"000001.json", %{valid | "rows" => "-1"}},
            {"000001.json", %{valid | "version" => "2"}},
            {"000001.json", %{}}
          ] do
        {config, fake} = start()
        FakeGCS.put(fake, prefix <> name, metadata)

        assert {:error, %Error{reason: :invalid, message: message}} = GCS.load(config, @job)
        assert message =~ "gs://checkpoints/#{prefix}#{name} is not a checkpoint of slice 0 to 7"
      end
    end

    test "never takes a failed listing for a job without finished slices" do
      for {status, reason} <- [{401, :unauthorized}, {403, :unauthorized}, {404, :not_found}, {503, :unavailable}] do
        {config, fake} = start(req_options: [retry: false])
        FakeGCS.inject(fake, :list, {:status, status})

        assert {:error, %Error{reason: ^reason, message: message}} = GCS.load(config, @job)
        assert message =~ "gs://checkpoints/jobs/events-backfill-2026-11/slices/: GCS responded #{status}"
      end
    end

    test "fails for a bucket that does not exist" do
      {config, _fake} = start()

      assert {:error, %Error{reason: :not_found, message: message}} = GCS.load(%{config | bucket: "missing"}, @job)
      assert message =~ "gs://missing/jobs/events-backfill-2026-11/job.json: GCS responded 404"
    end
  end

  describe "complete" do
    test "creates the slice's object only if there is none by that name" do
      {config, fake} = start(prefix: "iceberg")

      assert :ok = GCS.complete(config, @job, 17, %{batches: 24, rows: 240_000})

      assert [{:create, request}] = FakeGCS.requests(fake)
      assert %{"ifGenerationMatch" => "0", "uploadType" => "multipart"} = URI.decode_query(request.url.query)

      assert %{"iceberg/jobs/events-backfill-2026-11/slices/000017.json" => %{metadata: metadata, content: content}} =
               FakeGCS.objects(fake)

      assert metadata == %{"version" => "1", "slice" => "17", "batches" => "24", "rows" => "240000"}
      assert JSON.decode!(content) == %{"version" => 1, "slice" => 17, "batches" => 24, "rows" => 240_000}
    end

    test "succeeds for a slice recorded with the same rows, whatever its batches" do
      {config, fake} = start()

      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})
      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})
      assert :ok = GCS.complete(config, @job, 3, %{batches: 5, rows: 10})

      assert [%{metadata: %{"batches" => "2"}}] = Map.values(FakeGCS.objects(fake))
    end

    test "fails for a slice recorded with other rows, and leaves it as it was" do
      {config, fake} = start()
      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})

      assert {:error, %Error{reason: :incompatible, message: message}} =
               GCS.complete(config, @job, 3, %{batches: 2, rows: 11})

      assert message =~ "records slice 3 with 10 rows, not slice 3 with 11 rows"
      assert [%{metadata: %{"rows" => "10"}}] = Map.values(FakeGCS.objects(fake))
    end

    test "fails for a slice recorded by something else" do
      {config, fake} = start()
      FakeGCS.put(fake, "jobs/events-backfill-2026-11/slices/000003.json", %{"version" => "2"})

      assert {:error, %Error{reason: :invalid, message: message}} =
               GCS.complete(config, @job, 3, %{batches: 2, rows: 10})

      assert message =~ ~s[is not a checkpoint of version 1 (its version is "2")]
    end

    test "recognizes its own write when a retry finds the object a lost response created" do
      {config, fake} = start()
      FakeGCS.inject(fake, :create, :lost)

      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})
      assert fake |> FakeGCS.requests() |> Enum.map(&elem(&1, 0)) == [:create, :create, :read]
    end

    test "fails when the response is lost and not retried, though a load sees the slice" do
      {config, fake} = start(req_options: [retry: false])
      FakeGCS.inject(fake, :create, :lost)

      assert {:error, %Error{reason: :unavailable, message: message}} =
               GCS.complete(config, @job, 3, %{batches: 2, rows: 10})

      assert message =~ "gs://checkpoints/jobs/events-backfill-2026-11/slices/000003.json"
      assert message =~ "closed"
      assert {:ok, %{3 => %{batches: 2, rows: 10}}} = GCS.load(config, @job)
    end

    test "retries failures that may pass, and then fails" do
      {config, fake} = start(req_options: [max_retries: 2])
      for _attempt <- 1..3, do: FakeGCS.inject(fake, :create, {:status, 503})

      assert {:error, %Error{reason: :unavailable, message: message}} =
               GCS.complete(config, @job, 3, %{batches: 2, rows: 10})

      assert message =~ "GCS responded 503: status 503"
      assert FakeGCS.objects(fake) == %{}

      FakeGCS.inject(fake, :create, :closed)
      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})
    end

    test "fails when an existing object cannot be read" do
      {config, fake} = start(req_options: [retry: false])
      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})
      FakeGCS.inject(fake, :read, {:status, 500})

      assert {:error, %Error{reason: :unavailable}} = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})
    end

    test "does not retry what will fail again" do
      for status <- [400, 401, 403, 404] do
        {config, fake} = start()
        FakeGCS.inject(fake, :create, {:status, status})

        assert {:error, %Error{}} = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})
        assert [{:create, _request}] = FakeGCS.requests(fake)
      end
    end
  end

  describe "authentication" do
    test "sends the token of the function, and never shows it" do
      {config, fake} = start(auth: fn -> {:ok, "secret-token"} end, req_options: [retry: false])
      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})

      assert [{:create, request}] = FakeGCS.requests(fake)
      assert Req.Request.get_header(request, "authorization") == ["Bearer secret-token"]

      FakeGCS.inject(fake, :list, {:status, 401})
      assert {:error, %Error{reason: :unauthorized} = error} = GCS.load(config, @job)
      refute inspect(error) =~ "secret-token"
    end

    test "sends no token without authentication" do
      {config, fake} = start(auth: :none)
      assert :ok = GCS.complete(config, @job, 3, %{batches: 2, rows: 10})

      assert [{:create, request}] = FakeGCS.requests(fake)
      assert Req.Request.get_header(request, "authorization") == []
    end

    test "fails without a token, and sends nothing" do
      {config, fake} = start(auth: fn -> {:error, :metadata_server_unreachable} end)

      assert {:error, %Error{reason: :unauthorized, message: message}} =
               GCS.complete(config, @job, 3, %{batches: 2, rows: 10})

      assert message =~ "could not fetch an access token: :metadata_server_unreachable"
      assert FakeGCS.requests(fake) == []
    end

    test "fails when the Goth server is not running" do
      {config, _fake} = start(auth: {:goth, __MODULE__.MissingGoth})

      assert {:error, %Error{reason: :unauthorized, message: message}} = GCS.load(config, @job)
      assert message =~ "could not fetch an access token"
    end
  end

  defp start(opts \\ []) do
    {:ok, fake} = FakeGCS.start_link(Keyword.take(opts, [:page_size]))
    req_options = Keyword.merge(FakeGCS.req_options(fake), Keyword.get(opts, :req_options, []))

    {:ok, config} =
      [bucket: FakeGCS.bucket(), auth: :none]
      |> Keyword.merge(Keyword.take(opts, [:prefix, :auth]))
      |> Keyword.put(:req_options, req_options)
      |> GCS.init()

    {config, fake}
  end
end
