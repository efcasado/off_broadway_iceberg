defmodule OffBroadway.Iceberg.CheckpointTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias OffBroadway.Iceberg.Checkpoint.Error
  alias OffBroadway.Iceberg.Checkpoint.GCS
  alias OffBroadway.Iceberg.Producer
  alias OffBroadway.Iceberg.Test.Checkpoint
  alias OffBroadway.Iceberg.Test.FakeConnection
  alias OffBroadway.Iceberg.Test.ManualConsumer
  alias OffBroadway.Iceberg.Test.Telemetry

  # DBConnection logs the disconnects that cancelled scans cause.
  @moduletag :capture_log

  # 32 data files of 10 rows. With a slice of 10 rows, each file is a slice, and with 5
  # rows per message, each slice makes 2 messages.
  @slices 32
  @rows 320
  @batches 64
  @files for file <- 0..(@slices - 1),
             do: {"file-#{String.pad_leading("#{file}", 2, "0")}", Enum.map((file * 10)..(file * 10 + 9), &[&1])}
  @result %{batches: 2, rows: 10}

  @stop [:off_broadway_iceberg, :scan, :stop]
  @exception [:off_broadway_iceberg, :scan, :exception]
  @load [:off_broadway_iceberg, :checkpoint, :load]
  @write [:off_broadway_iceberg, :checkpoint, :write]

  setup do
    table = "events_#{System.unique_integer([:positive])}"
    Telemetry.attach([[:off_broadway_iceberg, :scan, :start], @stop, @exception, @load, @write], &(&1.table == table))
    {:ok, store} = Checkpoint.start_link()

    %{table: table, store: store, job_id: "job-#{System.unique_integer([:positive])}"}
  end

  describe "options" do
    test "require :distribute" do
      assert_raise ArgumentError, ~r/requires :distribute with :checkpoint/, fn ->
        prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, checkpoint: checkpoint("job", self()))
      end
    end

    test "require a job id and a backend that implements the behaviour" do
      for {checkpoint, message} <- [
            {[backend: {Checkpoint, store: self()}], ~r/required :job_id option not found/},
            {[job_id: "job"], ~r/required :backend option not found/},
            {[job_id: "job", backend: {String, []}],
             ~r/expected a module that implements OffBroadway.Iceberg.Checkpoint, got: String/},
            {[job_id: "job", backend: Checkpoint], ~r/expected \{module, options\}/},
            {[job_id: "job", backend: {Checkpoint, []}],
             ~r/invalid options for OffBroadway.Iceberg.Test.Checkpoint: required :store option not found/},
            {[job_id: "job", backend: {GCS, bucket: "B", auth: :none}], ~r/:bucket/}
          ] do
        assert_raise NimbleOptions.ValidationError, message, fn -> prepare(distributed(checkpoint: checkpoint)) end
      end
    end

    test "reject job ids that cannot name a job" do
      for job_id <- ["", ".", "..", "a\nb", <<0>>, <<0xFF>>, String.duplicate("j", 129), :job, 42] do
        assert_raise NimbleOptions.ValidationError, ~r/:job_id/, fn ->
          prepare(distributed(checkpoint: checkpoint(job_id, self())))
        end
      end
    end

    test "accept job ids of any other characters" do
      for job_id <- ["events-backfill-2026-11", "a/b", "a b", "ü", "../x", String.duplicate("j", 128)] do
        assert {[], _opts} = prepare(distributed(checkpoint: checkpoint(job_id, self())), 2)
      end
    end
  end

  describe "a checkpointed scan" do
    test "records each slice once all its batches succeeded, and only then counts it finished", ctx do
      %{job_id: job_id} = ctx
      Checkpoint.hold(ctx.store)

      %{consumer: consumer} = start_member(ctx, :a)
      ManualConsumer.ask(consumer, 1_000)

      written = for _slice <- 1..@slices, do: receive_write(job_id)
      assert Enum.sort(written) == for(slice <- 0..(@slices - 1), do: {slice, @result})
      refute_receive {:telemetry, @stop, _measurements, _metadata}, 200

      Checkpoint.release(ctx.store)

      assert_receive {:telemetry, @stop, measurements, %{job_id: ^job_id}}, 5_000
      assert %{rows: @rows, batches: @batches, failed_batches: 0} = measurements
      assert Checkpoint.slices(ctx.store, job_id) == Map.new(0..(@slices - 1), &{&1, @result})

      for _slice <- 1..@slices do
        assert_received {:telemetry, @write, %{batches: 2, rows: 10, duration: _}, %{slice: _, job_id: ^job_id}}
      end
    end

    test "does not read the slices an earlier run of its job finished", ctx do
      %{job_id: job_id} = ctx
      Checkpoint.put_slices(ctx.store, job_id, Map.new(0..15, &{&1, @result}))

      %{consumer: consumer} = start_member(ctx, :a)
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, @load, %{slices: @slices, finished: 16, duration: _},
                      %{job_id: ^job_id, snapshot_id: 42}}

      assert_receive {:telemetry, @stop, %{rows: @rows, batches: @batches, failed_batches: 0}, _metadata}, 5_000

      declared = for {:fake, {:declare, _statement, [42 | paths], _max_rows}} <- flush(), path <- paths, do: path
      assert Enum.sort(declared) == for({path, _rows} <- Enum.drop(@files, 16), do: path)
      assert ctx.store |> Checkpoint.slices(job_id) |> map_size() == @slices
    end

    test "reads nothing when an earlier run of its job finished every slice", ctx do
      Checkpoint.put_slices(ctx.store, ctx.job_id, Map.new(0..(@slices - 1), &{&1, @result}))

      start_member(ctx, :a)

      assert_receive {:telemetry, @stop, %{rows: @rows, batches: @batches, failed_batches: 0}, _metadata}, 5_000
      refute_received {:fake, {:declare, _statement, _params, _max_rows}}
      refute_received {:checkpoint, {:complete, _job_id, _slice, _result}}
    end

    test "loads before reading anything, and fails without reading if it cannot", ctx do
      error = %Error{reason: :unauthorized, message: "GCS responded 403"}
      Checkpoint.fail(ctx.store, :load, error)

      %{consumer: consumer} = start_member(ctx, :a)
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, @exception, %{batches: 0}, %{reason: ^error, snapshot_id: 42, job_id: job_id}}
      assert job_id == ctx.job_id
      refute_receive {:fake, {:declare, _statement, _params, _max_rows}}, 200
    end

    test "fails when its job id was used for another snapshot or plan", ctx do
      Checkpoint.put_job(ctx.store, ctx.job_id, 42, :crypto.hash(:sha256, "another plan"))

      start_member(ctx, :a)

      assert_receive {:telemetry, @exception, _measurements, %{reason: %Error{reason: :incompatible}}}
      refute_receive {:fake, {:declare, _statement, _params, _max_rows}}, 200
    end

    test "fails when the backend loads slices the plan does not have", ctx do
      Checkpoint.put_slices(ctx.store, ctx.job_id, %{@slices => @result})

      start_member(ctx, :a)

      assert_receive {:telemetry, @exception, _measurements, %{reason: %Error{reason: :invalid, message: message}}}
      assert message =~ "for slice 32, which is not a finished slice of a plan of 32 slices"
    end

    test "resumes after its producer is killed mid-scan, without reading the recorded slices again", ctx do
      %{job_id: job_id} = ctx
      %{producer: producer, consumer: consumer} = start_member(ctx, :a)
      # The messages of the first 10 slices. The scan only sees that a slice ended when
      # it is asked for the next message, so the 10th slice is not finished.
      ManualConsumer.ask(consumer, 20)

      for _slice <- 1..9, do: assert_receive({:telemetry, @write, _measurements, _metadata}, 5_000)
      refute_receive {:telemetry, @write, _measurements, _metadata}, 100
      Process.exit(producer, :kill)

      assert ctx.store |> Checkpoint.slices(job_id) |> Map.keys() |> Enum.sort() == Enum.to_list(0..8)
      flush()

      %{consumer: consumer} = start_member(ctx, :b)
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, @stop, %{rows: @rows, batches: @batches, failed_batches: 0}, _metadata}, 5_000
      assert :b |> received() |> Enum.map(& &1.metadata.slice) |> Enum.uniq() |> Enum.sort() == Enum.to_list(9..31)
    end

    test "does not record a slice with failed batches, which the next run of its job reads again", ctx do
      %{job_id: job_id} = ctx
      %{producer: producer} = start_member(ctx, :a, ack: false)
      {:ok, consumer} = ManualConsumer.start_link(producer, tag: :a)
      ManualConsumer.ask(consumer, 1_000)
      messages = received_all(:a, @batches)

      {[failing], successful} = Enum.split_with(messages, &(&1.metadata.slice == 5 and &1.metadata.batch_index == 1))

      log =
        capture_log(fn ->
          Broadway.Acknowledger.ack_messages(successful, [Message.failed(failing, "boom")])
          assert_receive {:telemetry, @stop, %{failed_batches: 1, rows: @rows}, %{job_id: ^job_id}}, 5_000
        end)

      assert log =~ "Not checkpointing slice 5 of 32"
      assert log =~ "The next run of job #{inspect(job_id)} reads it again"
      assert ctx.store |> Checkpoint.slices(job_id) |> Map.keys() |> Enum.sort() == Enum.to_list(0..31) -- [5]

      stop_supervised!(:a)
      %{consumer: consumer} = start_member(ctx, :b)
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, @stop, %{failed_batches: 0, rows: @rows, batches: @batches}, _metadata}, 5_000
      assert :b |> received() |> Enum.map(& &1.metadata.slice) |> Enum.uniq() == [5]
      assert ctx.store |> Checkpoint.slices(job_id) |> map_size() == @slices
    end

    test "fails when a finished slice cannot be recorded, which then does not count as finished", ctx do
      error = %Error{reason: :unavailable, message: "GCS responded 503"}
      Checkpoint.fail(ctx.store, :complete, error)

      %{consumer: consumer} = start_member(ctx, :a)
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, @exception, _measurements, %{reason: ^error}}, 5_000
      refute_receive {:telemetry, @stop, _measurements, _metadata}, 200
      assert Checkpoint.slices(ctx.store, ctx.job_id) == %{}
    end

    test "is shared by the producers of its job, which record every slice", ctx do
      %{job_id: job_id} = ctx
      members = for id <- [:a, :b], do: start_member(ctx, id, demand: 0)
      Enum.each(members, &ManualConsumer.ask(&1.consumer, 1_000))

      for _member <- members do
        assert_receive {:telemetry, @stop, measurements, %{job_id: ^job_id}}, 5_000
        assert %{rows: @rows, batches: @batches, failed_batches: 0} = measurements
      end

      a = received(:a)
      b = received(:b)
      assert a != [] and b != [], "both producers should have read slices"
      assert ctx.store |> Checkpoint.slices(job_id) |> map_size() == @slices
    end

    test "is taken over by the other producers of its job when one cannot record its slices", ctx do
      %{job_id: job_id} = ctx
      {:ok, failing} = Checkpoint.start_link()
      Checkpoint.fail(failing, :complete, %Error{reason: :unavailable, message: "GCS responded 503"})

      a = start_member(ctx, :a, demand: 0)
      b = start_member(%{ctx | store: failing}, :b, demand: 0)
      ManualConsumer.ask(a.consumer, 1_000)
      ManualConsumer.ask(b.consumer, 1_000)

      assert_receive {:telemetry, @exception, _measurements, %{reason: %Error{reason: :unavailable}}}, 5_000
      assert_receive {:telemetry, @stop, %{rows: @rows, failed_batches: 0}, %{job_id: ^job_id}}, 5_000
      assert ctx.store |> Checkpoint.slices(job_id) |> map_size() == @slices
    end

    test "of a snapshot that does not exist yet completes without a job", ctx do
      start_member(ctx, :a, script: [files: @files, history: []], snapshot_id: {:as_of, ~U[2020-01-01 00:00:00Z]})

      assert_receive {:telemetry, @stop, %{rows: 0, batches: 0}, %{snapshot_id: nil}}, 5_000
      refute_received {:checkpoint, {:load, _job}}
    end

    test "reports its job's id in its scan events", ctx do
      %{job_id: job_id} = ctx
      %{consumer: consumer} = start_member(ctx, :a)
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _measurements, %{job_id: ^job_id}}
      assert_receive {:telemetry, @stop, _measurements, %{job_id: ^job_id}}, 5_000
      assert_received {:checkpoint, {:load, %{id: ^job_id, table: [_table], snapshot_id: 42, slices: @slices}}}
    end
  end

  defp prepare(producer_opts, concurrency \\ 1) do
    Producer.prepare_for_start(__MODULE__, producer: [module: {Producer, producer_opts}, concurrency: concurrency])
  end

  defp distributed(opts) do
    Keyword.merge([connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: true], opts)
  end

  defp checkpoint(job_id, store), do: [job_id: job_id, backend: {Checkpoint, store: store}]

  # A producer of the job, on a fake database of its own, and a consumer that forwards
  # messages to the test as `{:messages, id, messages}`, acknowledging them unless
  # `ack: false`, which only starts the producer.
  defp start_member(ctx, id, opts \\ []) do
    pool =
      start_supervised!({FakeConnection, test: self(), script: Keyword.get(opts, :script, files: @files)},
        id: {:pool, id}
      )

    assert_receive {:fake, {:connect}}

    producer_opts = [
      connection: pool,
      table: ctx.table,
      snapshot_id: Keyword.get(opts, :snapshot_id, 42),
      fetch_size: 5,
      distribute: [slice_rows: 10],
      checkpoint: checkpoint(ctx.job_id, ctx.store)
    ]

    producer = start_supervised!(Supervisor.child_spec({Producer, producer_opts}, id: id, restart: :temporary))

    if Keyword.get(opts, :ack, true) do
      {:ok, consumer} = ManualConsumer.start_link(producer, tag: id, ack: true)
      %{producer: producer, consumer: consumer}
    else
      %{producer: producer}
    end
  end

  defp receive_write(job_id) do
    assert_receive {:checkpoint, {:complete, ^job_id, slice, result}}, 5_000
    {slice, result}
  end

  defp received(id, messages \\ []) do
    receive do
      {:messages, ^id, more} -> received(id, messages ++ more)
    after
      0 -> messages
    end
  end

  defp received_all(id, count, messages \\ [])
  defp received_all(_id, count, messages) when length(messages) >= count, do: messages

  defp received_all(id, count, messages) do
    assert_receive {:messages, ^id, more}, 5_000
    received_all(id, count, messages ++ more)
  end

  defp flush(messages \\ []) do
    receive do
      message -> flush([message | messages])
    after
      0 -> Enum.reverse(messages)
    end
  end
end
