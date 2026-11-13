defmodule OffBroadway.Iceberg.CheckpointTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias OffBroadway.Iceberg.Checkpoint.Error
  alias OffBroadway.Iceberg.Checkpoint.GCS
  alias OffBroadway.Iceberg.Distribution
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
  # Each file is a slice of its own.
  @plan Distribution.fingerprint(for {path, _rows} <- @files, do: [path])

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
    test "rejects a filename column before recording a job", ctx do
      start_member(ctx, :a, script: [files: @files, columns: ["id", "filename"]], producer: [columns: ["id"]])

      assert_receive {:telemetry, @exception, %{batches: 0}, %{reason: %ArgumentError{message: message}}}
      assert message =~ "virtual filename column"
      refute_received {:checkpoint, {:load, _job}}
      refute_received {:checkpoint, {:complete, _job_id, _slice, _result}}
      refute_received {:fake, {:declare, _statement, _params, _max_rows}}
    end

    test "records each slice once all its batches succeeded, and only then counts it finished", ctx do
      %{job_id: job_id} = ctx
      Checkpoint.hold(ctx.store)

      %{consumer: consumer} = start_member(ctx, :a)
      ManualConsumer.ask(consumer, 1_000)

      assert {0, @result} = receive_write(job_id)
      refute_receive {:checkpoint, {:complete, _, _, _}}, 100
      assert Checkpoint.slices(ctx.store, job_id) == %{}
      refute_receive {:telemetry, @stop, _measurements, _metadata}, 200

      Checkpoint.release(ctx.store)

      written = for _slice <- 1..(@slices - 1), do: receive_write(job_id)
      assert Enum.sort(written) == for(slice <- 1..(@slices - 1), do: {slice, @result})

      assert_receive {:telemetry, @stop, measurements, %{job_id: ^job_id}}, 5_000
      assert %{rows: @rows, batches: @batches, failed_batches: 0} = measurements
      assert Checkpoint.slices(ctx.store, job_id) == Map.new(0..(@slices - 1), &{&1, @result})

      for _slice <- 1..@slices do
        assert_received {:telemetry, @write, %{batches: 2, rows: 10, duration: _}, %{slice: _, job_id: ^job_id}}
      end
    end

    test "waits for every acknowledgement before starting the next slice", ctx do
      %{producer: producer} = start_member(ctx, :a, ack: false)
      {:ok, consumer} = ManualConsumer.start_link(producer, tag: :a)
      ManualConsumer.ask(consumer, 1_000)

      [first, last] = received_all(:a, 2)
      assert first.metadata.slice == 0 and last.metadata.slice == 0
      refute_receive {:messages, :a, _messages}, 100

      Broadway.Acknowledger.ack_messages([first], [])
      refute_receive {:checkpoint, {:complete, _, _, _}}, 100
      refute_receive {:messages, :a, _messages}, 100

      Broadway.Acknowledger.ack_messages([last], [])
      assert {0, @result} = receive_write(ctx.job_id)
      assert [%{metadata: %{slice: 1}}, %{metadata: %{slice: 1}}] = received_all(:a, 2)
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

    test "fails when its job id was recorded with any other table, snapshot or plan", ctx do
      job = %{id: ctx.job_id, table: [ctx.table], snapshot_id: 42, plan: @plan, slices: @slices}

      # The same job resumes, so the others differ only in the field they change.
      Checkpoint.put_job(ctx.store, job)
      %{consumer: consumer} = start_member(ctx, :same)
      assert_receive {:checkpoint, {:load, ^job}}
      ManualConsumer.ask(consumer, 1_000)
      assert_receive {:telemetry, @stop, %{rows: @rows}, _metadata}, 5_000
      flush()

      for {field, value} <- [
            table: ["lake", ctx.table],
            snapshot_id: 41,
            plan: :crypto.hash(:sha256, "another plan"),
            slices: @slices + 1
          ] do
        {:ok, store} = Checkpoint.start_link()
        Checkpoint.put_job(store, Map.put(job, field, value))

        start_member(%{ctx | store: store}, field)

        assert_receive {:telemetry, @exception, _measurements, %{reason: %Error{reason: :incompatible}}}, 5_000
        refute_receive {:fake, {:declare, _statement, _params, _max_rows}}, 100
      end
    end

    test "resumes its job only with the columns and rows the job's first run selected", ctx do
      selection = [columns: ["id"], where: {"id >= ?", [10]}]
      plan = Distribution.fingerprint(for({path, _rows} <- @files, do: [path]), ["id"], "id >= 10")

      %{consumer: consumer} = start_member(ctx, :first, producer: selection)
      assert_receive {:checkpoint, {:load, %{plan: ^plan} = job}}
      ManualConsumer.ask(consumer, 1_000)
      assert_receive {:telemetry, @stop, %{rows: @rows}, _metadata}, 5_000
      flush()

      # The same selection, with its params written in, resumes the job, which has
      # nothing left to read.
      start_member(ctx, :same, producer: [columns: ["id"], where: "id >= 10"])
      assert_receive {:checkpoint, {:load, ^job}}
      assert_receive {:telemetry, @stop, %{rows: @rows}, _metadata}, 5_000
      refute_received {:fake, {:declare, _statement, _params, _max_rows}}

      for {id, other} <- [
            other_rows: [columns: ["id"], where: "id >= 11"],
            other_columns: [columns: ["id", "ts"], where: "id >= 10"],
            everything: []
          ] do
        start_member(ctx, id, producer: other)

        assert_receive {:telemetry, @exception, _measurements, %{reason: %Error{reason: :incompatible}}}, 5_000
        refute_receive {:fake, {:declare, _statement, _params, _max_rows}}, 100
      end
    end

    test "names its job's table by its parts, so both forms of a table's name resume it", ctx do
      Telemetry.attach([@stop, @exception], &(&1.table == [ctx.table]))
      Checkpoint.put_slices(ctx.store, ctx.job_id, Map.new(0..(@slices - 1), &{&1, @result}))

      start_member(ctx, :dotted)
      assert_receive {:checkpoint, {:load, %{table: [_table]} = job}}
      assert_receive {:telemetry, @stop, _measurements, _metadata}, 5_000

      start_member(%{ctx | table: [ctx.table]}, :list)
      assert_receive {:checkpoint, {:load, ^job}}
      assert_receive {:telemetry, @stop, _measurements, %{table: [_table]}}, 5_000
      refute_received {:telemetry, @exception, _measurements, _metadata}
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

      log =
        capture_log(fn ->
          for slice <- 0..(@slices - 1) do
            messages = received_all(:a, 2)
            assert Enum.all?(messages, &(&1.metadata.slice == slice))
            {failed, successful} = Enum.split_with(messages, &(&1.metadata.slice == 5 and &1.metadata.batch_index == 1))
            Broadway.Acknowledger.ack_messages(successful, Enum.map(failed, &Message.failed(&1, "boom")))
          end

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
      members = for id <- [:a, :b], do: start_member(ctx, id)
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

    test "is not shared with producers that record the same job elsewhere", ctx do
      %{job_id: job_id} = ctx
      {:ok, elsewhere} = Checkpoint.start_link()

      a = start_member(ctx, :a)
      b = start_member(%{ctx | store: elsewhere}, :b)
      Enum.each([a, b], &ManualConsumer.ask(&1.consumer, 1_000))

      for _member <- [a, b] do
        assert_receive {:telemetry, @stop, %{rows: @rows, batches: @batches, failed_batches: 0}, %{job_id: ^job_id}},
                       5_000
      end

      # Each read every slice, and recorded it where the other would not have.
      assert length(received(:a)) == @batches
      assert length(received(:b)) == @batches
      assert ctx.store |> Checkpoint.slices(job_id) |> map_size() == @slices
      assert elsewhere |> Checkpoint.slices(job_id) |> map_size() == @slices
    end

    test "is taken over by the other producers of its job when one cannot record its slices", ctx do
      %{job_id: job_id} = ctx
      error = %Error{reason: :unavailable, message: "GCS responded 503"}

      a = start_member(ctx, :a)
      b = start_member(ctx, :b, backend: [fail_complete: error])
      ManualConsumer.ask(a.consumer, 1_000)
      ManualConsumer.ask(b.consumer, 1_000)

      assert_receive {:telemetry, @exception, _measurements, %{reason: %Error{reason: :unavailable}}}, 5_000
      assert_receive {:telemetry, @stop, %{rows: @rows, failed_batches: 0}, %{job_id: ^job_id}}, 5_000
      assert ctx.store |> Checkpoint.slices(job_id) |> map_size() == @slices
    end

    test "goes on reading a slice whose other copy failed, and records it", ctx do
      %{job_id: job_id} = ctx
      %{producer: producer, consumer: consumer} = start_member(ctx, :a)
      ManualConsumer.ask(consumer, 1)
      assert_receive {:messages, :a, [%{metadata: %{slice: 0}}]}, 5_000

      # Another member's copy of slice 0 had a failed batch.
      group = :sys.get_state(producer).state.group
      send(producer, {Distribution, group, {:done, %{0 => {1, 2, 10}}}})
      assert %{scan: %{slice: 0}} = :sys.get_state(producer).state

      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, @write, _measurements, %{slice: 0}}, 5_000
      assert_receive {:telemetry, @stop, %{failed_batches: 0, rows: @rows, batches: @batches}, _metadata}, 5_000
      assert ctx.store |> Checkpoint.slices(job_id) |> map_size() == @slices
    end

    test "reports :stop only once its own writes have returned", ctx do
      Checkpoint.hold(ctx.store)
      %{producer: producer, consumer: consumer} = start_member(ctx, :a)
      # The two messages of slice 0, and the request that ends it.
      ManualConsumer.ask(consumer, 3)
      assert_receive {:checkpoint, {:complete, _job_id, 0, @result}}, 5_000

      # Another member finished and recorded every slice.
      group = :sys.get_state(producer).state.group
      send(producer, {Distribution, group, {:done, Map.new(0..(@slices - 1), &{&1, {0, 2, 10}})}})
      refute_receive {:telemetry, @stop, _measurements, _metadata}, 200

      Checkpoint.release(ctx.store)

      assert_receive {:telemetry, @write, _measurements, %{slice: 0}}, 5_000
      assert_receive {:telemetry, @stop, %{rows: @rows, failed_batches: 0}, _metadata}, 5_000
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

  defp checkpoint(job_id, store, backend_opts \\ []) do
    [job_id: job_id, backend: {Checkpoint, Keyword.put(backend_opts, :store, store)}]
  end

  # A producer of the job, on a fake database of its own, and a consumer that forwards
  # messages to the test as `{:messages, id, messages}`, acknowledging them unless
  # `ack: false`, which only starts the producer.
  defp start_member(ctx, id, opts \\ []) do
    pool =
      start_supervised!({FakeConnection, test: self(), script: Keyword.get(opts, :script, files: @files)},
        id: {:pool, id}
      )

    assert_receive {:fake, {:connect}}

    producer_opts =
      Keyword.merge(
        [
          connection: pool,
          table: ctx.table,
          snapshot_id: Keyword.get(opts, :snapshot_id, 42),
          fetch_size: 5,
          distribute: [slice_rows: 10],
          checkpoint: checkpoint(ctx.job_id, ctx.store, Keyword.get(opts, :backend, []))
        ],
        Keyword.get(opts, :producer, [])
      )

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
