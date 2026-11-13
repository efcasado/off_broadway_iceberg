defmodule OffBroadway.Iceberg.Integration.ProducerTest do
  use ExUnit.Case, async: false

  alias OffBroadway.Iceberg.Test.Iceberg
  alias OffBroadway.Iceberg.Test.Pipeline
  alias OffBroadway.Iceberg.Test.Telemetry

  @moduletag :integration
  @moduletag :capture_log
  @moduletag timeout: 180_000

  @pool OffBroadway.Iceberg.Integration.Pool
  @port 19_494
  # DuckDB sends results in vectors of 2,048 rows, at least one per round trip. With rows of
  # about 1 KB and transfer batches smaller than a vector, every round trip after the first
  # carries exactly one vector.
  @target_batch_bytes 65_536
  @vector_rows 2_048

  setup_all do
    :ok = Iceberg.start_catalog!()
    on_exit(fn -> Iceberg.stop_catalog!() end)

    settings = [quack_target_batch_bytes: @target_batch_bytes]
    start_supervised!(Iceberg.duckdb_spec(@pool, @port, global_settings: settings))
    # A restarted pipeline attaches again, so attaching must be idempotent.
    Iceberg.attach_catalog!(@pool)
    Iceberg.attach_catalog!(@pool)

    %{schema: Iceberg.create_schema!(@pool)}
  end

  describe "snapshots" do
    test "an explicit snapshot reads the rows committed up to it", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      first = Iceberg.insert!(@pool, table, 0..999)
      second = Iceberg.insert!(@pool, table, 1_000..1_999)

      {messages, %{rows: 1_000}, %{snapshot_id: ^first}} = scan(table: table, snapshot_id: first, fetch_size: 100)
      assert ids(messages) == Enum.to_list(0..999)
      assert Enum.all?(messages, &(&1.metadata.snapshot_id == first))

      {messages, %{rows: 2_000}, %{snapshot_id: ^second}} = scan(table: table, snapshot_id: second, fetch_size: 100)
      assert ids(messages) == Enum.to_list(0..1_999)
    end

    test ":current reads the snapshot of the main branch, which a rollback moves back", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      first = Iceberg.insert!(@pool, table, 0..99)
      _second = Iceberg.insert!(@pool, table, 100..199)
      QuackDB.query!(@pool, "SELECT * FROM iceberg_rollback_to_snapshot(?, ?)", [table, first])

      {messages, _measurements, %{snapshot_id: ^first}} = scan(table: table)

      assert ids(messages) == Enum.to_list(0..99)
      assert Enum.all?(messages, &(&1.metadata.snapshot_id == first))
    end

    test "a snapshot whose rows were all deleted yields no messages", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      inserted = Iceberg.insert!(@pool, table, 0..999)
      QuackDB.query!(@pool, "DELETE FROM #{table}")
      [_inserted, deleted] = Iceberg.snapshots(@pool, table)

      assert {[], %{batches: 0, rows: 0}, %{snapshot_id: ^deleted}} = scan(table: table)

      {messages, _measurements, _metadata} = scan(table: table, snapshot_id: inserted)
      assert ids(messages) == Enum.to_list(0..999)
    end

    test "a table without snapshots yields no messages", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)

      assert {[], %{batches: 0, rows: 0}, %{snapshot_id: nil}} = scan(table: table)
    end

    test "commits made while a scan runs are not read by it", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      snapshot = Iceberg.insert!(@pool, table, 0..19_999)
      name = unique_name()
      Telemetry.attach_pipeline(name)

      start_pipeline(name, [table: table, fetch_size: 100], handler: blocking_handler(self()))

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{name: ^name, snapshot_id: ^snapshot}},
                     30_000

      assert_receive {:blocked, processor}, 30_000

      later = Iceberg.insert!(@pool, table, 20_000..20_999)
      assert later != snapshot
      assert %{rows: [[21_000]]} = QuackDB.query!(@pool, "SELECT count(*) FROM #{table}")

      send(processor, :continue)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: 20_000, batches: batches},
                      %{name: ^name}},
                     60_000

      assert batches |> collect_handled() |> ids() == Enum.to_list(0..19_999)
    end
  end

  describe "batches" do
    test "every row of a large snapshot is delivered exactly once", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      Iceberg.insert!(@pool, table, 0..99_999)

      {messages, measurements, _metadata} =
        scan([table: table, fetch_size: 1_000], processor_concurrency: 4, max_demand: 2)

      assert %{rows: 100_000, failed_batches: 0} = measurements
      assert ids(messages) == Enum.to_list(0..99_999)
      assert Enum.all?(messages, &(&1.data.num_rows in 1..1_000))
      assert messages |> Enum.map(& &1.metadata.batch_index) |> Enum.sort() == Enum.to_list(0..(length(messages) - 1))
    end
  end

  describe "backpressure" do
    test "nothing is fetched from DuckDB while the pipeline has no demand", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      Iceberg.insert!(@pool, table, 0..(10 * @vector_rows - 1), 1_000)
      fetches = count_fetches()
      Telemetry.attach([[:quackdb, :fetch, :start]])
      name = unique_name()
      Telemetry.attach_pipeline(name)
      test = self()

      handler = fn message ->
        send(test, {:fetched_before, message.metadata.batch_index, :counters.get(fetches, 1)})
        blocking_handler(test).(message)
      end

      # One message per vector: message 0 comes with the query, message n needs fetch n.
      start_pipeline(name, [table: table, fetch_size: @vector_rows], handler: handler)

      # The only processor is busy with the first message and asks for nothing more, so
      # the scan must not fetch.
      assert_receive {:blocked, processor}, 30_000
      refute_receive {:telemetry, [:quackdb, :fetch, :start], _measurements, _metadata}, 500

      send(processor, :continue)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{batches: 10, rows: 20_480}, %{name: ^name}},
                     60_000

      # While message n is handled, the processor has not asked for message n + 1 yet, so
      # the scan must not have fetched it.
      for _batch <- 1..10 do
        assert_receive {:fetched_before, index, count}
        assert count <= index, "#{count} fetches before message #{index} was handled"
      end

      assert :counters.get(fetches, 1) >= 9
    end

    test "memory stays bounded while a large snapshot is read", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      # 200,000 distinct payloads of 512 bytes: over 100 MB of strings.
      Iceberg.insert!(@pool, table, 0..199_999, 512)
      set_target_batch_bytes(1_048_576)
      on_exit(fn -> set_target_batch_bytes(@target_batch_bytes) end)

      name = unique_name()
      test = self()
      :erlang.garbage_collect()
      baseline = :erlang.memory(:total)

      handler = fn message ->
        [producer] = Enum.map(Broadway.producer_names(name), &Process.whereis/1)
        {:memory, producer_memory} = Process.info(producer, :memory)

        # The worker is gone once the snapshot has been read to the end.
        {:memory, scan_memory} =
          case :sys.get_state(producer).state.module_state.scan do
            %{pid: scan} -> Process.info(scan, :memory) || {:memory, 0}
            nil -> {:memory, 0}
          end

        send(test, {:memory, :erlang.memory(:total) - baseline, producer_memory, scan_memory})
        message
      end

      {_messages, %{rows: 200_000, batches: batches}, _metadata} =
        scan([table: table, fetch_size: 1_000], name: name, handler: handler, processor_concurrency: 2, collect: false)

      samples = for _batch <- 1..batches, do: assert_receive({:memory, _total, _producer, _scan})
      max = fn position -> samples |> Enum.map(&elem(&1, position)) |> Enum.max() end

      # Locally, BEAM memory grows by about 18 MB, and the producer and scan stay under
      # 2 MB each. Holding the snapshot would take well over 100 MB.
      assert max.(1) < 64 * 1_048_576, "BEAM memory grew by #{max.(1)} bytes"
      assert max.(2) < 4 * 1_048_576, "the producer reached #{max.(2)} bytes"
      assert max.(3) < 4 * 1_048_576, "the scan reached #{max.(3)} bytes"
    end
  end

  describe "failures" do
    test "a snapshot that does not exist fails the scan once", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      Iceberg.insert!(@pool, table, 0..9)
      name = unique_name()
      Telemetry.attach_pipeline(name)

      start_pipeline(name, table: table, snapshot_id: 123)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{name: ^name, snapshot_id: 123}}, 30_000

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], %{batches: 0}, %{name: ^name} = metadata},
                     30_000

      assert %QuackDB.Error{message: message} = metadata.reason
      assert message =~ "Could not find snapshot with id 123"
      refute_receive {:telemetry, _event, _measurements, %{name: ^name}}, 500
      refute_received {:handled, _message, _processor}
    end

    test "a table that does not exist fails the scan", %{schema: schema} do
      name = unique_name()
      Telemetry.attach_pipeline(name)

      start_pipeline(name, table: "#{schema}.missing")

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], _, %{name: ^name} = metadata}, 30_000
      assert %QuackDB.Error{message: message} = metadata.reason
      assert message =~ "does not exist"
    end

    test "losing the DuckDB server mid-scan fails the scan without replaying it", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      Iceberg.insert!(@pool, table, 0..(10 * @vector_rows - 1), 1_000)

      duckdb =
        start_supervised!(
          Iceberg.duckdb_spec(:doomed_pool, @port + 1,
            pool_size: 1,
            global_settings: [quack_target_batch_bytes: @target_batch_bytes]
          )
        )

      Iceberg.attach_catalog!(:doomed_pool)
      name = unique_name()
      Telemetry.attach_pipeline(name)

      start_pipeline(name, [connection: :doomed_pool, table: table, fetch_size: @vector_rows],
        handler: blocking_handler(self())
      )

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{name: ^name}}, 30_000
      assert_receive {:blocked, processor}, 30_000

      {server, _pid, _type, _modules} =
        duckdb |> Supervisor.which_children() |> Enum.find(&match?({_id, _pid, _type, [QuackDB.Server]}, &1))

      :ok = Supervisor.terminate_child(duckdb, server)
      send(processor, :continue)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], %{batches: 1}, %{name: ^name}}, 60_000
      assert_receive {:handled, %{metadata: %{batch_index: 0}}, _processor}
      refute_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{name: ^name}}, 1_000
      refute_received {:handled, _message, _processor}
    end
  end

  describe "shutdown" do
    test "stopping the pipeline mid-scan stops the scan and frees its connection", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      Iceberg.insert!(@pool, table, 0..9_999)
      single = start_supervised!({QuackDB, uri: Iceberg.uri(@port), token: Iceberg.token(), pool_size: 1})
      name = unique_name()
      Telemetry.attach_pipeline(name)

      start_pipeline(name, [connection: single, table: table, fetch_size: 100], handler: blocking_handler(self()))
      assert_receive {:blocked, processor}, 30_000

      [producer] = Broadway.producer_names(name)
      scan = :sys.get_state(producer).state.module_state.scan.pid
      ref = Process.monitor(scan)

      stopping = Task.async(fn -> Broadway.stop(name) end)
      assert_receive {:DOWN, ^ref, :process, ^scan, :shutdown}, 5_000
      send(processor, :continue)
      Task.await(stopping)

      # The pool has one connection, which the scan held.
      assert %{rows: [[42]]} = QuackDB.query!(single, "SELECT 42")
      refute_received {:telemetry, [:off_broadway_iceberg, :scan, :stop], _measurements, _metadata}
    end
  end

  describe "distributed scans" do
    test "producer stages share one scan, sliced by data file, with deletes applied", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      # 16 inserts make 16 data files of 1,250 rows, and the delete adds delete files.
      for file <- 0..15, do: Iceberg.insert!(@pool, table, (file * 1_250)..(file * 1_250 + 1_249))
      QuackDB.query!(@pool, "DELETE FROM #{table} WHERE id % 7 = 0")
      snapshot = @pool |> Iceberg.snapshots(table) |> List.last()
      expected_ids = Enum.reject(0..19_999, &(rem(&1, 7) == 0))
      name = unique_name()
      Telemetry.attach_pipeline(name)

      # Slices of at least 2,500 rows hold two files each.
      start_pipeline(name, [table: table, snapshot_id: snapshot, fetch_size: 500, distribute: [slice_rows: 2_500]],
        producer_concurrency: 2,
        processor_concurrency: 4,
        max_demand: 2
      )

      # Every producer stage reports the whole scan.
      for _stage <- 1..2 do
        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, %{name: ^name}}, 60_000
        assert measurements == Map.merge(measurements, %{rows: length(expected_ids), failed_batches: 0})
      end

      messages = drain_handled()
      assert messages |> ids() |> Enum.uniq() == expected_ids
      assert messages |> Enum.map(& &1.metadata.slice) |> Enum.uniq() |> Enum.sort() == Enum.to_list(0..7)

      # Slice i holds the rows of the data files 2 * i and 2 * i + 1, in path order.
      %{rows: files} =
        QuackDB.query!(@pool, "SELECT file_path FROM iceberg_files(?) WHERE content = 0 ORDER BY file_path", [table])

      slice_of_file = files |> List.flatten() |> Enum.with_index() |> Map.new(fn {path, i} -> {path, div(i, 2)} end)
      %{rows: rows} = QuackDB.query!(@pool, "SELECT id, filename FROM #{table}")
      slice_of_id = Map.new(rows, fn [id, path] -> {id, slice_of_file[path]} end)

      for message <- messages, id <- QuackDB.Columns.get(message.data, "id") do
        assert slice_of_id[id] == message.metadata.slice
      end
    end

    test "{:as_of, datetime} reads the snapshot that was current then", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      first = Iceberg.insert!(@pool, table, 0..99)

      %{rows: [[made_current_at]]} =
        QuackDB.query!(@pool, "SELECT made_current_at FROM iceberg_history(?) WHERE snapshot_id = ?", [table, first])

      as_of = DateTime.from_naive!(made_current_at, "Etc/UTC")
      _second = Iceberg.insert!(@pool, table, 100..199)

      {messages, _measurements, %{snapshot_id: ^first}} = scan(table: table, snapshot_id: {:as_of, as_of})
      assert ids(messages) == Enum.to_list(0..99)

      before_first = DateTime.shift(as_of, second: -1)
      assert {[], %{rows: 0}, %{snapshot_id: nil}} = scan(table: table, snapshot_id: {:as_of, before_first})
    end
  end

  describe "multiple pipelines" do
    test "scan the same table at the same time without interfering", %{schema: schema} do
      table = Iceberg.create_table!(@pool, schema)
      first = Iceberg.insert!(@pool, table, 0..4_999)
      second = Iceberg.insert!(@pool, table, 5_000..9_999)
      names = for _pipeline <- 1..2, do: unique_name()
      Enum.each(names, &Telemetry.attach_pipeline/1)

      for {name, snapshot_id} <- Enum.zip(names, [first, second]) do
        start_pipeline(name, table: table, snapshot_id: snapshot_id, fetch_size: 500)
      end

      batches =
        for name <- names do
          assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{batches: batches}, %{name: ^name}},
                         60_000

          batches
        end

      messages = collect_handled(Enum.sum(batches))
      by_snapshot = Enum.group_by(messages, & &1.metadata.snapshot_id)

      assert ids(by_snapshot[first]) == Enum.to_list(0..4_999)
      assert ids(by_snapshot[second]) == Enum.to_list(0..9_999)
    end
  end

  # Runs a whole scan in a new pipeline and returns its messages, with the measurements
  # and metadata of the scan's :stop event.
  defp scan(producer_opts, opts \\ []) do
    name = Keyword.get(opts, :name, unique_name())
    Telemetry.attach_pipeline(name)
    start_pipeline(name, producer_opts, opts)

    assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, %{name: ^name} = metadata}, 120_000

    messages = if Keyword.get(opts, :collect, true), do: collect_handled(measurements.batches), else: []
    {messages, measurements, metadata}
  end

  defp start_pipeline(name, producer_opts, opts \\ []) do
    pipeline_opts =
      opts
      |> Keyword.take([:handler, :producer_concurrency, :processor_concurrency, :max_demand])
      |> Keyword.merge(name: name, test: self(), producer: Keyword.merge([connection: @pool], producer_opts))

    start_supervised!({Pipeline, pipeline_opts}, id: name, restart: :temporary)
  end

  # Holds the first batch until the test sends :continue to the processor.
  defp blocking_handler(test) do
    fn message ->
      if message.metadata.batch_index == 0 do
        send(test, {:blocked, self()})

        receive do
          :continue -> :ok
        end
      end

      send(test, {:handled, message, self()})
      message
    end
  end

  # Every handled message received so far, once nothing more arrives for a moment.
  defp drain_handled(messages \\ []) do
    receive do
      {:handled, message, _processor} -> drain_handled([message | messages])
    after
      200 -> Enum.reverse(messages)
    end
  end

  defp collect_handled(count) do
    for _batch <- 1..count//1 do
      assert_receive {:handled, message, _processor}, 30_000
      message
    end
  end

  defp count_fetches do
    fetches = :counters.new(1, [])
    id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach(id, [:quackdb, :fetch, :start], &__MODULE__.count_fetch/4, fetches)
    on_exit(fn -> :telemetry.detach(id) end)
    fetches
  end

  def count_fetch(_event, _measurements, _metadata, fetches), do: :counters.add(fetches, 1, 1)

  defp set_target_batch_bytes(bytes) do
    QuackDB.query!(@pool, "SET GLOBAL quack_target_batch_bytes = #{bytes}")
  end

  defp ids(messages), do: messages |> Enum.flat_map(&QuackDB.Columns.get(&1.data, "id")) |> Enum.sort()

  defp unique_name, do: :"pipeline_#{System.unique_integer([:positive])}"
end
