defmodule OffBroadway.Iceberg.ProducerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias OffBroadway.Iceberg.Producer
  alias OffBroadway.Iceberg.Test.FakeConnection
  alias OffBroadway.Iceberg.Test.ManualConsumer
  alias OffBroadway.Iceberg.Test.Pipeline
  alias OffBroadway.Iceberg.Test.Telemetry

  # DBConnection logs every disconnect the tests cause.
  @moduletag :capture_log

  @scan_statement_suffix " AT (VERSION => ?)"

  describe "options" do
    test "accept tables, snapshots and connections in every supported form" do
      for table <- ["events", "betting.events", "lake.betting.events", ["lake", "odd \"name\"", "t.x"], ["t"]],
          snapshot_id <- [:current, 0, 0x7FFFFFFFFFFFFFFF],
          connection <- [MyApp.QuackDB, self(), {:global, :pool}, {:via, Registry, {MyApp.Registry, :pool}}] do
        assert {[], _opts} = prepare(connection: connection, table: table, snapshot_id: snapshot_id)
      end
    end

    test "require a connection and a table" do
      assert_raise NimbleOptions.ValidationError, ~r/required :connection option not found/, fn ->
        prepare(table: "events")
      end

      assert_raise NimbleOptions.ValidationError, ~r/required :table option not found/, fn ->
        prepare(connection: MyApp.QuackDB)
      end
    end

    test "reject tables that are not plain or explicitly split names" do
      for table <- [
            "",
            "lake..events",
            ".events",
            "lake.betting.events.extra",
            "1events",
            "events; DROP TABLE events",
            ~s("lake"."events"),
            "lake.betting events",
            [],
            ["lake", ""],
            ["a", "b", "c", "d"],
            ["lake", <<0>>],
            [:lake, :events],
            :events,
            42
          ] do
        assert_raise NimbleOptions.ValidationError, ~r/:table/, fn ->
          prepare(connection: MyApp.QuackDB, table: table)
        end
      end
    end

    test "reject snapshot ids that are not :current or a 64-bit snapshot id" do
      for snapshot_id <- [-1, 0x8000000000000000, "42", 4.2, :latest, nil] do
        assert_raise NimbleOptions.ValidationError, ~r/:snapshot_id/, fn ->
          prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: snapshot_id)
        end
      end
    end

    test "reject fetch sizes that are not positive integers" do
      for fetch_size <- [0, -1, "10", 1.5] do
        assert_raise NimbleOptions.ValidationError, ~r/:fetch_size/, fn ->
          prepare(connection: MyApp.QuackDB, table: "events", fetch_size: fetch_size)
        end
      end
    end

    test "reject connections that are not a pool pid or name" do
      for connection <- [nil, true, "pool", 42, {:global}, {:via, "Registry", :pool}] do
        assert_raise NimbleOptions.ValidationError, ~r/:connection/, fn ->
          prepare(connection: connection, table: "events")
        end
      end
    end

    test "reject unknown options" do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options \[:batch_size\]/, fn ->
        prepare(connection: MyApp.QuackDB, table: "events", batch_size: 10)
      end
    end

    test "reject producer concurrency other than 1" do
      assert_raise ArgumentError, ~r/requires producer concurrency 1, got: 2/, fn ->
        prepare([connection: MyApp.QuackDB, table: "events"], 2)
      end
    end

    test "make Broadway fail to start a pipeline with invalid options" do
      Process.flag(:trap_exit, true)

      assert {:error, {%NimbleOptions.ValidationError{}, _stacktrace}} =
               Pipeline.start_link(
                 name: unique_name(),
                 test: self(),
                 producer: [connection: MyApp.QuackDB, table: "1events"]
               )
    end
  end

  describe "queries" do
    test "pin the snapshot of the main branch for :current, then scan it" do
      pool = start_pool(rows: rows(3))
      %{consumer: consumer} = start_producer(pool, table: "lake.betting.events")

      quoted = ~s("lake"."betting"."events")
      assert_receive {:fake, {:query, statement, []}}
      assert statement == "SELECT * FROM #{quoted} LIMIT 0"
      assert_receive {:fake, {:query, "SELECT snapshot_id FROM iceberg_refs(?) WHERE name = 'main'", [^quoted]}}

      ManualConsumer.ask(consumer, 1)

      assert_receive {:fake, {:declare, statement, [42], 10_000}}
      assert statement == "SELECT * FROM #{quoted}" <> @scan_statement_suffix
    end

    test "scan an explicit snapshot without looking up the current one" do
      pool = start_pool(rows: rows(3))
      %{consumer: consumer} = start_producer(pool, table: ["lake", ~s(odd "name"), "t.x"], snapshot_id: 7)

      ManualConsumer.ask(consumer, 1)

      assert_receive {:fake, {:declare, statement, [7], 10_000}}
      assert statement == ~s(SELECT * FROM "lake"."odd ""name"""."t.x") <> @scan_statement_suffix
      refute_received {:fake, {:query, _statement, _params}}
    end
  end

  describe "demand" do
    test "nothing is fetched before Broadway asks for messages" do
      pool = start_pool(rows: rows(30))
      %{table: table} = start_producer(pool, fetch_size: 10)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _measurements, %{table: ^table}}
      refute_receive {:fake, {:declare, _statement, _params, _max_rows}}, 100
    end

    test "each unit of demand fetches one batch from the same cursor" do
      pool = start_pool(rows: rows(50))
      %{consumer: consumer} = start_producer(pool, fetch_size: 10)

      ManualConsumer.ask(consumer, 1)
      assert [%Message{}] = receive_messages(1)
      assert_receive {:fake, {:fetch, 1, 10}}
      refute_receive {:fake, {:fetch, 2, _row_count}}, 100

      ManualConsumer.ask(consumer, 1)
      ManualConsumer.ask(consumer, 1)
      assert [_second, _third] = receive_messages(2)
      assert_receive {:fake, {:fetch, 2, 10}}
      assert_receive {:fake, {:fetch, 3, 10}}
      refute_receive {:fake, {:fetch, 4, _row_count}}, 100

      assert_received {:fake, {:declare, _statement, _params, _max_rows}}
      refute_received {:fake, {:declare, _statement, _params, _max_rows}}
    end

    test "demand from before the snapshot is resolved is kept" do
      pool = start_pool(rows: rows(20))
      %{consumer: consumer} = start_producer(pool, fetch_size: 10)

      ManualConsumer.ask(consumer, 5)

      assert [first, second] = receive_messages(2)
      assert {first.metadata.batch_index, second.metadata.batch_index} == {0, 1}
    end
  end

  describe "batches" do
    test "hold at most :fetch_size rows, in snapshot order" do
      pool = start_pool(rows: rows(25))
      %{consumer: consumer, table: table} = start_producer(pool, fetch_size: 10)

      ManualConsumer.ask(consumer, 10)
      messages = receive_messages(3)

      assert Enum.map(messages, & &1.data.num_rows) == [10, 10, 5]
      assert Enum.flat_map(messages, &QuackDB.Columns.get(&1.data, "id")) == Enum.to_list(0..24)

      assert Enum.map(messages, & &1.metadata) == [
               %{table: table, snapshot_id: 42, batch_index: 0},
               %{table: table, snapshot_id: 42, batch_index: 1},
               %{table: table, snapshot_id: 42, batch_index: 2}
             ]

      assert %QuackDB.Columns{names: ["id"]} = hd(messages).data
    end

    test "fit a snapshot smaller than :fetch_size into one message" do
      pool = start_pool(rows: rows(25))
      %{consumer: consumer} = start_producer(pool, fetch_size: 100)

      ManualConsumer.ask(consumer, 10)

      assert [%Message{data: %QuackDB.Columns{num_rows: 25}}] = receive_messages(1)
      assert_receive {:fake, {:declare, _statement, _params, 100}}
    end

    test "are not emitted for an empty snapshot" do
      pool = start_pool(rows: [])
      %{consumer: consumer, table: table} = start_producer(pool)

      ManualConsumer.ask(consumer, 10)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{batches: 0, rows: 0}, %{table: ^table}}
      refute_received {:messages, _messages}
    end

    test "are not fetched for a table without snapshots" do
      pool = start_pool(refs: [])
      %{consumer: consumer, table: table} = start_producer(pool)

      ManualConsumer.ask(consumer, 10)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{table: ^table, snapshot_id: nil}}
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{batches: 0}, %{table: ^table}}
      refute_received {:fake, {:declare, _statement, _params, _max_rows}}
    end
  end

  describe "completion" do
    test "waits for every batch to be acknowledged" do
      pool = start_pool(rows: rows(20))
      %{consumer: consumer, table: table} = start_producer(pool, fetch_size: 10)

      ManualConsumer.ask(consumer, 5)
      [first, second] = receive_messages(2)
      assert_receive {:fake, {:deallocate}}

      ack([first])
      refute_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], _, _}, 100

      ack([second])

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, metadata}
      assert %{batches: 2, rows: 20, failed_batches: 0, duration: duration} = measurements
      assert is_integer(duration) and duration > 0
      assert metadata == %{name: nil, table: table, snapshot_id: 42}
    end

    test "releases the connection and never scans the snapshot again" do
      pool = start_pool(rows: rows(10))
      %{producer: producer, consumer: consumer, table: table} = start_producer(pool, fetch_size: 10)

      ManualConsumer.ask(consumer, 5)
      ack(receive_messages(1))
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], _, %{table: ^table}}

      assert :ok = DBConnection.run(pool, fn _conn -> :ok end, timeout: 1_000)
      flush_fake_events()

      ManualConsumer.ask(consumer, 5)
      refute_receive {:fake, _event}, 100
      refute_received {:messages, _messages}
      assert Process.alive?(producer)
    end
  end

  describe "acknowledgements" do
    test "count and log failed batches without retrying them" do
      pool = start_pool(rows: rows(20))
      %{consumer: consumer, table: table} = start_producer(pool, fetch_size: 10)

      ManualConsumer.ask(consumer, 5)
      [first, second] = receive_messages(2)

      log =
        capture_log(fn ->
          ack([first], [Message.failed(second, "boom")])
          assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{failed_batches: 1, batches: 2}, _}
        end)

      assert log =~ "1 batches of #{inspect(table)} at snapshot 42 failed and will not be retried: [1]"
      refute_received {:messages, _messages}
    end
  end

  describe "failures" do
    test "a missing table fails the scan before resolving a snapshot" do
      error = %QuackDB.Error{message: "Catalog Error: Table with name events does not exist!"}
      pool = start_pool(fail: {:bind, error})

      log =
        capture_log(fn ->
          %{table: table} = start_producer(pool)

          assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], %{batches: 0}, metadata}
          assert %{table: ^table, snapshot_id: nil, kind: :error, reason: ^error} = metadata
        end)

      assert log =~ "does not exist"
      refute_received {:fake, {:query, "SELECT snapshot_id" <> _rest, _params}}
    end

    test "a query error fails the scan and is not retried" do
      error = %QuackDB.Error{message: "Invalid Configuration Error: Could not find snapshot with id 7"}
      pool = start_pool(fail: {:declare, error})

      capture_log(fn ->
        %{producer: producer, consumer: consumer} = start_producer(pool, snapshot_id: 7)
        ManualConsumer.ask(consumer, 5)

        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], _, %{snapshot_id: 7, reason: ^error}}

        ManualConsumer.ask(consumer, 5)
        assert_received {:fake, {:declare, _statement, _params, _max_rows}}
        refute_receive {:fake, {:declare, _statement, _params, _max_rows}}, 100
        assert Process.alive?(producer)
      end)

      refute_received {:messages, _messages}
    end

    test "an error mid-scan keeps the batches already emitted and does not replay" do
      error = %QuackDB.Error{message: "IO Error: connection reset"}
      pool = start_pool(rows: rows(30), fail: {{:fetch, 2}, error})

      capture_log(fn ->
        %{consumer: consumer} = start_producer(pool, fetch_size: 10)
        ManualConsumer.ask(consumer, 5)

        assert [first] = receive_messages(1)

        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], %{batches: 1, rows: 10},
                        %{reason: ^error}}

        ack([first])
        refute_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], _, _}, 100
      end)

      refute_received {:messages, _messages}
      assert_received {:fake, {:declare, _statement, _params, _max_rows}}
      refute_received {:fake, {:declare, _statement, _params, _max_rows}}
    end

    test "a disconnect mid-scan fails the scan and the pool reconnects" do
      error = DBConnection.ConnectionError.exception("connection closed")
      pool = start_pool(rows: rows(30), fail: {{:fetch, 2}, {:disconnect, error}})

      capture_log(fn ->
        %{consumer: consumer} = start_producer(pool, fetch_size: 10)
        ManualConsumer.ask(consumer, 5)

        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], %{batches: 1}, %{reason: ^error}}
        assert_receive {:fake, {:disconnect, ^error}}
        assert_receive {:fake, {:connect}}
      end)
    end

    test "a pool that is not running fails the scan" do
      capture_log(fn ->
        %{table: table} = start_producer(:"no_such_pool_#{System.unique_integer([:positive])}")

        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], _, %{table: ^table, reason: _reason}}
      end)
    end
  end

  describe "cleanup" do
    test "shutting the producer down mid-scan stops the worker and disconnects its connection" do
      pool = start_pool(rows: rows(30))
      %{producer: producer, consumer: consumer} = start_producer(pool, fetch_size: 10)

      ManualConsumer.ask(consumer, 1)
      receive_messages(1)
      scan = :sys.get_state(producer).state.scan
      ref = Process.monitor(scan)

      stop_supervised!(Producer)

      assert_receive {:DOWN, ^ref, :process, ^scan, :shutdown}
      assert_receive {:fake, {:disconnect, %DBConnection.ConnectionError{}}}
      assert_receive {:fake, {:connect}}
      assert :ok = DBConnection.run(pool, fn _conn -> :ok end, timeout: 1_000)
    end

    test "stopping the producer normally mid-scan stops the worker too" do
      pool = start_pool(rows: rows(30))
      {:ok, producer} = GenStage.start(Producer, connection: pool, table: unique_table(), fetch_size: 10)
      {:ok, consumer} = ManualConsumer.start_link(producer)

      ManualConsumer.ask(consumer, 1)
      receive_messages(1)
      scan = :sys.get_state(producer).state.scan
      ref = Process.monitor(scan)

      GenStage.stop(producer)

      assert_receive {:DOWN, ^ref, :process, ^scan, :shutdown}
      assert_receive {:fake, {:disconnect, %DBConnection.ConnectionError{}}}
    end

    test "draining the pipeline cancels the scan" do
      pool = start_pool(rows: rows(30))
      name = unique_name()
      test = self()

      handler = fn message ->
        send(test, {:handling, self()})
        assert_receive :continue, 5_000
        message
      end

      # Broadway.stop/1 stops the pipeline normally, which the test supervisor would restart.
      start_supervised!(
        {Pipeline, name: name, handler: handler, producer: [connection: pool, table: "events", fetch_size: 10]},
        restart: :temporary
      )

      assert_receive {:handling, processor}
      [producer] = Broadway.producer_names(name)
      scan = :sys.get_state(producer).state.module_state.scan
      ref = Process.monitor(scan)

      stopping = Task.async(fn -> Broadway.stop(name) end)
      assert_receive {:DOWN, ^ref, :process, ^scan, :shutdown}
      send(processor, :continue)
      Task.await(stopping)

      assert_receive {:fake, {:disconnect, %DBConnection.ConnectionError{}}}
      refute_received {:fake, {:fetch, 2, _row_count}}
    end
  end

  describe "unexpected messages" do
    test "are logged and ignored" do
      pool = start_pool(rows: rows(10))
      %{producer: producer, consumer: consumer} = start_producer(pool, fetch_size: 10)

      log =
        capture_log(fn ->
          send(producer, :unexpected)
          ManualConsumer.ask(consumer, 1)
          assert [%Message{}] = receive_messages(1)
        end)

      assert log =~ "Producer received an unexpected message: :unexpected"
    end
  end

  describe "multiple producers" do
    test "scan independently on the same pool" do
      pool = start_pool(rows: rows(20), pool_size: 2)

      first = start_producer(pool, fetch_size: 10)
      second = start_producer(pool, fetch_size: 5, id: :second)

      ManualConsumer.ask(first.consumer, 10)
      ManualConsumer.ask(second.consumer, 10)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{table: table}} when table == first.table
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{table: table}} when table == second.table

      messages = receive_messages(6)
      {from_first, from_second} = Enum.split_with(messages, &(&1.metadata.table == first.table))

      assert from_first |> Enum.map(& &1.data.num_rows) |> Enum.sum() == 20
      assert from_second |> Enum.map(& &1.data.num_rows) |> Enum.sum() == 20
      assert length(from_first) == 2
      assert length(from_second) == 4
    end
  end

  describe "in a Broadway pipeline" do
    test "every batch is processed once and the scan completes" do
      pool = start_pool(rows: rows(95))
      name = unique_name()
      table = unique_table()
      Telemetry.attach_scan(table)

      start_supervised!(
        {Pipeline,
         name: name, test: self(), processor_concurrency: 3, producer: [connection: pool, table: table, fetch_size: 10]}
      )

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, %{name: ^name}}, 5_000
      assert %{batches: 10, rows: 95, failed_batches: 0} = measurements

      ids =
        for _batch <- 1..10 do
          assert_receive {:handled, message, _processor}
          QuackDB.Columns.get(message.data, "id")
        end

      assert ids |> List.flatten() |> Enum.sort() == Enum.to_list(0..94)
    end

    test "failed messages are counted when the scan completes" do
      pool = start_pool(rows: rows(30))
      table = unique_table()
      Telemetry.attach_scan(table)

      handler = fn
        %Message{metadata: %{batch_index: 1}} = message -> Message.failed(message, "boom")
        message -> message
      end

      producer = [connection: pool, table: table, fetch_size: 10]

      log =
        capture_log(fn ->
          start_supervised!({Pipeline, name: unique_name(), handler: handler, producer: producer})

          assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{failed_batches: 1, batches: 3}, _}, 5_000
        end)

      assert log =~ "failed and will not be retried: [1]"
    end
  end

  defp prepare(producer_opts, concurrency \\ 1) do
    Producer.prepare_for_start(Pipeline, producer: [module: {Producer, producer_opts}, concurrency: concurrency])
  end

  defp start_pool(script, opts \\ []) do
    opts = Keyword.merge([test: self(), script: script, pool_size: 1], opts)
    pool = start_supervised!({FakeConnection, opts})
    for _connection <- 1..opts[:pool_size], do: assert_receive({:fake, {:connect}})
    pool
  end

  defp start_producer(pool, opts \\ []) do
    {id, opts} = Keyword.pop(opts, :id, Producer)
    table = Keyword.get(opts, :table, unique_table())
    Telemetry.attach_scan(table)

    producer = start_supervised!({Producer, Keyword.merge([connection: pool, table: table], opts)}, id: id)
    {:ok, consumer} = ManualConsumer.start_link(producer)

    %{producer: producer, consumer: consumer, table: table}
  end

  defp receive_messages(count, received \\ [])

  defp receive_messages(count, received) when length(received) >= count, do: received

  defp receive_messages(count, received) do
    assert_receive {:messages, messages}
    receive_messages(count, received ++ messages)
  end

  defp flush_fake_events do
    receive do
      {:fake, _event} -> flush_fake_events()
    after
      0 -> :ok
    end
  end

  defp ack(successful, failed \\ []), do: Broadway.Acknowledger.ack_messages(successful, failed)

  defp rows(count), do: Enum.map(0..(count - 1)//1, &[&1])

  defp unique_table, do: "events_#{System.unique_integer([:positive])}"

  defp unique_name, do: :"pipeline_#{System.unique_integer([:positive])}"
end
