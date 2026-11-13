defmodule OffBroadway.Iceberg.DistributionTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Iceberg.Distribution
  alias OffBroadway.Iceberg.Producer
  alias OffBroadway.Iceberg.Test.FakeConnection
  alias OffBroadway.Iceberg.Test.ManualConsumer
  alias OffBroadway.Iceberg.Test.Pipeline
  alias OffBroadway.Iceberg.Test.Telemetry

  # DBConnection logs the disconnects that cancelled scans cause.
  @moduletag :capture_log

  @slices 32
  @rows 320
  # 10 rows per slice and 5 rows per message make 2 messages per slice.
  @batches 64

  describe "ownership" do
    test "does not depend on the order members are listed in" do
      members = for _member <- 1..5, do: spawn(fn -> :ok end)

      for slice <- 0..(@slices - 1) do
        assert Distribution.owner(:group, slice, members) == Distribution.owner(:group, slice, Enum.reverse(members))
      end
    end

    test "only moves the slices of a member that leaves" do
      [leaving | staying] = members = for _member <- 1..5, do: spawn(fn -> :ok end)
      before = Map.new(0..255, &{&1, Distribution.owner(:group, &1, members)})
      later = Map.new(0..255, &{&1, Distribution.owner(:group, &1, staying)})

      moved = for {slice, owner} <- before, later[slice] != owner, do: owner
      assert Enum.uniq(moved) == [leaving]
    end
  end

  describe "options" do
    test "require a snapshot every node resolves the same way" do
      assert_raise ArgumentError, ~r/requires an integer or \{:as_of, datetime\} :snapshot_id with :distribute/, fn ->
        prepare(connection: MyApp.QuackDB, table: "events", distribute: [key: "id"])
      end

      assert {[], _opts} = prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: [key: "id"])

      assert {[], _opts} =
               prepare(
                 connection: MyApp.QuackDB,
                 table: "events",
                 snapshot_id: {:as_of, DateTime.utc_now()},
                 distribute: [key: ["id", "odd \"name\""], slices: 8]
               )
    end

    test "reject keys that are not column names" do
      for key <- ["", [], [""], :id, ["id", 1], ["a" <> <<0>>]] do
        assert_raise NimbleOptions.ValidationError, ~r/:key/, fn ->
          prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: [key: key])
        end
      end
    end

    test "allow more than one producer stage" do
      opts = [connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: [key: "id"]]
      assert {[], _opts} = prepare(opts, 4)
    end
  end

  describe "snapshots" do
    test "{:as_of, datetime} resolves through the snapshot log, in UTC" do
      pool = start_pool(rows: rows(10))
      table = unique_table()

      as_of = %DateTime{
        year: 2026,
        month: 11,
        day: 12,
        hour: 13,
        minute: 0,
        second: 0,
        microsecond: {0, 0},
        time_zone: "Europe/Madrid",
        zone_abbr: "CET",
        utc_offset: 3600,
        std_offset: 0
      }

      start_supervised!({Producer, connection: pool, table: table, snapshot_id: {:as_of, as_of}})

      quoted = ~s("#{table}")
      assert_receive {:fake, {:query, "SELECT * FROM " <> _rest, []}}
      assert_receive {:fake, {:query, "SELECT snapshot_id FROM iceberg_history(?)" <> _rest, [^quoted, as_of_utc]}}
      assert as_of_utc == ~N[2026-11-12 12:00:00.000000]
    end
  end

  describe "a distributed scan" do
    test "is not reported complete while this producer's messages are still being processed" do
      pool = start_pool(rows: rows(@rows))
      table = unique_table()
      name = :"pipeline_#{System.unique_integer([:positive])}"
      Telemetry.attach_scan(table)
      test = self()

      # The only processor holds the first message until told to continue.
      handler = fn message ->
        if message.metadata.batch_index == 0 do
          send(test, {:blocked, self()})

          receive do
            :continue -> :ok
          end
        end

        message
      end

      producer = [
        connection: pool,
        table: table,
        snapshot_id: 42,
        fetch_size: 5,
        distribute: [key: "id", slices: @slices]
      ]

      start_supervised!({Pipeline, name: name, handler: handler, producer: producer})
      assert_receive {:blocked, processor}

      # As when another member finishes every slice, this one's included.
      [stage] = Broadway.producer_names(name)
      group = :sys.get_state(stage).state.module_state.group
      done = Map.new(0..(@slices - 1), &{&1, {0, 2, 10}})
      send(stage, {Distribution, group, {:done, done}})

      refute_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], _, _}, 200

      send(processor, :continue)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows, batches: @batches}, _}
    end

    test "is shared by its producers, which all report the whole scan" do
      pool = start_pool(rows: rows(@rows), pool_size: 4)
      table = unique_table()
      Telemetry.attach_scan(table)

      members = for id <- [:a, :b], do: start_member(pool, table, id)
      Enum.each(members, &ManualConsumer.ask(&1.consumer, 1_000))

      for _member <- members do
        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, _}, 5_000
        assert measurements == Map.merge(measurements, %{rows: @rows, batches: @batches, failed_batches: 0})
      end

      a = received(:a)
      b = received(:b)

      assert a != [] and b != [], "both producers should have read slices"
      assert (a ++ b) |> ids() |> Enum.uniq() |> Enum.sort() == Enum.to_list(0..(@rows - 1))
      # Only a slice a producer started before the other joined can be read twice.
      assert length(a) + length(b) < 2 * @batches, "each producer read the whole scan"

      for message <- a ++ b, id <- QuackDB.Columns.get(message.data, "id") do
        assert rem(id, @slices) == message.metadata.slice
      end

      assert_received {:fake, {:declare, statement, [42, @slices, _slice], 5}}
      assert statement =~ ~r/ AT \(VERSION => \?\) WHERE hash\("id"\) % \? = \?$/
    end

    test "is reported complete to a producer that joins after it ends, which reads nothing" do
      pool = start_pool(rows: rows(@rows), pool_size: 4)
      table = unique_table()
      Telemetry.attach_scan(table)

      members = for id <- [:a, :b], do: start_member(pool, table, id)
      Enum.each(members, &ManualConsumer.ask(&1.consumer, 1_000))
      for _member <- members, do: assert_receive({:telemetry, [:off_broadway_iceberg, :scan, :stop], _, _}, 5_000)

      start_member(pool, table, :c)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows, batches: @batches}, _}, 5_000
      assert received(:c) == []
    end

    test "survives a producer that stops mid-scan, whose slices others read" do
      pool = start_pool(rows: rows(@rows), pool_size: 4)
      table = unique_table()
      Telemetry.attach_scan(table)

      a = start_member(pool, table, :a)
      b = start_member(pool, table, :b)
      ManualConsumer.ask(a.consumer, 1_000)
      # b reads one message and then waits, holding its slices.
      ManualConsumer.ask(b.consumer, 1)

      refute_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], _, _}, 200

      stop_supervised!(:b)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows, batches: @batches}, _}, 5_000
      assert (received(:a) ++ received(:b)) |> ids() |> Enum.uniq() |> Enum.sort() == Enum.to_list(0..(@rows - 1))
    end

    test "survives a producer that fails, whose slices others read" do
      error = %QuackDB.Error{message: "IO Error: connection reset"}
      healthy = start_pool(rows: rows(@rows), pool_size: 2)
      failing = start_pool([rows: rows(@rows), fail: {:declare, error}], id: :failing)
      table = unique_table()
      Telemetry.attach_scan(table)

      a = start_member(healthy, table, :a)
      b = start_member(failing, table, :b)
      ManualConsumer.ask(a.consumer, 1_000)
      ManualConsumer.ask(b.consumer, 1_000)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], _, %{reason: ^error}}, 5_000
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows, batches: @batches}, _}, 5_000
      assert :a |> received() |> ids() |> Enum.uniq() |> Enum.sort() == Enum.to_list(0..(@rows - 1))
      refute_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], _, _}, 200
    end
  end

  defp prepare(producer_opts, concurrency \\ 1) do
    Producer.prepare_for_start(__MODULE__, producer: [module: {Producer, producer_opts}, concurrency: concurrency])
  end

  defp start_pool(script, opts \\ []) do
    opts = Keyword.merge([test: self(), script: script, pool_size: 1], opts)
    pool = start_supervised!({FakeConnection, opts})
    for _connection <- 1..opts[:pool_size], do: assert_receive({:fake, {:connect}})
    pool
  end

  # A producer of the scan, and a consumer that acknowledges every message and forwards
  # it to the test as `{:messages, id, messages}`.
  defp start_member(pool, table, id) do
    opts = [
      connection: pool,
      table: table,
      snapshot_id: 42,
      fetch_size: 5,
      distribute: [key: "id", slices: @slices]
    ]

    producer = start_supervised!({Producer, opts}, id: id)
    {:ok, consumer} = ManualConsumer.start_link(producer, tag: id, ack: true)
    %{producer: producer, consumer: consumer}
  end

  defp received(id, messages \\ []) do
    receive do
      {:messages, ^id, more} -> received(id, messages ++ more)
    after
      0 -> messages
    end
  end

  defp ids(messages), do: Enum.flat_map(messages, &QuackDB.Columns.get(&1.data, "id"))

  defp rows(count), do: Enum.map(0..(count - 1)//1, &[&1])

  defp unique_table, do: "events_#{System.unique_integer([:positive])}"
end
