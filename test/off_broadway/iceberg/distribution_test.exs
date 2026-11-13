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

  # 32 data files of 10 rows. With a slice of 10 rows, each file is a slice, and with 5
  # rows per message, each slice makes 2 messages.
  @slices 32
  @rows 320
  @batches 64
  @files for file <- 0..(@slices - 1),
             do: {"file-#{String.pad_leading("#{file}", 2, "0")}", Enum.map((file * 10)..(file * 10 + 9), &[&1])}

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
        prepare(connection: MyApp.QuackDB, table: "events", distribute: true)
      end

      assert {[], _opts} = prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: true)

      assert {[], _opts} =
               prepare(
                 connection: MyApp.QuackDB,
                 table: "events",
                 snapshot_id: {:as_of, DateTime.utc_now()},
                 distribute: [slice_rows: 100]
               )
    end

    test "reject anything but true or a positive :slice_rows" do
      for distribute <- [[slice_rows: 0], [slice_rows: "10"], [slices: 4], :yes] do
        assert_raise NimbleOptions.ValidationError, ~r/:distribute/, fn ->
          prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: distribute)
        end
      end
    end

    test "allow more than one producer stage" do
      assert {[], _opts} = prepare([connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: true], 4)
    end
  end

  describe "snapshots" do
    test "{:as_of, datetime} resolves through the snapshot log, in UTC" do
      pool = start_pool(rows: [[1]])
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

  describe "planning" do
    test "groups small data files into slices of at least :slice_rows rows" do
      pool = start_pool(files: Enum.take(@files, 10))
      table = unique_table()
      Telemetry.attach_scan(table)

      %{consumer: consumer} = start_member(pool, table, :a, distribute: [slice_rows: 25])
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: 100}, _}, 5_000

      assert_received {:fake,
                       {:query, "SELECT file_path, record_count FROM iceberg_files(?, snapshot_from_id => ?)" <> _,
                        [_, 42]}}

      declared = for {:fake, {:declare, _statement, [42 | paths], 5}} <- flush(), do: paths

      assert Enum.sort(declared) == [
               ~w(file-00 file-01 file-02),
               ~w(file-03 file-04 file-05),
               ~w(file-06 file-07 file-08),
               ~w(file-09)
             ]
    end

    test "reads each slice by its files' names" do
      pool = start_pool(files: Enum.take(@files, 1))
      table = unique_table()

      %{consumer: consumer} = start_member(pool, table, :a)
      ManualConsumer.ask(consumer, 1)

      assert_receive {:fake, {:declare, statement, [42, "file-00"], 5}}
      assert statement == ~s|SELECT * FROM "#{table}" AT (VERSION => ?) WHERE filename IN (?)|
    end

    test "a snapshot without data files completes at once" do
      pool = start_pool(files: [])
      table = unique_table()
      Telemetry.attach_scan(table)

      start_member(pool, table, :a)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: 0, batches: 0}, _}
    end

    test "a plan that fails fails the scan" do
      error = %QuackDB.Error{message: "IO Error: catalog unreachable"}
      pool = start_pool(files: @files, fail: {:plan, error})
      table = unique_table()
      Telemetry.attach_scan(table)

      start_member(pool, table, :a)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], _, %{reason: ^error, snapshot_id: nil}}
    end
  end

  describe "a distributed scan" do
    test "is not reported complete while this producer's messages are still being processed" do
      pool = start_pool(files: @files)
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

      producer = [connection: pool, table: table, snapshot_id: 42, fetch_size: 5, distribute: [slice_rows: 10]]
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
      pool = start_pool(files: @files, pool_size: 4)
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

      # Slice i is file i, which holds the rows 10 * i to 10 * i + 9.
      for message <- a ++ b, id <- QuackDB.Columns.get(message.data, "id") do
        assert div(id, 10) == message.metadata.slice
      end
    end

    test "is reported complete to a producer that joins after it ends, which reads nothing" do
      pool = start_pool(files: @files, pool_size: 4)
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
      pool = start_pool(files: @files, pool_size: 4)
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
      healthy = start_pool(files: @files, pool_size: 2)
      failing = start_pool([files: @files, fail: {:declare, error}], id: :failing)
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
  defp start_member(pool, table, id, opts \\ []) do
    opts =
      Keyword.merge(
        [connection: pool, table: table, snapshot_id: 42, fetch_size: 5, distribute: [slice_rows: 10]],
        opts
      )

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

  defp flush(messages \\ []) do
    receive do
      message -> flush([message | messages])
    after
      0 -> Enum.reverse(messages)
    end
  end

  defp ids(messages), do: Enum.flat_map(messages, &QuackDB.Columns.get(&1.data, "id"))

  defp unique_table, do: "events_#{System.unique_integer([:positive])}"
end
