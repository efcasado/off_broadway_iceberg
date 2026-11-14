defmodule OffBroadway.Iceberg.DistributionTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Iceberg.Distribution
  alias OffBroadway.Iceberg.Options
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

  describe "plan fingerprints" do
    test "are the same for the same plan, and differ for plans that only differ in grouping" do
      plans = [
        [["a"], ["b"]],
        [["a", "b"]],
        [["ab"]],
        [["a"], ["b"], []],
        [["b"], ["a"]],
        [["a", ""], ["b"]]
      ]

      fingerprints = Enum.map(plans, &Distribution.fingerprint/1)

      assert Enum.uniq(fingerprints) == fingerprints
      assert Distribution.fingerprint([["a"], ["b"]]) == hd(fingerprints)
      assert byte_size(hd(fingerprints)) == 32
    end

    test "cover the columns and rows a plan selects, and keep those of the slices alone without" do
      slices = [["a"], ["b"]]

      fingerprints =
        for {columns, where} <- [
              {nil, nil},
              {["id"], nil},
              {["ID"], nil},
              {["id", "ts"], nil},
              {["ts", "id"], nil},
              {["id,ts"], nil},
              {nil, "id > 1"},
              {nil, "id > 10"},
              {["id"], "id > 1"},
              {[""], nil},
              {nil, ""}
            ],
            do: Distribution.fingerprint(slices, columns, where)

      assert Enum.uniq(fingerprints) == fingerprints

      # Jobs recorded before plans could select anything keep their fingerprint.
      assert hd(fingerprints) == Distribution.fingerprint(slices)
      assert hd(fingerprints) == :crypto.hash(:sha256, <<2::32, 1::32, 1::32, "a", 1::32, 1::32, "b">>)
    end
  end

  describe "options" do
    test "defaults the top-level slice target to one million rows" do
      opts = Options.validate!(connection: MyApp.QuackDB, table: "events", snapshot_id: 7)
      assert opts[:slice_rows] == 1_000_000

      opts = Options.validate!(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, slice_rows: 25)
      assert opts[:slice_rows] == 25
    end

    test "requires a fixed snapshot" do
      assert_raise NimbleOptions.ValidationError, ~r/required :snapshot_id option not found/, fn ->
        prepare(connection: MyApp.QuackDB, table: "events")
      end

      assert_raise NimbleOptions.ValidationError, ~r/:snapshot_id/, fn ->
        prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: :current)
      end

      assert {[], _opts} = prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7)

      assert {[], _opts} =
               prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: {:as_of, ~U[2026-11-12 00:00:00Z]})
    end

    test "rejects non-positive slice targets and the removed distribute option" do
      for slice_rows <- [0, -1, "10", true, nil] do
        assert_raise NimbleOptions.ValidationError, ~r/:slice_rows/, fn ->
          prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, slice_rows: slice_rows)
        end
      end

      for distribute <- [true, false, [slice_rows: 10]] do
        assert_raise NimbleOptions.ValidationError, ~r/unknown options.*distribute/, fn ->
          prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7, distribute: distribute)
        end
      end
    end

    test "managed mode starts exactly one server and pool before any producer stage" do
      opts = [
        managed: [server: [init_sql: "INSTALL iceberg; LOAD iceberg;"], client: [pool_size: 4]],
        table: "events",
        snapshot_id: 7
      ]

      {children, broadway} = prepare(opts, 4)
      assert length(children) == 2
      assert [%{start: {QuackDB.Server, :start_link, [server_opts]}}, _pool] = children
      assert server_opts[:duckdb] == :managed
      assert server_opts[:init_sql] == "INSTALL iceberg; LOAD iceberg;"

      {Producer, scan_opts} = broadway[:producer][:module]
      assert scan_opts[:connection] == Module.concat(__MODULE__, QuackDB)
      refute Keyword.has_key?(scan_opts, :managed)
      assert broadway[:producer][:concurrency] == 4
      assert Options.validate!(scan_opts)[:connection] == Module.concat(__MODULE__, QuackDB)
    end

    test "managed mode respects explicit names and server settings" do
      opts = [
        managed: [
          server: [name: MyApp.DuckDB, settings: [threads: 2]],
          client: [name: MyApp.QuackDB, pool_size: 3]
        ],
        table: "events",
        snapshot_id: 7
      ]

      {[server, _pool], broadway} = prepare(opts)
      assert server.id == MyApp.DuckDB
      {Producer, scan_opts} = broadway[:producer][:module]
      assert scan_opts[:connection] == MyApp.QuackDB
      assert scan_opts[:snapshot_id] == 7
    end

    test "requires exactly one of an external pool or managed DuckDB" do
      assert_raise ArgumentError, ~r/exactly one/, fn ->
        prepare(table: "events", snapshot_id: 7)
      end

      assert_raise ArgumentError, ~r/exactly one/, fn ->
        prepare(connection: MyApp.QuackDB, managed: [server: []], table: "events", snapshot_id: 7)
      end

      assert {[], _} = prepare(connection: MyApp.QuackDB, table: "events", snapshot_id: 7)
    end

    test "allows more than one producer stage" do
      assert {[], _opts} = prepare([connection: MyApp.QuackDB, table: "events", snapshot_id: 7], 4)
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
    test "one producer groups files into slices using the default target" do
      pool = start_pool(files: @files)
      table = unique_table()
      Telemetry.attach_scan(table)
      producer = start_supervised!({Producer, connection: pool, table: table, snapshot_id: 42, fetch_size: 5})
      {:ok, consumer} = ManualConsumer.start_link(producer, ack: true)
      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows, batches: @batches}, _}, 5_000
      paths = Enum.map(@files, &elem(&1, 0))
      assert_receive {:fake, {:declare, _statement, [42 | ^paths], 5}}
      refute_received {:fake, {:declare, _statement, _params, _fetch_size}}
    end

    test "groups small data files into slices of at least :slice_rows rows" do
      pool = start_pool(files: Enum.take(@files, 10))
      table = unique_table()
      Telemetry.attach_scan(table)

      %{consumer: consumer} = start_member(pool, table, :a, slice_rows: 25)
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

    test "selects columns and rows from each slice, after binding them once before listing files" do
      pool = start_pool(files: Enum.take(@files, 1))
      table = unique_table()

      %{consumer: consumer} = start_member(pool, table, :a, columns: ["id"], where: {"id >= ?", [0]})
      ManualConsumer.ask(consumer, 1)

      assert_receive {:fake, {:declare, statement, [42, "file-00"], 5}}

      assert statement ==
               ~s|SELECT "id" FROM (SELECT * FROM "#{table}" AT (VERSION => ?) WHERE filename IN (?)) | <>
                 ~s|WHERE (\nid >= 0\n)|

      assert [{schema, [42]}, {bind, [42]}, {"SELECT file_path, record_count FROM iceberg_files" <> _, _params}] =
               for({:fake, {:query, statement, params}} <- flush(), do: {statement, params})

      assert schema == ~s|SELECT * FROM "#{table}" AT (VERSION => ?) LIMIT 0|
      assert bind == ~s|SELECT "id" FROM (SELECT * FROM "#{table}" AT (VERSION => ?)) WHERE (\nid >= 0\n) LIMIT 0|
    end

    test "rejects physical filename columns before listing files, even if they are not selected" do
      pool = start_pool(files: @files, columns: ["id", "FileName"])
      table = unique_table()
      Telemetry.attach_scan(table)

      start_member(pool, table, :a, columns: ["id"])

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], _,
                      %{reason: %ArgumentError{message: message}}}

      assert message =~ ~s(column named "FileName")
      assert message =~ "virtual filename column"
      refute_received {:fake, {:query, "SELECT file_path" <> _rest, _params}}
      refute_received {:fake, {:declare, _statement, _params, _max_rows}}
    end

    test "a selection DuckDB cannot bind fails the plan, before any data file is listed" do
      error = %QuackDB.Error{message: ~s(Binder Error: Referenced column "amount" not found)}
      pool = start_pool(files: @files, fail: {:bind, error})
      table = unique_table()
      Telemetry.attach_scan(table)

      start_member(pool, table, :a, columns: ["amount"])

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :exception], _, %{reason: ^error, snapshot_id: nil}}
      refute_received {:fake, {:query, "SELECT file_path" <> _rest, _params}}
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

  describe "failed batches" do
    test "of a slice another member finished count as this producer's own, not the scan's" do
      pool = start_pool(files: @files)
      table = unique_table()
      name = :"pipeline_#{System.unique_integer([:positive])}"
      Telemetry.attach_scan(table)
      test = self()

      # The only processor holds the first message, then fails it.
      handler = fn message ->
        if message.metadata.batch_index == 0 do
          send(test, {:blocked, self()})

          receive do
            :continue -> Broadway.Message.failed(message, "boom")
          end
        else
          message
        end
      end

      producer = [connection: pool, table: table, snapshot_id: 42, fetch_size: 5, slice_rows: 10]
      start_supervised!({Pipeline, name: name, handler: handler, producer: producer})
      assert_receive {:blocked, processor}

      # Another member finished every slice without failures, this one's included.
      [stage] = Broadway.producer_names(name)
      group = :sys.get_state(stage).state.module_state.group
      send(stage, {Distribution, group, {:done, Map.new(0..(@slices - 1), &{&1, {0, 2, 10}})}})
      send(processor, :continue)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, _}
      assert %{failed_batches: 0, local_failed_batches: 1, rows: @rows} = measurements
    end

    test "of a copy of a slice that lost to a copy without failures count as this producer's own" do
      pool = start_pool(files: @files)
      table = unique_table()
      Telemetry.attach_scan(table)

      # Read every slice, without acknowledging anything yet.
      producer =
        start_supervised!({Producer, connection: pool, table: table, snapshot_id: 42, fetch_size: 5, slice_rows: 10})

      {:ok, consumer} = ManualConsumer.start_link(producer, tag: :a)
      ManualConsumer.ask(consumer, 1_000)
      messages = received_all(:a, @batches)

      # Another member processed slice 0 without failures.
      group = :sys.get_state(producer).state.group
      send(producer, {Distribution, group, {:done, %{0 => {0, 2, 10}}}})

      # This producer's copy of slice 0 has a failed batch.
      {[failing], successful} = Enum.split_with(messages, &(&1.metadata.slice == 0 and &1.metadata.batch_index == 0))
      Broadway.Acknowledger.ack_messages(successful, [Broadway.Message.failed(failing, "boom")])

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, _}
      assert %{failed_batches: 0, local_failed_batches: 1, rows: @rows, batches: @batches} = measurements
    end

    test "of another member's copy of a slice end this producer's copy, without checkpoints" do
      pool = start_pool(files: @files)
      table = unique_table()
      Telemetry.attach_scan(table)

      %{producer: producer, consumer: consumer} = start_member(pool, table, :a)
      ManualConsumer.ask(consumer, 1)
      assert_receive {:messages, :a, [%{metadata: %{slice: 0}}]}

      group = :sys.get_state(producer).state.group
      send(producer, {Distribution, group, {:done, %{0 => {1, 2, 10}}}})
      refute match?(%{scan: %{slice: 0}}, :sys.get_state(producer).state)

      ManualConsumer.ask(consumer, 1_000)

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{failed_batches: 1, rows: @rows}, _}, 5_000
      refute Enum.any?(received(:a), &(&1.metadata.slice == 0))
    end

    test "of the only copy of a slice count as the scan's too" do
      pool = start_pool(files: @files)
      table = unique_table()
      Telemetry.attach_scan(table)

      producer =
        start_supervised!({Producer, connection: pool, table: table, snapshot_id: 42, fetch_size: 5, slice_rows: 10})

      {:ok, consumer} = ManualConsumer.start_link(producer, tag: :a)
      ManualConsumer.ask(consumer, 1_000)
      messages = received_all(:a, @batches)

      {[failing], successful} = Enum.split_with(messages, &(&1.metadata.slice == 3 and &1.metadata.batch_index == 1))
      Broadway.Acknowledger.ack_messages(successful, [Broadway.Message.failed(failing, "boom")])

      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, _}
      assert %{failed_batches: 1, local_failed_batches: 1} = measurements
    end
  end

  describe "a distributed scan" do
    test "each slice is opened once when all producers join before demand" do
      pool = start_pool(files: @files, pool_size: 4)
      table = unique_table()
      Telemetry.attach_scan(table)

      members = for id <- [:a, :b, :c], do: {id, start_member(pool, table, id)}

      # Wait until all producers agree on membership before issuing any demand.
      # Joining later during a read can legitimately cause duplicate work.
      pids = Enum.map(members, fn {_id, %{producer: producer}} -> producer end)

      assert Enum.any?(1..100, fn _attempt ->
               if Enum.all?(pids, fn pid ->
                    state = :sys.get_state(pid).state
                    state.group != nil and Enum.sort(state.members) == Enum.sort(pids)
                  end) do
                 true
               else
                 Process.sleep(10)
                 false
               end
             end)

      Enum.each(members, fn {_id, %{consumer: consumer}} -> ManualConsumer.ask(consumer, 1_000) end)

      for _ <- members do
        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop],
                        %{rows: @rows, batches: @batches, failed_batches: 0}, _}, 5_000
      end

      # The FakeConnection records each actual DuckDB cursor opened, independently
      # of the final messages. If two producers read a slice, this catches it.
      events = flush()

      declared_paths =
        for {:fake, {:declare, _statement, [42 | paths], 5}} <- events,
            path <- paths,
            do: path

      expected_paths = Enum.map(@files, &elem(&1, 0))
      assert Enum.sort(declared_paths) == expected_paths

      # All three producers did the metadata planning independently.
      plans =
        for {:fake,
             {:query, "SELECT file_path, record_count FROM iceberg_files(?, snapshot_from_id => ?)" <> _, [_, 42]}} <-
              events,
            do: true

      assert length(plans) == length(members)
    end

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

      producer = [connection: pool, table: table, snapshot_id: 42, fetch_size: 5, slice_rows: 10]
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

    test "is not shared by producers that select other columns or rows" do
      pool = start_pool(files: @files, pool_size: 4)
      table = unique_table()
      Telemetry.attach_scan(table)

      %{consumer: consumer} = start_member(pool, table, :a, columns: ["id"], where: "id >= 0")
      ManualConsumer.ask(consumer, 1_000)
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows}, _}, 5_000

      # A producer of the same scan that joins after it ended reads nothing, while one
      # that selects anything else reads every slice itself.
      start_member(pool, table, :same, columns: ["id"], where: {"id >= ?", [0]})
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows}, _}, 5_000
      assert received(:same) == []

      for {id, selection} <- [
            other_rows: [columns: ["id"], where: "id >= 1"],
            other_columns: [columns: ["id", "ts"], where: "id >= 0"],
            everything: []
          ] do
        %{consumer: consumer} = start_member(pool, table, id, selection)
        ManualConsumer.ask(consumer, 1_000)

        assert length(received_all(id, @batches)) == @batches
        assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows}, _}, 5_000
      end
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
    Producer.prepare_for_start(__MODULE__, name: __MODULE__, producer: [module: {Producer, producer_opts}, concurrency: concurrency])
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
        [connection: pool, table: table, snapshot_id: 42, fetch_size: 5, slice_rows: 10],
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

  # Waits for `count` messages from the consumer tagged `id`.
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

  defp ids(messages), do: Enum.flat_map(messages, &QuackDB.Columns.get(&1.data, "id"))

  defp unique_table, do: "events_#{System.unique_integer([:positive])}"
end
