defmodule OffBroadway.Iceberg.ClusterTest do
  # The producers of a distributed scan, each on its own node, connected only through
  # Erlang distribution. The test VM becomes a distributed node for this, so it does
  # not run alongside other tests.
  use ExUnit.Case, async: false

  alias OffBroadway.Iceberg.Test.Member

  @moduletag :capture_log

  # 32 data files of 10 rows. With a slice of 10 rows, each file is a slice, and with 5
  # rows per message, each slice makes 2 messages.
  @slices 32
  @rows 320
  @batches 64
  @files for file <- 0..(@slices - 1),
             do: {"file-#{String.pad_leading("#{file}", 2, "0")}", Enum.map((file * 10)..(file * 10 + 9), &[&1])}

  setup_all do
    if !Node.alive?() do
      {_output, 0} = System.cmd("epmd", ["-daemon"])
      {:ok, _pid} = :net_kernel.start(:"off_broadway_iceberg_test@127.0.0.1", %{name_domain: :longnames})
    end

    :ok
  end

  setup do
    %{table: "events_#{System.unique_integer([:positive])}"}
  end

  test "producers on different nodes share one scan", %{table: table} do
    # Both join before either reads, so that neither reads alone.
    members = for tag <- [:a, :b], do: tag |> start_node() |> start_member(tag, table, demand: 0)
    Enum.each(members, &Member.ask(&1, 1_000))

    for _member <- [:a, :b] do
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, _}, 10_000
      assert measurements == Map.merge(measurements, %{rows: @rows, batches: @batches, failed_batches: 0})
    end

    a = received(:a)
    b = received(:b)

    assert a != [] and b != [], "both nodes should have read slices"
    assert (a ++ b) |> ids() |> Enum.uniq() |> Enum.sort() == Enum.to_list(0..(@rows - 1))
    # Only a slice a node started before the other joined can be read twice.
    assert length(a) + length(b) < 2 * @batches, "each node read the whole scan"
  end

  test "named pipelines with different local concurrency share the same scan across nodes", %{table: table} do
    name = :"pipeline_#{System.unique_integer([:positive])}"

    for {tag, concurrency} <- [a: 1, b: 3] do
      {_peer, node} = start_node(tag)

      :erpc.call(node, Member, :start_pipeline, [
        self(),
        tag,
        [
          name: name,
          files: @files,
          table: table,
          snapshot_id: 42,
          fetch_size: 5,
          slice_rows: 10,
          producer_concurrency: concurrency
        ]
      ])
    end

    # Hold processing until all four stages have planned the scan. This also exercises
    # Broadway's pipeline name propagation; stage indexes must not split the group.
    for _stage <- 1..4 do
      assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :start], _, %{name: ^name}}, 10_000
    end

    messages = collect_pipeline(name, 4)
    assert messages |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort() == [:a, :b]
    assert messages |> Enum.map(&elem(&1, 1)) |> ids() |> Enum.uniq() |> Enum.sort() == Enum.to_list(0..(@rows - 1))
    assert length(messages) < 2 * @batches, "each app instance read the whole scan"
  end

  test "a node that leaves mid-scan has its slices read by the others", %{table: table} do
    node_a = start_node(:a)
    {peer_b, _node} = node_b = start_node(:b)
    # b starts first, reads one message and then waits, holding its slices.
    start_member(node_b, :b, table, demand: 1)
    start_member(node_a, :a, table)

    refute_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], _, _}, 500

    # As when an autoscaler removes a pod.
    :peer.stop(peer_b)

    assert_receive {:telemetry, [:off_broadway_iceberg, :scan, :stop], %{rows: @rows, batches: @batches}, _}, 10_000
    assert (received(:a) ++ received(:b)) |> ids() |> Enum.uniq() |> Enum.sort() == Enum.to_list(0..(@rows - 1))
  end

  defp start_node(tag) do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"off_broadway_iceberg_#{tag}_#{System.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
      })

    # As DNSCluster or libcluster do, connect every node to every other one.
    for other <- Node.list(), other != node, do: true = :erpc.call(node, :net_kernel, :connect_node, [other])
    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    :ok = :erpc.call(node, Logger, :configure, [[level: :error]])
    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:off_broadway_iceberg])
    {peer, node}
  end

  defp start_member({_peer, node}, tag, table, opts \\ []) do
    opts =
      Keyword.merge(
        [
          files: @files,
          table: table,
          snapshot_id: 42,
          fetch_size: 5,
          slice_rows: 10
        ],
        opts
      )

    :erpc.call(node, Member, :start, [self(), tag, opts])
  end

  defp received(tag, messages \\ []) do
    receive do
      {:messages, ^tag, more} -> received(tag, messages ++ more)
    after
      0 -> messages
    end
  end

  defp collect_pipeline(name, remaining, messages \\ [])
  defp collect_pipeline(_name, 0, messages), do: messages

  defp collect_pipeline(name, remaining, messages) do
    receive do
      {:handled, tag, message, processor} ->
        send(processor, :continue)
        collect_pipeline(name, remaining, [{tag, message} | messages])

      {:telemetry, [:off_broadway_iceberg, :scan, :stop], measurements, %{name: ^name}} ->
        assert %{rows: @rows, batches: @batches, failed_batches: 0} = measurements
        collect_pipeline(name, remaining - 1, messages)

      {:telemetry, [:off_broadway_iceberg, :scan, :exception], _, %{name: ^name} = metadata} ->
        flunk("pipeline scan failed: #{inspect(metadata.reason)}")
    after
      10_000 -> flunk("#{remaining} producer stages did not complete")
    end
  end

  defp ids(messages), do: Enum.flat_map(messages, &QuackDB.Columns.get(&1.data, "id"))
end
