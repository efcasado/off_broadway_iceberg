defmodule OffBroadway.Iceberg.ClusterTest do
  # The producers of a distributed scan, each on its own node, connected only through
  # Erlang distribution. The test VM becomes a distributed node for this, so it does
  # not run alongside other tests.
  use ExUnit.Case, async: false

  alias OffBroadway.Iceberg.Test.Member

  @moduletag :capture_log

  @slices 32
  @rows 320
  # 10 rows per slice and 5 rows per message make 2 messages per slice.
  @batches 64

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
          rows: Enum.map(0..(@rows - 1), &[&1]),
          table: table,
          snapshot_id: 42,
          fetch_size: 5,
          distribute: [key: "id", slices: @slices]
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

  defp ids(messages), do: Enum.flat_map(messages, &QuackDB.Columns.get(&1.data, "id"))
end
