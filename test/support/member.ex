defmodule OffBroadway.Iceberg.Test.Member do
  @moduledoc """
  Runs one member of a distributed scan, on whichever node calls `start/3`, for tests
  that spread a scan over several nodes.

  The member is a fake database, a producer and a consumer that acknowledges every
  message. It reports to `test` as `ManualConsumer` and `FakeConnection` do, and
  forwards the scan's telemetry events for its table as
  `{:telemetry, event, measurements, metadata}`. It lives until `stop/1`, and its
  consumer asks for more with `ask/2`.
  """

  alias OffBroadway.Iceberg.Producer
  alias OffBroadway.Iceberg.Test.FakeConnection
  alias OffBroadway.Iceberg.Test.ManualConsumer
  alias OffBroadway.Iceberg.Test.Telemetry

  @events [
    [:off_broadway_iceberg, :scan, :start],
    [:off_broadway_iceberg, :scan, :stop],
    [:off_broadway_iceberg, :scan, :exception]
  ]

  @doc """
  Starts the member, tagging what it forwards with `tag`. Options: `:files`, the data
  files of the fake database, `:demand` (default 1_000), and the producer's `:table`,
  `:snapshot_id`, `:fetch_size` and `:distribute`.
  """
  def start(test, tag, opts) do
    owner = self()

    holder =
      spawn(fn ->
        Logger.configure(level: :error)
        table = Keyword.fetch!(opts, :table)
        id = {__MODULE__, tag}
        :telemetry.attach_many(id, @events, &Telemetry.handle_event/4, {test, &(&1.table == table)})

        {:ok, pool} = FakeConnection.start_link(test: test, script: [files: Keyword.fetch!(opts, :files)], pool_size: 1)
        producer_opts = Keyword.take(opts, [:table, :snapshot_id, :fetch_size, :distribute])
        {:ok, producer} = GenStage.start_link(Producer, [connection: pool] ++ producer_opts)
        {:ok, consumer} = ManualConsumer.start_link(producer, test: test, tag: tag, ack: true)
        demand = Keyword.get(opts, :demand, 1_000)
        if demand > 0, do: ManualConsumer.ask(consumer, demand)
        send(owner, {__MODULE__, :started})
        serve(consumer, id)
      end)

    receive do
      {__MODULE__, :started} -> holder
    end
  end

  @doc """
  Asks the member's consumer for `demand` more messages.
  """
  def ask(holder, demand), do: send(holder, {:ask, demand})

  @doc """
  Stops the member's processes.
  """
  def stop(holder), do: send(holder, :stop)

  defp serve(consumer, id) do
    receive do
      {:ask, demand} ->
        ManualConsumer.ask(consumer, demand)
        serve(consumer, id)

      :stop ->
        :telemetry.detach(id)
        exit(:shutdown)
    end
  end
end
