defmodule OffBroadway.Iceberg.Test.ManualConsumer do
  @moduledoc """
  A GenStage consumer that asks for exactly the demand a test gives it and forwards
  the events it receives to the test process as `{:messages, messages}`.

  Options:

  - `:test` - the process to forward to (default: the caller)
  - `:tag` - forward `{:messages, tag, messages}` instead
  - `:ack` - acknowledge every message as successful when it arrives (default `false`)
  """

  use GenStage

  def start_link(producer, opts \\ []) do
    opts = Keyword.put_new(opts, :test, self())
    {:ok, consumer} = GenStage.start_link(__MODULE__, opts)
    {:ok, _subscription} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
    {:ok, consumer}
  end

  def ask(consumer, demand), do: GenStage.call(consumer, {:ask, demand})

  @impl true
  def init(opts), do: {:consumer, opts |> Map.new() |> Map.put(:subscription, nil)}

  @impl true
  def handle_subscribe(:producer, _opts, from, state), do: {:manual, %{state | subscription: from}}

  @impl true
  def handle_call({:ask, demand}, _from, state) do
    GenStage.ask(state.subscription, demand)
    {:reply, :ok, [], state}
  end

  @impl true
  def handle_events(messages, _from, state) do
    if state[:ack], do: Broadway.Acknowledger.ack_messages(messages, [])

    case Map.fetch(state, :tag) do
      {:ok, tag} -> send(state.test, {:messages, tag, messages})
      :error -> send(state.test, {:messages, messages})
    end

    {:noreply, [], state}
  end
end
