defmodule OffBroadway.Iceberg.Test.ManualConsumer do
  @moduledoc """
  A GenStage consumer that asks for exactly the demand a test gives it and forwards
  the events it receives to the test process as `{:messages, messages}`.
  """

  use GenStage

  def start_link(producer) do
    {:ok, consumer} = GenStage.start_link(__MODULE__, self())
    {:ok, _subscription} = GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)
    {:ok, consumer}
  end

  def ask(consumer, demand), do: GenStage.call(consumer, {:ask, demand})

  @impl true
  def init(test), do: {:consumer, %{test: test, subscription: nil}}

  @impl true
  def handle_subscribe(:producer, _opts, from, state), do: {:manual, %{state | subscription: from}}

  @impl true
  def handle_call({:ask, demand}, _from, state) do
    GenStage.ask(state.subscription, demand)
    {:reply, :ok, [], state}
  end

  @impl true
  def handle_events(messages, _from, state) do
    send(state.test, {:messages, messages})
    {:noreply, [], state}
  end
end
