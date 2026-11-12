defmodule OffBroadway.Iceberg.Test.Pipeline do
  @moduledoc """
  A Broadway pipeline over `OffBroadway.Iceberg.Producer` for tests.

  Each handled message is sent to the `:test` process as `{:handled, message, pid}`,
  where `pid` is the processor. A `:handler` function, given the message, replaces that.
  """

  use Broadway

  def start_link(opts) do
    handler =
      Keyword.get_lazy(opts, :handler, fn ->
        test = Keyword.fetch!(opts, :test)
        &send(test, {:handled, &1, self()})
      end)

    Broadway.start_link(__MODULE__,
      name: Keyword.get(opts, :name, __MODULE__),
      producer: [
        module: {OffBroadway.Iceberg.Producer, Keyword.fetch!(opts, :producer)},
        concurrency: Keyword.get(opts, :producer_concurrency, 1)
      ],
      processors: [
        default: [
          concurrency: Keyword.get(opts, :processor_concurrency, 1),
          max_demand: Keyword.get(opts, :max_demand, 1)
        ]
      ],
      context: %{handler: handler}
    )
  end

  @impl true
  def handle_message(_processor, message, context) do
    case context.handler.(message) do
      %Broadway.Message{} = message -> message
      _other -> message
    end
  end
end
