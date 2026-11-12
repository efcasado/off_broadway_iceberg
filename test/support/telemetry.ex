defmodule OffBroadway.Iceberg.Test.Telemetry do
  @moduledoc """
  Forwards telemetry events to the test process as
  `{:telemetry, event, measurements, metadata}`.
  """

  @scan_events [
    [:off_broadway_iceberg, :scan, :start],
    [:off_broadway_iceberg, :scan, :stop],
    [:off_broadway_iceberg, :scan, :exception]
  ]

  @doc """
  Forwards the producer's scan events for `table`. Async tests share the handlers, so
  events for other tables are dropped.
  """
  def attach_scan(table), do: attach(@scan_events, &(&1.table == table))

  @doc """
  Forwards the scan events of the Broadway pipeline `name`.
  """
  def attach_pipeline(name), do: attach(@scan_events, &(&1.name == name))

  @doc """
  Forwards `events` whose metadata passes `filter`, until the test exits.
  """
  def attach(events, filter \\ fn _metadata -> true end) do
    id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(id, events, &__MODULE__.handle_event/4, {self(), filter})
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
  end

  def handle_event(event, measurements, metadata, {test, filter}) do
    if filter.(metadata), do: send(test, {:telemetry, event, measurements, metadata})
  end
end
