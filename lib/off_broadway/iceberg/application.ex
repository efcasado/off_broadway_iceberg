defmodule OffBroadway.Iceberg.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link([OffBroadway.Iceberg.Distribution],
      strategy: :one_for_one,
      name: OffBroadway.Iceberg.Supervisor
    )
  end
end
