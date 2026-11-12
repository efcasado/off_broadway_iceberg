defmodule OffBroadway.Iceberg.Test.Iceberg do
  @moduledoc """
  The integration test environment: an Iceberg REST catalog in Docker (see
  `docker-compose.yml`) and a managed DuckDB server, started by QuackDB, attached to it.
  """

  @catalog_uri "http://localhost:18181"
  @token "off-broadway-iceberg-test"

  @doc "The URI of the DuckDB server `duckdb_spec/3` starts on `port`."
  def uri(port), do: "http://[::1]:#{port}"

  @doc "The token of every DuckDB server `duckdb_spec/3` starts."
  def token, do: @token

  @doc """
  Starts the REST catalog with an empty warehouse under `tmp/`.
  """
  def start_catalog! do
    warehouse = warehouse()
    File.rm_rf!(warehouse)
    File.mkdir_p!(warehouse)
    # The catalog creates directories in it as a user of its own.
    File.chmod!(warehouse, 0o777)

    compose!(["up", "--detach", "--wait"])
  end

  @doc """
  Stops the REST catalog and deletes the warehouse.
  """
  def stop_catalog! do
    compose!(["down", "--volumes"])
    File.rm_rf!(warehouse())
  end

  @doc """
  A child spec for a managed DuckDB server on `port` and a QuackDB pool named `name`
  connected to it. Every quack setting is global, so `global_settings` go to the server.
  """
  def duckdb_spec(name, port, opts \\ []) do
    children =
      QuackDB.Server.child_specs(
        server: [
          duckdb: :managed,
          endpoint: "quack:localhost:#{port}",
          token: @token,
          settings: [threads: 2],
          global_settings: Keyword.get(opts, :global_settings, [])
        ],
        client: [name: name, pool_size: Keyword.get(opts, :pool_size, 4)]
      )

    %{
      id: Keyword.get(opts, :id, name),
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]}
    }
  end

  @doc """
  Loads the iceberg extension on `pool`'s server and attaches the catalog as `lake`,
  as the README does. Running it again changes nothing.
  """
  def attach_catalog!(pool) do
    QuackDB.query!(pool, "INSTALL iceberg")
    QuackDB.query!(pool, "LOAD iceberg")

    QuackDB.query!(
      pool,
      "ATTACH IF NOT EXISTS 'warehouse' AS lake " <>
        "(TYPE iceberg, URI '#{@catalog_uri}', AUTHORIZATION_TYPE 'none')"
    )
  end

  @doc """
  Creates an empty schema in the catalog and returns its name.
  """
  def create_schema!(pool) do
    schema = "lake.it_#{System.unique_integer([:positive])}"
    QuackDB.query!(pool, "CREATE SCHEMA #{schema}")
    schema
  end

  @doc """
  Creates an empty `(id BIGINT, payload VARCHAR)` table in `schema` and returns its name.
  """
  def create_table!(pool, schema) do
    table = "#{schema}.t_#{System.unique_integer([:positive])}"
    QuackDB.query!(pool, "CREATE TABLE #{table} (id BIGINT, payload VARCHAR)")
    table
  end

  @doc """
  Commits the rows with ids in `range` as one snapshot and returns the snapshot's id.
  Each payload is `payload_bytes` long and unique to its row.
  """
  def insert!(pool, table, first..last//1, payload_bytes \\ 32) do
    QuackDB.query!(
      pool,
      "INSERT INTO #{table} SELECT i, left(repeat(md5(i::VARCHAR), ?), ?) FROM range(?, ?) t(i)",
      [div(payload_bytes, 32) + 1, payload_bytes, first, last + 1]
    )

    pool |> snapshots(table) |> List.last()
  end

  @doc """
  The table's snapshot ids, oldest first.
  """
  def snapshots(pool, table) do
    %{rows: rows} =
      QuackDB.query!(pool, "SELECT snapshot_id FROM iceberg_snapshots(?) ORDER BY sequence_number", [table])

    List.flatten(rows)
  end

  defp warehouse, do: Path.expand("tmp/warehouse")

  defp compose!(args) do
    {output, status} =
      System.cmd("docker", ["compose" | args], env: [{"ICEBERG_WAREHOUSE", warehouse()}], stderr_to_stdout: true)

    if status != 0, do: raise("docker compose #{Enum.join(args, " ")} failed:\n#{output}")

    :ok
  end
end
