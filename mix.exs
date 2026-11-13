defmodule OffBroadwayIceberg.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_iceberg,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:ex_unit]
      ],
      description: description(),
      package: package(),
      source_url: "https://github.com/efcasado/off_broadway_iceberg",
      docs: [
        main: "OffBroadway.Iceberg.Producer",
        extras: ["README.md"]
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.github": :test,
        "coveralls.html": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {OffBroadway.Iceberg.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.3"},
      {:db_connection, "~> 2.10"},
      {:nimble_options, "~> 1.2"},
      {:quackdb, "~> 0.7.0"},
      {:telemetry, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.4", only: :dev, runtime: false},
      {:styler, "~> 1.12", only: [:dev, :test], runtime: false},
      {:junit_formatter, "~> 3.4", only: :test},
      {:excoveralls, "~> 0.18.5", only: :test}
    ]
  end

  defp description do
    "A Broadway producer for Apache Iceberg snapshots, built on QuackDB and DuckDB."
  end

  defp package do
    [
      name: "off_broadway_iceberg",
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE),
      licenses: ~w(MIT),
      links: %{
        "GitHub" => "https://github.com/efcasado/off_broadway_iceberg"
      }
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
