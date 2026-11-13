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
        extras: ["README.md", "guides/iceberg-and-duckdb.md"],
        groups_for_extras: [Guides: ~r"guides/"],
        before_closing_body_tag: %{
          html: """
          <script defer src="https://cdn.jsdelivr.net/npm/mermaid@10.9.3/dist/mermaid.min.js"></script>
          <script>
            (() => {
              let sequence = 0;

              window.addEventListener("exdoc:loaded", () => {
                if (!window.mermaid) return;

                mermaid.initialize({
                  startOnLoad: false,
                  theme: document.body.classList.contains("dark") ? "dark" : "default"
                });

                for (const code of document.querySelectorAll("pre code.mermaid")) {
                  const pre = code.parentElement;
                  const container = document.createElement("div");
                  container.className = "iceberg-mermaid";
                  container.style.maxWidth = "100%";
                  const id = "iceberg-mermaid-" + sequence++;

                  mermaid.render(id, code.textContent).then(({svg, bindFunctions}) => {
                    if (!pre.isConnected) return;
                    container.innerHTML = svg;
                    const renderedSvg = container.querySelector("svg");
                    if (renderedSvg) {
                      renderedSvg.style.maxWidth = "100%";
                      renderedSvg.style.height = "auto";
                    }
                    bindFunctions?.(container);
                    pre.replaceWith(container);
                  }).catch(error => {
                    console.warn("Could not render Mermaid diagram:", error);
                  });
                }
              });
            })();
          </script>
          """,
          epub: ""
        }
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
      extra_applications: [:crypto, :logger]
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
      # For OffBroadway.Iceberg.Checkpoint.GCS only.
      {:req, "~> 0.7.5", optional: true},
      {:goth, "~> 1.4", optional: true},
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
