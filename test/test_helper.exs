Logger.configure(level: :info)

Application.put_env(:junit_formatter, :report_dir, "test/reports")
Application.put_env(:junit_formatter, :report_file, "junit.xml")
Application.put_env(:junit_formatter, :automatic_create_dir?, true)

# Integration tests need Docker and download DuckDB. Run them with
# `mix test --include integration`, or `mix test --only integration`.
ExUnit.start(exclude: [:integration], formatters: [JUnitFormatter, ExUnit.CLIFormatter])
