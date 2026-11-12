.PHONY: all deps setup compile shell test integration
.PHONY: clean publish

## Targets
##=========================================================================

all: setup deps compile

setup:
	mise install

deps:
	mix deps.get

compile: deps setup
	mix compile

test:
	mix test

# Needs Docker. Starts and stops the Iceberg REST catalog in docker-compose.yml.
integration:
	mix test --only integration

shell:
	iex -S mix

clean:
	mix clean

publish: deps
	mix local.hex --force
	mix hex.publish --yes
