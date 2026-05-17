# Reference image for running Fyrnheim's analytics MCP server.
#
# Build this from a Fyrnheim checkout for local evaluation:
#   docker build -f examples/mcp-container/Dockerfile -t fyrnheim-mcp:local .
#
# Downstream projects can either copy this Dockerfile or use it as a template
# and pin Fyrnheim to a release/tag/commit in their own dependency files.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Fyrnheim with MCP + DuckDB + ClickHouse extras. DuckDB supports
# local smoke tests; ClickHouse is the expected warehouse path for teams like
# Propel Data. Add other extras (for example bigquery) in a downstream image if
# needed.
COPY pyproject.toml README.md ./
COPY src ./src
RUN uv pip install --system --no-cache-dir '.[mcp,duckdb,clickhouse]'

# Projects should be mounted at runtime rather than baked with secrets.
ENV FYRNHEIM_PROJECT=/project \
    FYRNHEIM_ENTITIES_DIR=/project/entities \
    FYRNHEIM_CONFIG=/project/fyrnheim.yaml

ENTRYPOINT ["fyr-mcp-analytics"]
CMD ["--entities-dir", "/project/entities", "--project-path", "/project", "--config", "/project/fyrnheim.yaml"]
