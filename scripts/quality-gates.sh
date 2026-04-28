#!/usr/bin/env bash
set -euo pipefail

run_with_uv() {
  echo "==> uv run pytest"
  uv run pytest
  echo "==> uv run ruff check src/ tests/"
  uv run ruff check src/ tests/
  echo "==> uv run mypy src/"
  uv run mypy src/
}

run_with_venv() {
  echo "==> .venv/bin/python -m pytest"
  .venv/bin/python -m pytest
  echo "==> .venv/bin/ruff check src/ tests/"
  .venv/bin/ruff check src/ tests/
  echo "==> .venv/bin/mypy src/"
  .venv/bin/mypy src/
}

run_with_path() {
  echo "==> python -m pytest"
  python -m pytest
  echo "==> ruff check src/ tests/"
  ruff check src/ tests/
  echo "==> mypy src/"
  mypy src/
}

if command -v uv >/dev/null 2>&1; then
  run_with_uv
elif [[ -x .venv/bin/python && -x .venv/bin/ruff && -x .venv/bin/mypy ]]; then
  run_with_venv
else
  run_with_path
fi
