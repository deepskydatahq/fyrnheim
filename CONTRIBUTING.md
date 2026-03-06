# Contributing to Fyrnheim

Thanks for your interest in contributing to Fyrnheim!

## Getting Started

```bash
git clone https://github.com/deepskydatahq/fyrnheim.git
cd fyrnheim
uv sync --all-extras
```

## Development

```bash
# Run tests
uv run python -m pytest

# Lint
uv run ruff check src/ tests/

# Type check
uv run mypy src/
```

## Submitting Changes

1. Fork the repo and create a feature branch from `main`
2. Write tests for any new functionality
3. Make sure all checks pass: tests, ruff, mypy
4. Open a pull request against `main`

## Pull Request Guidelines

- Keep PRs focused — one feature or fix per PR
- All tests must pass and lint must be clean before merge
- Write a clear description of what changed and why

## Reporting Issues

Open an issue on GitHub with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Python version and OS

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
