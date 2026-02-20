# Contributing Guide

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Development Workflow

1. Create a branch from `main`.
2. Implement your change and add tests.
3. Run `pytest` before pushing.
4. Open a pull request with a clear description.

## Pull Request Checklist

- [ ] Includes tests for new behavior or bug fixes.
- [ ] Does not break existing tests.
- [ ] Updates documentation if public API behavior changes.

## Code Style

- Prefer clear, readable code.
- Keep pull requests focused; avoid unrelated refactors.
