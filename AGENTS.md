# AGENTS.md

## Project Overview

Split.io Python SDK — a feature flag evaluation client for the [Split platform](https://www.split.io/). Serves feature flag treatments to users and supports multiple runtime modes: standard (synchronous), asyncio (async), Redis-backed, uWSGI, and Kerberos-authenticated deployments.

- GitHub: https://github.com/splitio/python-client
- Compatible with Python 3.7+
- Published to PyPI as `splitio_client`

## Build System

- **Build tool**: setuptools / pip
- **Install (dev + test deps)**: `pip install -e ".[test]"`
- **Install with extras**: `pip install -e ".[redis,asyncio,kerberos]"`

## Testing

- **Run all tests**: `pytest`
- **Coverage**: enabled by default — `setup.cfg` addopts add `--verbose --cov=splitio --cov-report xml` (writes `coverage.xml`)
- **Run a specific test file**: `pytest tests/client/test_client.py`
- **Run a specific test case**: `pytest tests/client/test_client.py::ClientTests::test_get_treatment`
- **Run async tests**: `pytest tests/ -k "async"`
- **Test file pattern**: `tests/<module>/test_<name>.py`, classes named `*Tests`
- **Redis tests**: `tests/integration/test_redis_integration.py` and other Redis-backed tests need a local `redis-server` on `localhost:6379`
- **CI**: Harness pipeline (`.harness/orgs/PROD/projects/Harness_Split/pipelines/pythonclient.yaml`) installs with `pip install -e .`, starts `redis-server`, runs `python setup.py test` on Python 3.7.16, then runs SonarQube using `coverage.xml`

## Linting & Formatting

- **Lint check**: `flake8 splitio/`
- **Max line length**: 100 characters
- **Linting excludes**: `tests/`
- Run flake8 before committing to catch style issues early.

## Git Workflow

- **Default branch**: `main`
- **Development branch**: `development` (integration branch; target of all PRs)
- **Branch naming**: `short-description` or `ticket-short-description`
- **Commit format**: Free-form, descriptive (e.g., `"fixed typo for segment event type"`, `"added support for rule-based segments"`)
- **PR title format (required)**: PR title must follow the expected format: `<type>: [PROJECT-123]: <Description>`
  - Example: `feat: [BT-100]: Add new dashboard endpoint`
  - Valid types: `feat`, `fix`, `techdebt`, `hotfixpreqa`, `chore`
- **PR target branch**: Always open PRs against `development`, never `main`. `main` is the default
  branch and holds released code; it is only updated by merging `development` during a release.
  The PR creation UI preselects `main`, so set the target branch explicitly.
- **PR description**: No PR template in the repo — describe what you accomplished, how to test it, and any extra notes.

## DOs

- Always update `CHANGES.txt` when adding new features or fixing bugs. Follow the existing format: `version (date)\n- Description`.
- Run `flake8 splitio/` before committing to check for style violations.
- Run `pytest` before committing to verify tests pass.
- Match existing code style — synchronous and async variants of classes live side by side in the same module, async ones suffixed `Async` (e.g., `Client` / `ClientAsync` in `splitio/client/client.py`, `InMemorySplitStorage` / `InMemorySplitStorageAsync` in `splitio/storage/inmemmory.py`), often sharing a `*Base` class.
- When adding a new capability, check if both the sync and the `*Async` variant need updating, along with their tests.
- Import optional dependencies (`aiohttp`, `aiofiles`, etc.) through `splitio/optional/loaders.py` rather than directly, so the SDK still works without the `asyncio` extra.
- Follow the existing package structure: logic in `splitio/`, tests mirroring in `tests/`.

## DON'Ts

- Never force push to `main` or `development`.
- Never commit secrets, `.env` files, or API keys.
- Never skip flake8 or pytest when implementing changes.
- Never modify `.harness/` CI pipeline files unless specifically asked to update the pipeline.

## Commands to Never Run

- `git push --force origin main`
- `git push --force origin development`
- `rm -rf /` or any destructive recursive delete outside the repo
- `DROP DATABASE` / `DROP TABLE` (not applicable here but listed for completeness)

## Project Structure

```
python-client/
├── splitio/              # Main SDK package
│   ├── api/              # HTTP API clients (feature flags, segments, auth, telemetry)
│   ├── client/           # Public SDK client & factory (get_factory, Client, SplitFactory)
│   ├── engine/           # Feature flag evaluation engine (matchers, splitter, rules)
│   ├── events/           # Event tracking and delivery
│   ├── models/           # Data models (splits, segments, impressions, events)
│   ├── optional/         # Optional-dependency loaders (asyncio/aiohttp/aiofiles)
│   ├── push/             # Streaming / push notification support (SSE)
│   ├── recorder/         # Impression and event recorders
│   ├── storage/          # Storage backends (in-memory, Redis, pluggable); adapters/ holds Redis/cache adapters
│   ├── sync/             # Synchronization managers (polling, streaming)
│   ├── tasks/            # Background tasks (periodic sync, impression flush)
│   ├── util/             # Utilities (logging, decorators, etc.)
│   ├── factories.py      # Backwards-compat shim re-exporting get_factory from client/factory.py
│   ├── impressions.py    # Backwards-compat shim re-exporting ImpressionListener
│   ├── exceptions.py     # Re-exports public exceptions (TimeoutException, SentinelConfigurationException)
│   ├── spec.py           # SDK spec version (SPEC_VERSION)
│   └── version.py        # Package version (__version__), read by setup.py
├── tests/                # Test suite mirroring splitio/ structure
│   ├── integration/      # Integration tests
│   └── helpers/          # Test helpers and fixtures
├── setup.py              # Package metadata and dependencies
├── setup.cfg             # pytest, flake8, and wheel config
├── CHANGES.txt           # Changelog — update for every release
└── .harness/             # Harness CI pipeline + input sets (don't edit unless asked)
```

## Important Packages

- `splitio/client/` — Public API surface (`Client`/`ClientAsync`, `SplitFactory`/`SplitFactoryAsync`, `get_factory`, config, input validation). Entry point for most feature work.
- `splitio/engine/` — Core evaluation logic. Changes here affect all treatment results.
- `splitio/storage/` — Storage abstraction layer. Supports in-memory, Redis, and pluggable backends.
- `splitio/sync/` — Manages synchronization of feature flags and segments from the Split backend.
- `splitio/push/` — SSE-based streaming update support.
- `splitio/api/` — HTTP communication with Split backend services (including proxy support).

## Language-Specific Guidelines

This is a Python project. Use the `make-agent-friendly:python-conventions` skill for Python-specific patterns, testing conventions, and tooling guidance.
