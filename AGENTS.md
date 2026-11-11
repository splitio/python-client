# AGENTS.md

## Project Overview

Split.io Python SDK — a feature flag evaluation client for the [Split platform](https://www.split.io/). Serves feature flag treatments to users and supports multiple runtime modes: standard (synchronous), asyncio (async), Redis-backed, uWSGI, and Kerberos-authenticated deployments.

- GitHub: https://github.com/splitio/python-client
- Compatible with Python 3.7+
- Published to PyPI as `splitio_client`
- **Release candidate versions**: `X.Y.ZrcN` (for example `10.7.0rc1`). This is the PEP 440 form this package publishes. `X.Y.Z-rcN` and `X.Y.Z.rcN` normalize to the same version, but write the version without a separator.

## Prerequisites

- **Python**: 3.7+ is supported (3.7 to latest stable). Develop on 3.12 (see `.python-version`); CI runs 3.7.16 and latest stable.
- **Redis**: local `redis-server` on `localhost:6379` for the Redis-backed tests.
- **Kerberos extra**: only needed for Kerberos proxy auth, but installed by the test extras. Builds `gssapi` from source: on Debian/Ubuntu install `libkrb5-dev` + `build-essential` first; macOS needs nothing extra (`brew install krb5` only if the build fails).
- **Optional**: a virtual environment (`python -m venv .venv` or uv) and pyenv/uv to switch Python versions.

## Build System

- **Build tool**: setuptools / pip
- **Install (dev + test deps)**: `pip install -e ".[test]"`
- **Install with extras**: `pip install -e ".[redis,asyncio,kerberos]"`

### Optional: uv

[uv](https://docs.astral.sh/uv/) is optional; plain `pip`/`pytest` keep working. The repo has no `pyproject.toml`
or `uv.lock`, so don't add them (that would make uv mandatory).

- **Setup uv (one time)**: `brew install uv`, then in the repo root:
  `uv venv --seed --python 3.12` (`--seed` adds `pip` to `.venv`) and
  `uv pip install -e ".[test,redis,asyncio,kerberos]"`
- **Using uv**: prefix commands with `uv run` to use `.venv` without activating it:
  `uv run pytest`, `uv run flake8`. Re-run the `uv pip install` command when `setup.py` dependencies change.

## Testing

- **Run all tests**: `pytest`
- **Coverage**: enabled by default — `setup.cfg` addopts add `--verbose --cov=splitio --cov-report xml` (writes `coverage.xml`)
- **Run a specific test file**: `pytest tests/client/test_client.py`
- **Run a specific test case**: `pytest tests/client/test_client.py::ClientTests::test_get_treatment`
- **Run async tests**: `pytest tests/ -k "async"`
- **Test file pattern**: `tests/<module>/test_<name>.py`, classes named `*Tests`
- **Redis tests**: `tests/integration/test_redis_integration.py` and other Redis-backed tests need a local `redis-server` on `localhost:6379`
- **No real network in tests**: `tests/conftest.py` fails any test whose thread resolves a `*.split.io` host (message: "test contacted the Split backend"), and names the test that created the thread. Mock the HTTP layer (see the `_offline` fixture in `tests/client/test_factory.py`). Patches only last for the test: a factory's background sync threads can outlive them and hit the real backend during a later test, so the error shows up in an unrelated test. Read the "created in <test>" part of the message to find the origin; it is timing-dependent and may not reproduce on every run.
- **Full suite takes about 6 minutes** and needs `redis-server` running. Run it in the background and, if a test errors only in the full run, rerun it alone to tell flaky leaks from real failures.
- **CI**: Harness pipeline (`.harness/orgs/PROD/projects/Harness_Split/pipelines/pythonclient_hc.yaml`) installs with `pip install -e ".[test,redis,asyncio,kerberos]"`, starts `redis-server`, runs `python -m pytest` on Python 3.7.16 and latest stable, and reports coverage (`coverage.xml`)

## Linting & Formatting

- **Lint check**: `flake8` from the repo root (`setup.cfg` restricts it to `splitio/` and `tests/` via `filename`, so it skips `.venv`, `build`, `dist`). Always run it bare or with a `./` prefix: `filename` patterns only match `./...` paths, so `flake8 splitio/` silently reports nothing (e.g. `flake8 ./splitio/client`).
- **Max line length**: 100 characters is the target, but `E501` (line too long) is ignored in `setup.cfg` (`extend-ignore`), so flake8 does not enforce it
- **Tests are linted too.** Never leave duplicate test names (F811): a later definition silently shadows the earlier one so it never runs. Remove identical copies; rename different ones (e.g. `*_async` for async variants, `*AsyncTests` for classes). The large JSON fixtures in `tests/integration/__init__.py` carry `# noqa: F601`.
- Run flake8 before committing to catch style issues early.
- **Config lives in `setup.cfg`** (`[flake8]`), which the CLI and IDE extensions read. To relax a rule use `extend-ignore`, never `ignore`: `ignore` replaces flake8's default ignore list and re-enables checks that are off by default.
- **Fixing style issues in bulk**: `pip install autopep8`, then `autopep8 --ignore-local-config --in-place --recursive --max-line-length 100 --ignore E501 splitio/ tests/` (`--ignore-local-config` is needed or autopep8 reads `setup.cfg` and may skip paths) for whitespace, blank-line, indentation and `== None` issues (add `--aggressive` for E711/E712 and review that diff). Fix F-codes and anything behavior-related by hand, then rerun `flake8` and `pytest`. Keep mechanical reformatting in its own commit, separate from functional changes.
- **Pylint is not used.** Do not add `# pylint: disable=...` comments; flake8 ignores them and nothing enforces them.
- **Intentional re-exports** (the compatibility shims `splitio/__init__.py`, `exceptions.py`, `factories.py`, `impressions.py`, `key.py`): declare `__all__` instead of `# noqa: F401`. This also documents the public API. Do not remove these modules or names; users import them.
- **Unused imports**: remove them. If a test patches a name through another module (e.g. `splitio.client.factory.X`), patch it where it is defined (`splitio.tasks.impressions_sync.X`) and drop the import, rather than keeping an import only for the test.

## Git Workflow

- **Default branch**: `main`
- **Development branch**: `development` (integration branch; target of feature and refactor PRs)
- **Branch naming**: `short-description` or `ticket-short-description`
- **Commit format**: Free-form, descriptive (e.g., `"fixed typo for segment event type"`, `"added support for rule-based segments"`)
- **PR title format (required)**: PR title must follow the expected format: `<type>: [PROJECT-123]: <Description>`
  - Example: `feat: [BT-100]: Add new dashboard endpoint`
  - Valid types: `feat`, `fix`, `techdebt`, `hotfixpreqa`, `chore`
- **Branching model (GitFlow)**:
  - `main`: stable release history. Every stable release is tagged with its version, but not every commit on `main` necessarily maps to a release (e.g. chores on metadata or CI files have no tag).
  - `development`: integration branch for new features and ongoing development.
  - Other branches: features, refactors, hotfixes, and optional `release-x.x.x` (to prepare stable releases).
  - RC releases can be done from any branch and don't require a tag.
- **PR target branch**: Open PRs against `development`, including `release-x.x.x` branches; `development` is then merged into `main` for stable releases. Hotfixes may target `main` directly, bypassing `development`.
  The PR creation UI preselects `main`, so always set the target branch explicitly.
- **PR description**: No PR template in the repo — describe what you accomplished, how to test it, and any extra notes.

## DOs

- Always update `CHANGES.txt` when adding new features or fixing bugs. Follow the existing format: `version (date)\n- Description`.
- Run `flake8` before committing to check for style violations.
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
