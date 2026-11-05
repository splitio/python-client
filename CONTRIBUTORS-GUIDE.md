# Contributing to the Split Python SDK

Split SDK is an open source project and we welcome feedback and contribution. The information below describes how to build the project with your changes, run the tests, and send the Pull Request(PR).

## Development

### Prerequisites

- **Python**: 3.7 or newer. Python 3.12 is recommended for day-to-day development; the repo's `.python-version` points to it for pyenv/uv users.
- **Redis**: a local `redis-server` listening on `localhost:6379`, required by the Redis tests.
- **Kerberos extra** (`pip install -e ".[kerberos]"`): only needed for Kerberos proxy authentication, and installed by the test extras since the tests cover it. It builds `gssapi` from source, so on Debian/Ubuntu install `libkrb5-dev` and `build-essential` first. macOS ships Kerberos, so nothing extra is usually needed (`brew install krb5` only if the build fails).
- **Optional**: a virtual environment (`python -m venv .venv`, or [uv](https://docs.astral.sh/uv/)) to keep dependencies isolated, and pyenv or uv to install other Python versions.

### Development process

1. Fork the repository and create a topic branch from `development` branch (we follow GitFlow: `main` holds the stable release history and `development` is the integration branch). Please use a descriptive name for your branch.
2. While developing, use descriptive messages in your commits. Avoid short or meaningless sentences like "fix bug".
3. Make sure to add tests for both positive and negative cases.
4. <if applicable> Run the linter script of the project and fix any issues you find.
5. Run the build script and make sure it runs with no errors.
6. Run all tests and make sure there are no failures.
7. `git push` your changes to GitHub within your topic branch.
8. Open a Pull Request(PR) from your forked repo and into the `development` branch of the original repository.
9. When creating your PR, please describe what you accomplished, how to test your changes, and any extra notes reviewers should know about.
10. Check for conflicts once the pull request is created to make sure your PR can be merged cleanly into `development`.
11. Keep an eye out for any feedback or comments from Split's SDK team.

### Running tests

To run the tests, from the repository root:
1. `pip install -U pip setuptools`
2. `pip install -e ".[test,redis,asyncio,kerberos]"`
3. Start Redis if it isn't running: `redis-server --daemonize yes`
4. `python -m pytest`

### Linting and other useful checks

To lint, run `flake8 splitio/`.

# Contact

If you have any other questions or need to contact us directly in a private manner send us a note at sdks@split.io.
