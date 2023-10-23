# Contributing Guide

We happily welcome contributions to the `databricks-sql-connector` package. We use [GitHub Issues](https://github.com/databricks/databricks-sql-python/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/databricks-sql-python/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication
Before starting work on a major feature, please reach out to us via GitHub, Slack, email, etc. We will make sure no one else is already working on it and ask you to open a GitHub issue.
A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.
We will use the GitHub issue to discuss the feature and come to agreement.
This is to prevent your time being wasted, as well as ours.
The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.
If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.
Specifically, if the goal is to add a new extension, please read the extension policy.
Small patches and bug fixes don't need prior communication.

## Coding Style
We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with one exception: lines can be up to 100 characters in length, not 79.

## Sign your work
The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch. The rules are pretty simple: if you can certify the below (from developercertificate.org):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Then you just add a line to every git commit message:

```
Signed-off-by: Joe Smith <joe.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`.

## Set up your environment

This project uses [Poetry](https://python-poetry.org/) for dependency management, tests, and linting.

1. Clone this respository
2. Run `poetry install` 

### Run tests

We use [Pytest](https://docs.pytest.org/en/7.1.x/) as our test runner. Invoke it with `poetry run python -m pytest`, all other arguments are passed directly to `pytest`.

#### Unit tests

Unit tests do not require a Databricks account.

```bash
poetry run python -m pytest tests/unit
```
#### Only a specific test file

```bash
poetry run python -m pytest tests/unit/tests.py
```

#### Only a specific method

```bash
poetry run python -m pytest tests/unit/tests.py::ClientTestSuite::test_closing_connection_closes_commands
```

#### e2e Tests

End-to-end tests require a Databricks account. Before you can run them, you must set connection details for a Databricks SQL endpoint in your environment:

```bash
export host=""
export http_path=""
export access_token=""
export catalog=""
export schema=""
```

Or you can write these into a file called `test.env` in the root of the repository:

```
host="****.cloud.databricks.com"
http_path="/sql/1.0/warehouses/***"
access_token="dapi***"
staging_ingestion_user="***@example.com"
```

To see logging output from pytest while running tests, set `log_cli = "true"` under `tool.pytest.ini_options` in `pyproject.toml`. You can also set `log_cli_level` to any of the default Python log levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

There are several e2e test suites available:
- `PySQLCoreTestSuite`
- `PySQLLargeQueriesSuite`
- `PySQLStagingIngestionTestSuite`
- `PySQLRetryTestSuite.HTTP503Suite` **[not documented]**
- `PySQLRetryTestSuite.HTTP429Suite` **[not documented]**
- `PySQLUnityCatalogTestSuite` **[not documented]**

To execute the core test suite:

```bash
poetry run python -m pytest tests/e2e/driver_tests.py::PySQLCoreTestSuite
```

The `PySQLCoreTestSuite` namespace contains tests for all of the connector's basic features and behaviours. This is the default namespace where tests should be written unless they require specially configured clusters or take an especially long-time to execute by design.

The `PySQLLargeQueriesSuite` namespace contains long-running query tests and is kept separate. In general, if the `PySQLCoreTestSuite` passes then these tests will as well.

The `PySQLStagingIngestionTestSuite` namespace requires a cluster running DBR version > 12.x which supports staging ingestion commands.

The suites marked `[not documented]` require additional configuration which will be documented at a later time.

#### SQLAlchemy dialect tests

See README.tests.md for details.

### Code formatting

This project uses [Black](https://pypi.org/project/black/).

```
poetry run python3 -m black src --check
```

Remove the `--check` flag to write reformatted files to disk.

To simplify reviews you can format your changes in a separate commit.

### Change a pinned dependency version

Modify the dependency specification (syntax can be found [here](https://python-poetry.org/docs/dependency-specification/)) in `pyproject.toml` and run one of the following in your terminal:

- `poetry update`
- `rm poetry.lock && poetry install`

Sometimes `poetry update` can freeze or run forever. Deleting the `poetry.lock` file and calling `poetry install` is guaranteed to update everything but is usually _slower_ than `poetry update` **if `poetry update` works at all**. 

