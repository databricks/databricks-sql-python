# Contributing

To contribute to this repository, fork it and send pull requests.

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
```

There are several e2e test suites available:
- `PySQLCoreTestSuite`
- `PySQLLargeQueriesSuite`
- `PySQLRetryTestSuite.HTTP503Suite` **[not documented]**
- `PySQLRetryTestSuite.HTTP429Suite` **[not documented]**
- `PySQLUnityCatalogTestSuite` **[not documented]**

To execute the core test suite:

```bash
poetry run python -m pytest tests/e2e/driver_tests.py::PySQLCoreTestSuite
```

The suites marked `[not documented]` require additional configuration which will be documented at a later time.
### Code formatting

This project uses [Black](https://pypi.org/project/black/).

```
# Remove the --check flag write changes to files
poetry run python3 -m black src --check
```

To simplify reviews you can format your changes in a separate commit.
## Pull Request Process

1. Update the [CHANGELOG.md](README.md) or similar documentation with details of changes you wish to make, if applicable.
2. Add any appropriate tests.
3. Make your code or other changes.
4. Review guidelines such as
   [How to write the perfect pull request][github-perfect-pr], thanks!

[github-perfect-pr]: https://blog.github.com/2015-01-21-how-to-write-the-perfect-pull-request/
