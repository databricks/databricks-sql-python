# Contributing

To contribute to this repository, fork it and send pull requests.

## Set up your environment

This project uses [Poetry](https://python-poetry.org/) for dependency management, tests, and linting.

1. Clone this respository
2. Run `poetry install` 

### Unit Tests

We use [Pytest](https://docs.pytest.org/en/7.1.x/) as our test runner. Invoke it with `poetry run pytest`, all other arguments are passed directly to `pytest`.

#### All tests
```bash
poetry run pytest tests
```

#### Only a specific test file

```bash
poetry run pytest tests/tests.py
```

#### Only a specific method

```bash
poetry run pytest tests/tests.py::ClientTestSuite::test_closing_connection_closes_commands
```

### Code formatting

This project uses [Black](https://pypi.org/project/black/).

```
poetry run black src
```
## Pull Request Process

1. Update the [CHANGELOG.md](README.md) or similar documentation with details of changes you wish to make, if applicable.
2. Add any appropriate tests.
3. Make your code or other changes.
4. Review guidelines such as
   [How to write the perfect pull request][github-perfect-pr], thanks!

[github-perfect-pr]: https://blog.github.com/2015-01-21-how-to-write-the-perfect-pull-request/
