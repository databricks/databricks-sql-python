# Contributing

To contribute to this repository, fork it and send pull requests.

## Set up your environment

This project uses [Poetry](https://python-poetry.org/) for dependency management, tests, and linting.

1. Clone this respository
2. Run `poetry install` 

### Unit Tests

#### All tests
```bash
poetry run pytest tests
```

#### Specific tests

```bash
poetry run pytest tests/path/to/test.py
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