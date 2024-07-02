## SQLAlchemy Dialect Compliance Test Suite with Databricks

The contents of the `test/` directory follow the SQLAlchemy developers' [guidance] for running the reusable dialect compliance test suite. Since not every test in the suite is applicable to every dialect, two options are provided to skip tests:

- Any test can be skipped by subclassing its parent class, re-declaring the test-case and adding a `pytest.mark.skip` directive.
- Any test that is decorated with a `@requires` decorator can be skipped by marking the indicated requirement as `.closed()` in `requirements.py`

We prefer to skip test cases directly with the first method wherever possible. We only mark requirements as `closed()` if there is no easier option to avoid a test failure. This principally occurs in test cases where the same test in the suite is parametrized, and some parameter combinations are conditionally skipped depending on `requirements.py`. If we skip the entire test method, then we skip _all_ permutations, not just the combinations we don't support.

## Regression, Unsupported, and Future test cases

We maintain three files of test cases that we import from the SQLAlchemy source code:

* **`_regression.py`** contains all the tests cases with tests that we expect to pass for our dialect. Each one is marked with `pytest.mark.reiewed` to indicate that we've evaluated it for relevance. This file only contains base class declarations.
* **`_unsupported.py`** contains test cases that fail because of missing features in Databricks. We mark them as skipped with a `SkipReason` enumeration. If Databricks comes to support these features, those test or entire classes can be moved to `_regression.py`.
* **`_future.py`** contains test cases that fail because of missing features in the dialect itself, but which _are_ supported by Databricks generally. We mark them as skipped with a `FutureFeature` enumeration. These are features that have not been prioritised or that do not violate our acceptance criteria. All of these test cases will eventually move to either `_regression.py`.

In some cases, only certain tests in class should be skipped with a `SkipReason` or `FutureFeature` justification. In those cases, we import the class into `_regression.py`, then import it from there into one or both of `_future.py` and `_unsupported.py`. If a class needs to be "touched" by regression, unsupported, and future, the class will be imported in that order. If an entire class should be skipped, then we do not import it into `_regression.py` at all.

We maintain `_extra.py` with test cases that depend on SQLAlchemy's reusable dialect test fixtures but which are specific to Databricks (e.g TinyIntegerTest).

## Running the reusable dialect tests

```
poetry shell
cd src/databricks/sqlalchemy/test
python -m pytest test_suite.py --dburi \
  "databricks://token:$access_token@$host?http_path=$http_path&catalog=$catalog&schema=$schema"
```

Whatever schema you pass in the `dburi` argument should be empty. Some tests also require the presence of an empty schema named `test_schema`. Note that we plan to implement our own `provision.py` which SQLAlchemy can automatically use to create an empty schema for testing. But for now this is a manual process.

You can run only reviewed tests by appending `-m "reviewed"` to the test runner invocation.

You can run only the unreviewed tests by appending `-m "not reviewed"` instead.

Note that because these tests depend on SQLAlchemy's custom pytest plugin, they are not discoverable by IDE-based test runners like VSCode or PyCharm and must be invoked from a CLI.

## Running local unit and e2e tests

Apart from the SQLAlchemy reusable suite, we maintain our own unit and e2e tests under the `test_local/` directory. These can be invoked from a VSCode or Pycharm since they don't depend on a custom pytest plugin. Due to pytest's lookup order, the `pytest.ini` which is required for running the reusable dialect tests, also conflicts with VSCode and Pycharm's default pytest implementation and overrides the settings in `pyproject.toml`. So to run these tests, you can delete or rename `pytest.ini`.


[guidance]: "https://github.com/sqlalchemy/sqlalchemy/blob/rel_2_0_22/README.dialects.rst"
