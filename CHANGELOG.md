# Release History

# 3.6.0 (2024-10-25)

- Support encryption headers in the cloud fetch request (https://github.com/databricks/databricks-sql-python/pull/460 by @jackyhu-db)

# 3.5.0 (2024-10-18)

- Create a non pyarrow flow to handle small results for the column set (databricks/databricks-sql-python#440 by @jprakash-db)
- Fix: On non-retryable error, ensure PySQL includes useful information in error (databricks/databricks-sql-python#447 by @shivam2680)

# 3.4.0 (2024-08-27)

- Unpin pandas to support v2.2.2 (databricks/databricks-sql-python#416 by @kfollesdal)
- Make OAuth as the default authenticator if no authentication setting is provided (databricks/databricks-sql-python#419 by @jackyhu-db)
- Fix (regression): use SSL options with HTTPS connection pool (databricks/databricks-sql-python#425 by @kravets-levko)

# 3.3.0 (2024-07-18)

- Don't retry requests that fail with HTTP code 401 (databricks/databricks-sql-python#408 by @Hodnebo)
- Remove username/password (aka "basic") auth option (databricks/databricks-sql-python#409 by @jackyhu-db)
- Refactor CloudFetch handler to fix numerous issues with it (databricks/databricks-sql-python#405 by @kravets-levko)
- Add option to disable SSL verification for CloudFetch links (databricks/databricks-sql-python#414 by @kravets-levko)

Databricks-managed passwords reached end of life on July 10, 2024. Therefore, Basic auth support was removed from
the library. See https://docs.databricks.com/en/security/auth-authz/password-deprecation.html

The existing option `_tls_no_verify=True` of `sql.connect(...)` will now also disable SSL cert verification
(but not the SSL itself) for CloudFetch links. This option should be used as a workaround only, when other ways
to fix SSL certificate errors didn't work.

# 3.2.0 (2024-06-06)

- Update proxy authentication (databricks/databricks-sql-python#354 by @amir-haroun)
- Relax `pyarrow` pin (databricks/databricks-sql-python#389 by @dhirschfeld)
- Fix error logging in OAuth manager (databricks/databricks-sql-python#269 by @susodapop)
- SQLAlchemy: enable delta.feature.allowColumnDefaults for all tables (databricks/databricks-sql-python#343 by @dhirschfeld)
- Update `thrift` dependency (databricks/databricks-sql-python#397 by @m1n0)

# 3.1.2 (2024-04-18)

- Remove broken cookie code (#379)
- Small typing fixes (#382, #384 thanks @wyattscarpenter)

# 3.1.1 (2024-03-19)

- Don't retry requests that fail with code 403 (#373)
- Assume a default retry-after for 429/503 (#371)
- Fix boolean literals (#357)

# 3.1.0 (2024-02-16)

- Revert retry-after behavior to be exponential backoff (#349)
- Support Databricks OAuth on Azure (#351)
- Support Databricks OAuth on GCP (#338)

# 3.0.3 (2024-02-02)

- Revised docstrings and examples for OAuth (#339)
- Redact the URL query parameters from the urllib3.connectionpool logs (#341)

# 3.0.2 (2024-01-25)

- SQLAlchemy dialect now supports table and column comments (thanks @cbornet!)
- Fix: SQLAlchemy dialect now correctly reflects TINYINT types (thanks @TimTheinAtTabs!)
- Fix: `server_hostname` URIs that included `https://` would raise an exception
- Other: pinned to `pandas<=2.1` and `urllib3>=1.26` to avoid runtime errors in dbt-databricks (#330)

## 3.0.1 (2023-12-01)

- Other: updated docstring comment about default parameterization approach (#287)
- Other: added tests for reading complex types and revised docstrings and type hints (#293)
- Fix: SQLAlchemy dialect raised DeprecationWarning due to `dbapi` classmethod (#294)
- Fix: SQLAlchemy dialect could not reflect TIMESTAMP_NTZ columns (#296)

## 3.0.0 (2023-11-17)

- Remove support for Python 3.7
- Add support for native parameterized SQL queries. Requires DBR 14.2 and above. See docs/parameters.md for more info.
- Completely rewritten SQLAlchemy dialect
  - Adds support for SQLAlchemy >= 2.0 and drops support for SQLAlchemy 1.x
  - Full e2e test coverage of all supported features
  - Detailed usage notes in `README.sqlalchemy.md`
  - Adds support for:
    - New types: `TIME`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `TINYINT`
    - `Numeric` type scale and precision, like `Numeric(10,2)`
    - Reading and writing `PrimaryKeyConstraint` and `ForeignKeyConstraint`
    - Reading and writing composite keys
    - Reading and writing from views
    - Writing `Identity` to tables (i.e. autoincrementing primary keys)
    - `LIMIT` and `OFFSET` for paging through results
    - Caching metadata calls
- Enable cloud fetch by default. To disable, set `use_cloud_fetch=False` when building `databricks.sql.client`.
- Add integration tests for Databricks UC Volumes ingestion queries
- Retries:
  - Add `_retry_max_redirects` config
  - Set `_enable_v3_retries=True` and warn if users override it
- Security: bump minimum pyarrow version to 14.0.1 (CVE-2023-47248)

## 2.9.3 (2023-08-24)

- Fix: Connections failed when urllib3~=1.0.0 is installed (#206)

## 2.9.2 (2023-08-17)

**Note: this release was yanked from Pypi on 13 September 2023 due to compatibility issues with environments where `urllib3<=2.0.0` were installed. The log changes are incorporated into version 2.9.3 and greater.**

- Other: Add `examples/v3_retries_query_execute.py` (#199)
- Other: suppress log message when `_enable_v3_retries` is not `True` (#199)
- Other: make this connector backwards compatible with `urllib3>=1.0.0` (#197)

## 2.9.1 (2023-08-11)

**Note: this release was yanked from Pypi on 13 September 2023 due to compatibility issues with environments where `urllib3<=2.0.0` were installed.**

- Other: Explicitly pin urllib3 to ^2.0.0 (#191)

## 2.9.0 (2023-08-10)

- Replace retry handling with DatabricksRetryPolicy. This is disabled by default. To enable, set `_enable_v3_retries=True` when creating `databricks.sql.client` (#182)
- Other: Fix typo in README quick start example (#186)
- Other: Add autospec to Client mocks and tidy up `make_request` (#188)

## 2.8.0 (2023-07-21)

- Add support for Cloud Fetch. Disabled by default. Set `use_cloud_fetch=True` when building `databricks.sql.client` to enable it (#146, #151, #154)
- SQLAlchemy has_table function now honours schema= argument and adds catalog= argument (#174)
- SQLAlchemy set non_native_boolean_check_constraint False as it's not supported by Databricks (#120)
- Fix: Revised SQLAlchemy dialect and examples for compatibility with SQLAlchemy==1.3.x (#173)
- Fix: oauth would fail if expired credentials appeared in ~/.netrc (#122)
- Fix: Python HTTP proxies were broken after switch to urllib3 (#158)
- Other: remove unused import in SQLAlchemy dialect
- Other: Relax pandas dependency constraint to allow ^2.0.0 (#164)
- Other: Connector now logs operation handle guids as hexadecimal instead of bytes (#170)
- Other: test_socket_timeout_user_defined e2e test was broken (#144)

## 2.7.0 (2023-06-26)

- Fix: connector raised exception when calling close() on a closed Thrift session
- Improve e2e test development ergonomics
- Redact logged thrift responses by default
- Add support for OAuth on Databricks Azure

## 2.6.2 (2023-06-14)

- Fix: Retry GetOperationStatus requests for http errors

## 2.6.1 (2023-06-08)

- Fix: http.client would raise a BadStatusLine exception in some cases

## 2.6.0 (2023-06-07)

- Add support for HTTP 1.1 connections (connection pools)
- Add a default socket timeout for thrift RPCs

## 2.5.2 (2023-05-08)

- Fix: SQLAlchemy adapter could not reflect TIMESTAMP or DATETIME columns
- Other: Relax pandas and alembic dependency specifications

## 2.5.1 (2023-04-28)

- Other: Relax sqlalchemy required version as it was unecessarily strict.

## 2.5.0 (2023-04-14)

- Add support for External Auth providers
- Fix: Python HTTP proxies were broken
- Other: All Thrift requests that timeout during connection will be automatically retried

## 2.4.1 (2023-03-21)

- Less strict numpy and pyarrow dependencies
- Update examples in README to use security best practices
- Update docstring for client.execute() for clarity

## 2.4.0 (2023-02-21)

- Improve compatibility when installed alongside other Databricks namespace Python packages
- Add SQLAlchemy dialect

## 2.3.0 (2023-01-10)

- Support staging ingestion commands for DBR 12+

## 2.2.2 (2023-01-03)

- Support custom oauth client id and redirect port
- Fix: Add none check on \_oauth_persistence in DatabricksOAuthProvider

## 2.2.1 (2022-11-29)

- Add support for Python 3.11

## 2.2.0 (2022-11-15)

- Bump thrift version to address https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-13949
- Add support for lz4 compression

## 2.1.0 (2022-09-30)

- Introduce experimental OAuth support while Bring Your Own IDP is in Public Preview on AWS
- Add functional examples

## 2.0.5 (2022-08-23)

- Fix: closing a connection now closes any open cursors from that connection at the server
- Other: Add project links to pyproject.toml (helpful for visitors from PyPi)

## 2.0.4 (2022-08-17)

- Add support for Python 3.10
- Add unit test matrix for supported Python versions

Huge thanks to @dbaxa for contributing this change!

## 2.0.3 (2022-08-05)

- Add retry logic for `GetOperationStatus` requests that fail with an `OSError`
- Reorganised code to use Poetry for dependency management.

## 2.0.2 (2022-05-04)

- Better exception handling in automatic connection close

## 2.0.1 (2022-04-21)

- Fixed Pandas dependency in setup.cfg to be >= 1.2.0

## 2.0.0 (2022-04-19)

- Initial stable release of V2
- Added better support for complex types, so that in Databricks runtime 10.3+, Arrays, Maps and Structs will get
  deserialized as lists, lists of tuples and dicts, respectively.
- Changed the name of the metadata arg to http_headers

## 2.0.b2 (2022-04-04)

- Change import of collections.Iterable to collections.abc.Iterable to make the library compatible with Python 3.10
- Fixed bug with .tables method so that .tables works as expected with Unity-Catalog enabled endpoints

## 2.0.0b1 (2022-03-04)

- Fix packaging issue (dependencies were not being installed properly)
- Fetching timestamp results will now return aware instead of naive timestamps
- The client will now default to using simplified error messages

## 2.0.0b (2022-02-08)

- Initial beta release of V2. V2 is an internal re-write of large parts of the connector to use Databricks edge features. All public APIs from V1 remain.
- Added Unity Catalog support (pass catalog and / or schema key word args to the .connect method to select initial schema and catalog)

---

**Note**: The code for versions prior to `v2.0.0b` is not contained in this repository. The below entries are included for reference only.

---

## 1.0.0 (2022-01-20)

- Add operations for retrieving metadata
- Add the ability to access columns by name on result rows
- Add the ability to provide configuration settings on connect

## 0.9.4 (2022-01-10)

- Improved logging and error messages.

## 0.9.3 (2021-12-08)

- Add retries for 429 and 503 HTTP responses.

## 0.9.2 (2021-12-02)

- (Bug fix) Increased Thrift requirement from 0.10.0 to 0.13.0 as 0.10.0 was in fact incompatible
- (Bug fix) Fixed error message after query execution failed -SQLSTATE and Error message were misplaced

## 0.9.1 (2021-09-01)

- Public Preview release, Experimental tag removed
- minor updates in internal build/packaging
- no functional changes

## 0.9.0 (2021-08-04)

- initial (Experimental) release of pyhive-forked connector
- Python DBAPI 2.0 (PEP-0249), thrift based
- see docs for more info: https://docs.databricks.com/dev-tools/python-sql-connector.html
