# Release History

## 2.1.x (Unreleased)

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
- Added Unity Catalog support (pass catalog and / or  schema key word args to the .connect method to select initial schema and catalog)

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
