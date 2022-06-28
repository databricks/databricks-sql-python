# Release History

## 2.x.x (Unreleased)

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

**Note**: The code for versions prior to `v2.0.0b` is not contained in this repository.
