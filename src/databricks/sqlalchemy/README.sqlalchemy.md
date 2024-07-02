## Databricks dialect for SQLALchemy 2.0

The Databricks dialect for SQLAlchemy serves as bridge between [SQLAlchemy](https://www.sqlalchemy.org/) and the Databricks SQL Python driver. The dialect is included with `databricks-sql-connector==3.0.0` and above. A working example demonstrating usage can be found in `examples/sqlalchemy.py`.

## Usage with SQLAlchemy <= 2.0
A SQLAlchemy 1.4 compatible dialect was first released in connector [version 2.4](https://github.com/databricks/databricks-sql-python/releases/tag/v2.4.0). Support for SQLAlchemy 1.4 was dropped from the dialect as part of `databricks-sql-connector==3.0.0`. To continue using the dialect with SQLAlchemy 1.x, you can use `databricks-sql-connector^2.4.0`.


## Installation

To install the dialect and its dependencies:

```shell
pip install databricks-sql-connector[sqlalchemy]
```

If you also plan to use `alembic` you can alternatively run:

```shell
pip install databricks-sql-connector[alembic]
```

## Connection String

Every SQLAlchemy application that connects to a database needs to use an [Engine](https://docs.sqlalchemy.org/en/20/tutorial/engine.html#tutorial-engine), which you can create by passing a connection string to `create_engine`. The connection string must include these components:

1. Host
2. HTTP Path for a compute resource
3. API access token
4. Initial catalog for the connection
5. Initial schema for the connection

**Note: Our dialect is built and tested on workspaces with Unity Catalog enabled. Support for the `hive_metastore` catalog is untested.**

For example:

```python
import os
from sqlalchemy import create_engine

host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")
catalog = os.getenv("DATABRICKS_CATALOG")
schema = os.getenv("DATABRICKS_SCHEMA")

engine = create_engine(
    f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
    )
```

## Types

The [SQLAlchemy type hierarchy](https://docs.sqlalchemy.org/en/20/core/type_basics.html) contains backend-agnostic type implementations (represented in CamelCase) and backend-specific types (represented in UPPERCASE). The majority of SQLAlchemy's [CamelCase](https://docs.sqlalchemy.org/en/20/core/type_basics.html#the-camelcase-datatypes) types are supported. This means that a SQLAlchemy application using these types should "just work" with Databricks.

|SQLAlchemy Type|Databricks SQL Type|
|-|-|
[`BigInteger`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.BigInteger)| [`BIGINT`](https://docs.databricks.com/en/sql/language-manual/data-types/bigint-type.html)
[`LargeBinary`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.LargeBinary)| (not supported)|
[`Boolean`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Boolean)| [`BOOLEAN`](https://docs.databricks.com/en/sql/language-manual/data-types/boolean-type.html)
[`Date`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Date)| [`DATE`](https://docs.databricks.com/en/sql/language-manual/data-types/date-type.html)
[`DateTime`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.DateTime)| [`TIMESTAMP_NTZ`](https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html)|
[`Double`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Double)| [`DOUBLE`](https://docs.databricks.com/en/sql/language-manual/data-types/double-type.html)
[`Enum`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Enum)| (not supported)|
[`Float`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Float)| [`FLOAT`](https://docs.databricks.com/en/sql/language-manual/data-types/float-type.html)
[`Integer`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Integer)| [`INT`](https://docs.databricks.com/en/sql/language-manual/data-types/int-type.html)
[`Numeric`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Numeric)| [`DECIMAL`](https://docs.databricks.com/en/sql/language-manual/data-types/decimal-type.html)|
[`PickleType`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.PickleType)| (not supported)|
[`SmallInteger`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.SmallInteger)| [`SMALLINT`](https://docs.databricks.com/en/sql/language-manual/data-types/smallint-type.html)
[`String`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.String)| [`STRING`](https://docs.databricks.com/en/sql/language-manual/data-types/string-type.html)|
[`Text`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Text)| [`STRING`](https://docs.databricks.com/en/sql/language-manual/data-types/string-type.html)|
[`Time`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Time)| [`STRING`](https://docs.databricks.com/en/sql/language-manual/data-types/string-type.html)|
[`Unicode`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Unicode)| [`STRING`](https://docs.databricks.com/en/sql/language-manual/data-types/string-type.html)|
[`UnicodeText`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.UnicodeText)| [`STRING`](https://docs.databricks.com/en/sql/language-manual/data-types/string-type.html)|
[`Uuid`](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Uuid)| [`STRING`](https://docs.databricks.com/en/sql/language-manual/data-types/string-type.html)

In addition, the dialect exposes three UPPERCASE SQLAlchemy types which are specific to Databricks:

- [`databricks.sqlalchemy.TINYINT`](https://docs.databricks.com/en/sql/language-manual/data-types/tinyint-type.html)
- [`databricks.sqlalchemy.TIMESTAMP`](https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-type.html)
- [`databricks.sqlalchemy.TIMESTAMP_NTZ`](https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html)


### `LargeBinary()` and `PickleType()`

Databricks Runtime doesn't currently support binding of binary values in SQL queries, which is a pre-requisite for this functionality in SQLAlchemy.

## `Enum()` and `CHECK` constraints

Support for `CHECK` constraints is not implemented in this dialect. Support is planned for a future release.

SQLAlchemy's `Enum()` type depends on `CHECK` constraints and is therefore not yet supported.

### `DateTime()`, `TIMESTAMP_NTZ()`, and `TIMESTAMP()`

Databricks Runtime provides two datetime-like types: `TIMESTAMP` which is always timezone-aware and `TIMESTAMP_NTZ` which is timezone agnostic. Both types can be imported from `databricks.sqlalchemy` and used in your models.

The SQLAlchemy documentation indicates that `DateTime()` is not timezone-aware by default. So our dialect maps this type to `TIMESTAMP_NTZ()`. In practice, you should never need to use `TIMESTAMP_NTZ()` directly. Just use `DateTime()`.

If you need your field to be timezone-aware, you can import `TIMESTAMP()` and use it instead.

_Note that SQLAlchemy documentation suggests that you can declare a `DateTime()` with `timezone=True` on supported backends. However, if you do this with the Databricks dialect, the `timezone` argument will be ignored._

```python
from sqlalchemy import DateTime
from databricks.sqlalchemy import TIMESTAMP

class SomeModel(Base):
    some_date_without_timezone  = DateTime()
    some_date_with_timezone     = TIMESTAMP()
```

### `String()`, `Text()`, `Unicode()`, and `UnicodeText()`

Databricks Runtime doesn't support length limitations for `STRING` fields. Therefore `String()` or `String(1)` or `String(255)` will all produce identical DDL. Since `Text()`, `Unicode()`, `UnicodeText()` all use the same underlying type in Databricks SQL, they will generate equivalent DDL.

### `Time()`

Databricks Runtime doesn't have a native time-like data type. To implement this type in SQLAlchemy, our dialect stores SQLAlchemy `Time()` values in a `STRING` field. Unlike `DateTime` above, this type can optionally support timezone awareness (since the dialect is in complete control of the strings that we write to the Delta table).

```python
from sqlalchemy import Time

class SomeModel(Base):
    time_tz     = Time(timezone=True)
    time_ntz    = Time()
```


# Usage Notes

## `Identity()` and `autoincrement`

Identity and generated value support is currently limited in this dialect.

When defining models, SQLAlchemy types can accept an [`autoincrement`](https://docs.sqlalchemy.org/en/20/core/metadata.html#sqlalchemy.schema.Column.params.autoincrement) argument. In our dialect, this argument is currently ignored. To create an auto-incrementing field in your model you can pass in an explicit [`Identity()`](https://docs.sqlalchemy.org/en/20/core/defaults.html#identity-ddl) instead.

Furthermore, in Databricks Runtime, only `BIGINT` fields can be configured to auto-increment. So in SQLAlchemy, you must use the `BigInteger()` type.

```python
from sqlalchemy import Identity, String

class SomeModel(Base):
    id      = BigInteger(Identity())
    value   = String()
```

When calling `Base.metadata.create_all()`, the executed DDL will include `GENERATED ALWAYS AS IDENTITY` for the `id` column. This is useful when using SQLAlchemy to generate tables. However, as of this writing, `Identity()` constructs are not captured when SQLAlchemy reflects a table's metadata (support for this is planned).

## Parameters

`databricks-sql-connector` supports two approaches to parameterizing SQL queries: native and inline. Our SQLAlchemy 2.0 dialect always uses the native approach and is therefore limited to DBR 14.2 and above. If you are writing parameterized queries to be executed by SQLAlchemy, you must use the "named" paramstyle (`:param`). Read more about parameterization in `docs/parameters.md`.

## Usage with pandas

Use [`pandas.DataFrame.to_sql`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html) and [`pandas.read_sql`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html#pandas.read_sql) to write and read from Databricks SQL. These methods both accept a SQLAlchemy connection to interact with Databricks.

### Read from Databricks SQL into pandas
```python
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("databricks://token:dapi***@***.cloud.databricks.com?http_path=***&catalog=main&schema=test")
with engine.connect() as conn:
    # This will read the contents of `main.test.some_table`
    df = pd.read_sql("some_table", conn)
```

### Write to Databricks SQL from pandas

```python
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("databricks://token:dapi***@***.cloud.databricks.com?http_path=***&catalog=main&schema=test")
squares = [(i, i * i) for i in range(100)]
df = pd.DataFrame(data=squares,columns=['x','x_squared'])

with engine.connect() as conn:
    # This will write the contents of `df` to `main.test.squares`
    df.to_sql('squares',conn)
```

## [`PrimaryKey()`](https://docs.sqlalchemy.org/en/20/core/constraints.html#sqlalchemy.schema.PrimaryKeyConstraint) and [`ForeignKey()`](https://docs.sqlalchemy.org/en/20/core/constraints.html#defining-foreign-keys)

Unity Catalog workspaces in Databricks support PRIMARY KEY and FOREIGN KEY constraints. _Note that Databricks Runtime does not enforce the integrity of FOREIGN KEY constraints_. You can establish a primary key by setting `primary_key=True` when defining a column.

When building `ForeignKey` or `ForeignKeyConstraint` objects, you must specify a `name` for the constraint.

If your model definition requires a self-referential FOREIGN KEY constraint, you must include `use_alter=True` when defining the relationship.

```python
from sqlalchemy import Table, Column, ForeignKey, BigInteger, String

users = Table(
    "users",
    metadata_obj,
    Column("id", BigInteger, primary_key=True),
    Column("name", String(), nullable=False),
    Column("email", String()),
    Column("manager_id", ForeignKey("users.id", name="fk_users_manager_id_x_users_id", use_alter=True))
)
```
