# Introduction

This is work-in-progress of a SQLAlchemy dialect for Databricks.

The dialect is embedded within the Databricks SQL Connector.

## Connection String

Using the dialect requires the following:

1. SQL Warehouse hostname
2. Endpoint
3. Access token

The schema `default` is used unless an alternate is specified via _Default-schema_.

The connection string is constructed as follows:

`databricks+thrift://token:`_Access-token_`@`_SQL-warehouse-hostname_`/`_Default-schema_`?http_path=`_Endpoint_


## Data Types

|Databricks type| SQLAlchemy type | Extra|
|:-|:-|:-|
 `smallint`  | `integer` |
 `int`       | `integer` |
 `bigint`    | `integer` |
 `float`     | `float`   |
 `decimal`   | `float`   |
 `boolean`   | `boolean` |         
 `string`    | WIP       |
 `date`      | WIP       |
 `timestamp` | WIP       |



## Sample Code

The focus of this dialect is enabling SQLAlchemy Core (as opposed to SQLAchemy ORM). 



### The Simplest Program

A program (see [`sample-app-select.py`](https://github.com/overcoil/fork-databricks-sql-python/blob/sqlalchemy-dev/src/databricks/sqlalchemy/sample-app-select.py)) to read from a Databricks table looks roughly as follows:

```Python
import os

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, Float, Boolean
from sqlalchemy import select

# pickup settings from the env
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path       = os.getenv("DATABRICKS_HTTP_PATH")
access_token    = os.getenv("DATABRICKS_TOKEN")
default_schema  = os.getenv("DATABRICKS_SCHEMA")

# use echo=True for verbose log
engine = create_engine(f"databricks+thrift://token:{access_token}@{server_hostname}/{default_schema}?http_path={http_path}", echo=False, future=True)

metadata_obj = MetaData()

# NB: sample_numtypes is a pre-created/populated table 
tableName = "sample_numtypes"

# declare the schema we're expecting
numtypes = Table(
    tableName,
    metadata_obj,
    Column('f_byte', Integer),
    Column('f_short', Integer),
    Column('f_int', Integer),
    Column('f_long', BigInteger),
    Column('f_float', Float),
    Column('f_decimal', Float),
    Column('f_boolean', Boolean)
)

# SELECT * FROM t WHERE f_byte = -125
stmt = select(numtypes).where(numtypes.c.f_byte == -125)
print(f"Attempting to execute: {stmt}\n")

print(f"Rows from table {tableName}")

with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)
```


### Table definition via reflection
Reflection may be used to recover the schema of a table dynamically via [the `Table` constructor's `autoload_with` parameter](https://docs.sqlalchemy.org/en/14/core/reflection.html).  

```Python
some_table = Table("some_table", metadata_obj, autoload_with=engine)
stmt = select(some_table).where(some_table.c.f_byte == -125)
...
```

### INSERT statement
```Python

```

### Unmanaged table creation
```Python
# TODO
metadata_obj = MetaData()
user_table = Table(
    "user_account",
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('name', String(30)),
    Column('fullname', String)
)
metadata_obj.create_all(engine)
```

### Direct access to Spark SQL
```Python
# TODO: does this work?
with engine.connect() as conn:
    result = conn.execute(text("VACCUM tablename"))
    print(result.all())
```

