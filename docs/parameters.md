# Parameterized Query Execution in `databricks-sql-connector`

This connector supports native parameterized query execution. By default, when you call `cursor.execute(query, parameters=...)` the values within the `parameters` collection are sent separately to Databricks Runtime to be escaped. This can improve query performance and prevents SQL injection.


## Requirements

Native parameterized query execution requires `databricks-sql-connector>=3.0.0` and a SQL warehouse or all-purpose cluster running Databricks Runtime >=14.2.

This behaviour is distinct from parameterized execution in versions below 3.0.0. See **Using Inline Parameters** below.

## SQL Syntax

There are two ways to demark a parameter in your SQL query: by name or by position. With either approach, each marker that appears in your SQL query will be replaced with the parameterized value supplied to `cursor.execute()`.

When passed by name, the `named` paramstyle defined in PEP-249 should be used.

```sql
SELECT * FROM TABLE WHERE field = :value
```

When passed by position, the `qmark` paramstyle should be used.

```sql
SELECT * FROM TABLE WHERE field = ?
```

A single query cannot mix the `named` and `qmark` paramstyles.

## Python Syntax

The Python syntax for `cursor.execute()` conforms to the [PEP-249 specification](https://peps.python.org/pep-0249/#id20). The `parameters` argument of `.execute()` should be a dictionary if using the `named` paramstyle or a listif using the `qmark` paramstyle.

### `named` paramstyle Usage Example

When parameter values are passed as a dictionary, this connector will use a named approach to bind values to the query text. Therefore, the dictionary's keys must correspond to the `named` markers in your query text. The corresponding dictionary values will be bound to those markers when DBR executes the query. The length of the dictionary must exactly match the count of `named` markers in your query or an exception will be raised.

```python
from databricks import sql

with sql.connect(...) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = :value1 AND another_field = :value2"
        parameters = {"value1": "foo", "value2": 20}
        result = cursor.execute(query, parameters=parameters).fetchone()
```

### `qmark` paramstyle Usage Example

By default, when parameters are passed as a list this connector will use a positional approach to bind values to the query text. The order of values in the list corresponds to the order of the `qmark` markers in your query text. The first value will replace the first `?`, the second value will replace the second `?` etc. The length of the list must exactly match the count of `?` markers in your query or an exception will be raised.

```python
from databricks import sql

with sql.connect(...) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = ? AND another_field = ?"
        parameters = ["foo", 20]
        result = cursor.execute(query, parameters=parameters).fetchone()
```

The result of the above two examples is identical.

### Type inference

 Under the covers, parameter values are annotated with a valid Databricks SQL type. As shown in the examples above, this connector accepts primitive Python types like `int`, `str`, and `Decimal`. When this happens, the connector infers the corresponding Databricks SQL type (e.g. `INT`, `STRING`, `DECIMAL`) automatically. This means that the parameters passed to `cursor.execute()` are always wrapped in a `TDbsqlParameter` subtype prior to execution.

Automatic inferrence is sufficient for most usages. But you can bypass the inference by explicitly setting the Databricks SQL type of a bound parameter in your client code. All supported Databricks SQL types have corresponding `TDbsqlParameter` implementations which you can import from `databricks.sql.parameters`. 

`TDbsqlParameter` objects must always be passed within a list. Either paramstyle (`:named` or `?`) may be used. However, if your querye uses the `named` paramstyle, all `TDbsqlParameter` objects must be provided a `name` when they are constructed.

```python
from databricks import sql
from databricks.sql.parameters import StringParameter, IntegerParameter

# with `named` markers
with sql.connect(...) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = :value1 AND another_field = :value2"
        parameters = [
            StringParameter(name="value1", value="foo"),
            IntegerParameter(name="value2", value=20)
        ]
        result = cursor.execute(query, parameters=parameters).fetchone()

# with `?` markers
with sql.connect(...) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = ? AND another_field = ?"
        parameters = [
            StringParameter(value="foo"),
            IntegerParameter(value=20)
        ]
        result = cursor.execute(query, parameters=parameters).fetchone()
```

In general, we recommend using `?` markers when passing `TDbsqlParameter`'s directly.

**Note**: When using `?` markers, you can bypass inference for _some_ parameters by passing a list containing both primitive Python types and `TDbsqlParameter` objects. `TDbsqlParameter` objects can never be passed in a dictionary.

