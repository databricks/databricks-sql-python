# Using Native Parameters

This connector supports native parameterized query execution. By default, when you call `cursor.execute(query, parameters=...)` the values within the `parameters` collection are sent separately to Databricks Runtime to be escaped. This can improve query performance and prevents SQL injection.


## Requirements

Native parameterized query execution requires `databricks-sql-connector>=3.0.0` and a SQL warehouse or all-purpose cluster running Databricks Runtime >=14.2.

This behaviour is distinct from legacy "inline" parameterized execution in versions below 3.0.0. The legacy behavior is preserved in v3 and above for backwards compatibility but will be removed in a future release. See [Using Inline Parameters](#using-inline-parameters) for more information. See [Limitations](#limitations) for a discussion of when to use native versus inline parameters.

## Limitations

- A query executed with native parameters can contain at most 255 parameter markers
- The maximum size of all parameterized values cannot exceed 1MB

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

The Python syntax for `cursor.execute()` conforms to the [PEP-249 specification](https://peps.python.org/pep-0249/#id20). The `parameters` argument of `.execute()` should be a dictionary if using the `named` paramstyle or a list if using the `qmark` paramstyle.

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

### Legacy `pyformat` paramstyle

Prior to version 3.0.0, this connector's paramstyle when using named parameters was the PEP-249 `pyformat`. To assist customers transitioning their codebases from `pyformat` to `named`, we added a simple transformer to this connector's query execution code path dynamically replaces `pyformat` markers like `%(param)s` with equivalent `named` markers like `:param`. This happens automatically when `use_inline_params=False` (which is the default).

Support for the legacy `pyformat` paramstyle will be removed in a future major release. Users should update their client code to replace `pyformat` markers with `named` markers. 

For example:

```sql
-- a query written for databricks-sql-connector==2.9.3 and below

SELECT field1, field2, %(param1)s FROM table WHERE field4 = %(param2)s

-- rewritten for databricks-sql-connector==3.0.0 and above

SELECT field1, field2, :param1 FROM table WHERE field4 = :param2
```

**Note:** While named `pyformat` markers are transarently replaced when `use_inline_params=False`, un-named inline `%s`-style markers are ignored for parameterization. If your client code makes extensive use of `%s` markers, these queries will need to be updated to use `?` markers before you can execute them when `use_inline_params=False`. See [When to use inline parameters](#using-inline-parameters) for more information.

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

# Using Inline Parameters

This connector has implemented the PEP-249 interface from the beginning. So it's `cursor.execute()` method has always supported passing a sequence or mapping of parameter values. Prior to Databricks Runtime introducing native parameter support, however, "parameterized" queries could not be executed in a guaranteed safe manner. Instead, the connector made a best effort to escape parameter values and and render those strings inline with the query.

This approach has several drawbacks:

- It's not guaranteed to be safe from SQL injection
- The server could not boost performance by caching prepared statements
- The parametter marker syntax conflicted with SQL syntax in some cases

Nevertheless, this behaviour is preserved in version 3.0.0 and above for legacy purposes. It will be removed in a subsequent major release. To enable this legacy code path, you must now construct your connection with `use_inline_params=True`.

## Requirements

Rendering parameters inline is supported on all versions of DBR since these queries are indistinguishable from ad-hoc query text.

## SQL Syntax

There are two ways to demark a parameter in your SQL query: by name or by position. With either approach, each marker that appears in your SQL query will be replaced with an escaped string rendering of the value passed to `cursor.execute()`.

When passed by name, the `pyformat` paramstyle defined in PEP-249 should be used.

```sql
SELECT * FROM TABLE WHERE field = %(value)s
```

When passed by position, a PEP-249 non-compliant paramstyle (`%s`) is required:

```sql
SELECT * FROM TABLE WHERE field = %s
```

## Python Syntax

The Python syntax for `cursor.execute()` conforms to the [PEP-249 specification](https://peps.python.org/pep-0249/#id20). The `parameters` argument of `.execute()` should be a dictionary if using the `pyformat` paramstyle or a list if using the `%s` paramstyle.

### `pyformat` paramstyle Usage Example

```python
from databricks import sql

with sql.connect(..., use_inline_params=True) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = %(value1)s AND another_field = %(value2)s"
        parameters = {"value1": "foo", "value2": 20}
        result = cursor.execute(query, parameters=parameters).fetchone()
```

The above query would be rendered into the following SQL:

```sql
SELECT field FROM table WHERE field = 'foo' AND another_field = 20
```

### `%s` paramstyle Usage Example

```python
from databricks import sql

with sql.connect(..., use_inline_params=True) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = %s AND another_field = %s"
        parameters = ["foo", 20]
        result = cursor.execute(query, parameters=parameters).fetchone()
```

The result of the above two examples is identical.

**Note:** This `%s` syntax overlaps with valid SQL syntax around the usage of `LIKE` DML. For example if your query includes a clause like `WHERE field LIKE '%sequence'`, the parameter inlining function will raise an exception because this string appears to include an inline marker but none is provided.

### When to Use Inline Parameters

You should only set `use_inline_params=True` in the following cases:
1. Your client code passes more than 255 parameters in a single query execution
2. Your client code passes parameter values greater than 1MB in a single query execution
3. Your client code makes extensive use of `%s` positional parameter markers

We expect limitations (1) andd (2) to be addressed in a future Databricks Runtime release.

Native parameters are meant to be a drop-in replacement to inline parameters for most use-cases. With no immediate changes to your client code, upgrading to connector version 3.0.0 and above will grant pre-v3 queries an immediate improvement to safety. And future improvements to parameterization (such as support for binding complex types like `STRUCT`, `MAP`, and `ARRAY`) will only be available when `use_inline_params=False`.
