# Using Native Parameters

This connector supports native parameterized query execution. When you execute a query that includes variable markers, then you can pass a collection of parameters which are sent separately to Databricks Runtime for safe execution. This prevents SQL injection and can improve query performance.

This behaviour is distinct from legacy "inline" parameterized execution in versions below 3.0.0. The legacy behavior is preserved behind a flag called `use_inline_params`, which will be removed in a future release. See [Using Inline Parameters](#using-inline-parameters) for more information.

See **[below](#migrating-to-native-parameters)** for details about updating your client code to use native parameters.

See `examples/parameters.py` in this repository for a runnable demo.

## Requirements

- `databricks-sql-connector>=3.0.0`
- A SQL warehouse or all-purpose cluster running Databricks Runtime >=14.2

## Limitations

- A query executed with native parameters can contain at most 255 parameter markers
- The maximum size of all parameterized values cannot exceed 1MB

## SQL Syntax

Variables in your SQL query can use one of three PEP-249 [paramstyles](https://peps.python.org/pep-0249/#paramstyle). A parameterized query can use exactly one paramstyle.

|paramstyle|example|comment|
|-|-|-|
|`named`|`:param`|Parameters must be named|
|`qmark`|`?`|Parameter names are ignored|
|`pyformat`|`%(param)s`|Legacy syntax. Will be deprecated. Parameters must be named.|

#### Example

```sql
-- named paramstyle
SELECT * FROM table WHERE field = :value

-- qmark paramstyle
SELECT * FROM table WHERE field = ?

-- pyformat paramstyle (legacy)
SELECT * FROM table WHERE field = %(value)s
```

## Python Syntax

This connector follows the [PEP-249 interface](https://peps.python.org/pep-0249/#id20). The expected structure of the parameter collection follows the paramstyle of the variables in your query. 

### `named` paramstyle Usage Example

When your SQL query uses `named` paramstyle variable markers, you need specify a name for each value that corresponds to a variable marker in your query.

Generally, you do this by passing `parameters` as a dictionary whose keys match the variables in your query. The length of the dictionary must exactly match the count of variable markers or an exception will be raised.

```python
from databricks import sql

with sql.connect(...) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = :value1 AND another_field = :value2"
        parameters = {"value1": "foo", "value2": 20}
        result = cursor.execute(query, parameters=parameters).fetchone()
```

This paramstyle is a drop-in replacement for the `pyformat` paramstyle which was used in connector versions below 3.0.0. It should be used going forward.

### `qmark` paramstyle Usage Example

When your SQL query uses `qmark` paramstyle variable markers, you only need to specify a value for each variable marker in your query.

You do this by passing `parameters` as a list. The order of values in the list corresponds to the order of `qmark` variables in your query. The length of the list must exactly match the count of variable markers in your query or an exception will be raised.

```python
from databricks import sql

with sql.connect(...) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = ? AND another_field = ?"
        parameters = ["foo", 20]
        result = cursor.execute(query, parameters=parameters).fetchone()
```

The result of the above two examples is identical.

### Legacy `pyformat` paramstyle Usage Example

Databricks Runtime expects variable markers to use either `named` or `qmark` paramstyles. Historically, this connector used `pyformat` which Databricks Runtime does not support. So to assist assist customers transitioning their codebases from `pyformat` → `named`, we can dynamically rewrite the variable markers before sending the query to Databricks. This happens only when `use_inline_params=False`.

 This dynamic rewrite will be deprecated in a future release. New queries should be written using the `named` paramstyle instead. And users should update their client code to replace `pyformat` markers with `named` markers. 

For example:

```sql
-- a query written for databricks-sql-connector==2.9.3 and below

SELECT field1, field2, %(param1)s FROM table WHERE field4 = %(param2)s

-- rewritten for databricks-sql-connector==3.0.0 and above

SELECT field1, field2, :param1 FROM table WHERE field4 = :param2
```


**Note:** While named `pyformat` markers are transparently replaced when `use_inline_params=False`, un-named inline `%s`-style markers are ignored. If your client code makes extensive use of `%s` markers, these queries will need to be updated to use `?` markers before you can execute them when `use_inline_params=False`. See [When to use inline parameters](#when-to-use-inline-parameters) for more information.

### Type inference

Under the covers, parameter values are annotated with a valid Databricks SQL type. As shown in the examples above, this connector accepts primitive Python types like `int`, `str`, and `Decimal`. When this happens, the connector infers the corresponding Databricks SQL type (e.g. `INT`, `STRING`, `DECIMAL`) automatically. This means that the parameters passed to `cursor.execute()` are always wrapped in a `TDbsqlParameter` subtype prior to execution.

Automatic inferrence is sufficient for most usages. But you can bypass the inference by explicitly setting the Databricks SQL type in your client code. All supported Databricks SQL types have `TDbsqlParameter` implementations which you can import from `databricks.sql.parameters`. 

`TDbsqlParameter` objects must always be passed within a list. Either paramstyle (`:named` or `?`) may be used. However, if your query uses the `named` paramstyle, all `TDbsqlParameter` objects must be provided a `name` when they are constructed.

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

Since its initial release, this connector's `cursor.execute()` method has supported passing a sequence or mapping of parameter values. Prior to Databricks Runtime introducing native parameter support, however, "parameterized" queries could not be executed in a guaranteed safe manner. Instead, the connector made a best effort to escape parameter values and and render those strings inline with the query.

This approach has several drawbacks:

- It's not guaranteed to be safe from SQL injection
- The server could not boost performance by caching prepared statements
- The parameter marker syntax conflicted with SQL syntax in some cases

Nevertheless, this behaviour is preserved in version 3.0.0 and above for legacy purposes. It will be removed in a subsequent major release. To enable this legacy code path, you must now construct your connection with `use_inline_params=True`.

## Requirements

Rendering parameters inline is supported on all versions of DBR since these queries are indistinguishable from ad-hoc query text.


## SQL Syntax

Variables in your SQL query can look like `%(param)s` or like `%s`. 

#### Example

```sql
-- pyformat paramstyle is used for named parameters
SELECT * FROM table WHERE field = %(value)s

-- %s is used for positional parameters
SELECT * FROM table WHERE field = %s
```

## Python Syntax

This connector follows the [PEP-249 interface](https://peps.python.org/pep-0249/#id20). The expected structure of the parameter collection follows the paramstyle of the variables in your query. 

### `pyformat` paramstyle Usage Example

Parameters must be passed as a dictionary.

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

Parameters must be passed as a list.

```python
from databricks import sql

with sql.connect(..., use_inline_params=True) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field = %s AND another_field = %s"
        parameters = ["foo", 20]
        result = cursor.execute(query, parameters=parameters).fetchone()
```

The result of the above two examples is identical.

**Note**: `%s` is not compliant with PEP-249 and only works due to the specific implementation of our inline renderer. 

**Note:** This `%s` syntax overlaps with valid SQL syntax around the usage of `LIKE` DML. For example if your query includes a clause like `WHERE field LIKE '%sequence'`, the parameter inlining function will raise an exception because this string appears to include an inline marker but none is provided. This means that connector versions below 3.0.0 it has been impossible to execute a query that included both parameters and LIKE wildcards. When `use_inline_params=False`, we will pass `%s` occurrences along to the database, allowing it to be used as expected in `LIKE` statements.

### Passing sequences as parameter values

Parameter values can also be passed as a sequence. This is typically used when writing `WHERE ... IN` clauses:

```python
from databricks import sql

with sql.connect(..., use_inline_params=True) as conn:
    with conn.cursor() as cursor():
        query = "SELECT field FROM table WHERE field IN %(value_list)s"
        parameters = {"value_list": [1,2,3,4,5]}
        result = cursor.execute(query, parameters=parameters).fetchone()
```

Output:

```sql
SELECT field FROM table WHERE field IN (1,2,3,4,5)
```

**Note**: this behavior is not specified by PEP-249 and only works due to the specific implementation of our inline renderer.

### Migrating to native parameters

Native parameters are meant to be a drop-in replacement for inline parameters. In most use-cases, upgrading to `databricks-sql-connector>=3.0.0` will grant an immediate improvement to safety. Plus, native parameters allow you to use SQL LIKE wildcards (`%`) in your queries which is impossible with inline parameters. Future improvements to parameterization (such as support for binding complex types like `STRUCT`, `MAP`, and `ARRAY`) will only be available when `use_inline_params=False`.

To completely migrate, you need to [revise your SQL queries](#legacy-pyformat-paramstyle-usage-example) to use the new paramstyles.


### When to use inline parameters

You should only set `use_inline_params=True` in the following cases:

1. Your client code passes more than 255 parameters in a single query execution
2. Your client code passes parameter values greater than 1MB in a single query execution
3. Your client code makes extensive use of [`%s` positional parameter markers](#s-paramstyle-usage-example)
4. Your client code uses [sequences as parameter values](#passing-sequences-as-parameter-values)

We expect limitations (1) and (2) to be addressed in a future Databricks Runtime release.
