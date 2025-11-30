# Transaction Support

The Databricks SQL Connector for Python supports multi-statement transactions (MST). This allows you to group multiple SQL statements into atomic units that either succeed completely or fail completely.

## Autocommit Behavior

By default, every SQL statement executes in its own transaction and commits immediately (autocommit mode). This is the standard behavior for most database connectors.

```python
from databricks import sql

connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/abc123"
)

# Default: autocommit is True
print(connection.autocommit)  # True

# Each statement commits immediately
cursor = connection.cursor()
cursor.execute("INSERT INTO my_table VALUES (1, 'data')")
# Already committed - data is visible to other connections
```

To use explicit transactions, disable autocommit:

```python
connection.autocommit = False

# Now statements are grouped into a transaction
cursor = connection.cursor()
cursor.execute("INSERT INTO my_table VALUES (1, 'data')")
# Not committed yet - must call connection.commit()

connection.commit()  # Now it's visible
```

## Basic Transaction Operations

### Committing Changes

When autocommit is disabled, you must explicitly commit your changes:

```python
connection.autocommit = False
cursor = connection.cursor()

try:
    cursor.execute("INSERT INTO orders VALUES (1, 100.00)")
    cursor.execute("INSERT INTO order_items VALUES (1, 'Widget', 2)")
    connection.commit()  # Both inserts succeed together
except Exception as e:
    connection.rollback()  # Neither insert is saved
    raise
finally:
    connection.autocommit = True  # Restore default state
```

### Rolling Back Changes

Use `rollback()` to discard all changes made in the current transaction:

```python
connection.autocommit = False
cursor = connection.cursor()

cursor.execute("INSERT INTO accounts VALUES (1, 1000)")
cursor.execute("UPDATE accounts SET balance = balance - 500 WHERE id = 1")

# Changed your mind?
connection.rollback()  # All changes discarded
```

Note: Calling `rollback()` when autocommit is enabled is safe (it's a no-op), but calling `commit()` will raise a `TransactionError`.

### Sequential Transactions

After a commit or rollback, a new transaction starts automatically:

```python
connection.autocommit = False

# First transaction
cursor.execute("INSERT INTO logs VALUES (1, 'event1')")
connection.commit()

# Second transaction starts automatically
cursor.execute("INSERT INTO logs VALUES (2, 'event2')")
connection.rollback()  # Only the second insert is discarded
```

## Multi-Table Transactions

Transactions span multiple tables atomically. Either all changes are committed, or all are rolled back:

```python
connection.autocommit = False
cursor = connection.cursor()

try:
    # Insert into multiple tables
    cursor.execute("INSERT INTO customers VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO orders VALUES (1, 1, 100.00)")
    cursor.execute("INSERT INTO shipments VALUES (1, 1, 'pending')")
    
    connection.commit()  # All three inserts succeed atomically
except Exception as e:
    connection.rollback()  # All three inserts are discarded
    raise
finally:
    connection.autocommit = True  # Restore default state
```

This is particularly useful for maintaining data consistency across related tables.

## Transaction Isolation

Databricks uses **Snapshot Isolation** (mapped to `REPEATABLE_READ` in standard SQL terminology). This means:

- **Repeatable reads**: Once you read data in a transaction, subsequent reads will see the same data (even if other transactions modify it)
- **Atomic commits**: Changes are visible to other connections only after commit
- **Write serializability within a single table**: Concurrent writes to the same table will cause conflicts
- **Snapshot isolation across tables**: Concurrent writes to different tables can succeed

### Getting the Isolation Level

```python
level = connection.get_transaction_isolation()
print(level)  # Output: REPEATABLE_READ
```

### Setting the Isolation Level

Currently, only `REPEATABLE_READ` is supported:

```python
from databricks import sql

# Using the constant
connection.set_transaction_isolation(sql.TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ)

# Or using a string
connection.set_transaction_isolation("REPEATABLE_READ")

# Other levels will raise NotSupportedError
connection.set_transaction_isolation("READ_COMMITTED")  # Raises NotSupportedError
```

### What Repeatable Read Means in Practice

Within a transaction, you'll always see a consistent snapshot of the data:

```python
connection.autocommit = False
cursor = connection.cursor()

# First read
cursor.execute("SELECT balance FROM accounts WHERE id = 1")
balance1 = cursor.fetchone()[0]  # Returns 1000

# Another connection updates the balance
# (In a separate connection: UPDATE accounts SET balance = 500 WHERE id = 1)

# Second read in the same transaction
cursor.execute("SELECT balance FROM accounts WHERE id = 1")
balance2 = cursor.fetchone()[0]  # Still returns 1000 (repeatable read!)

connection.commit()

# After commit, new transactions will see the updated value (500)
```

## Error Handling

### Setting Autocommit During a Transaction

You cannot change autocommit mode while a transaction is active:

```python
connection.autocommit = False
cursor = connection.cursor()

try:
    cursor.execute("INSERT INTO logs VALUES (1, 'data')")
    
    # This will raise TransactionError
    connection.autocommit = True  # Error: transaction is active
    
except sql.TransactionError as e:
    print(f"Cannot change autocommit: {e}")
    connection.rollback()  # Clean up the transaction
finally:
    connection.autocommit = True  # Now it's safe to restore
```

### Committing Without an Active Transaction

If autocommit is enabled, there's no active transaction, so calling `commit()` will fail:

```python
connection.autocommit = True  # Default

try:
    connection.commit()  # Raises TransactionError
except sql.TransactionError as e:
    print(f"No active transaction: {e}")
```

However, `rollback()` is safe in this case (it's a no-op).

### Recovering from Query Failures

If a statement fails during a transaction, roll back and start a new transaction:

```python
connection.autocommit = False
cursor = connection.cursor()

try:
    cursor.execute("INSERT INTO valid_table VALUES (1, 'data')")
    cursor.execute("INSERT INTO nonexistent_table VALUES (2, 'data')")  # Fails
    connection.commit()
except Exception as e:
    connection.rollback()  # Discard the partial transaction
    
    # Log the error (with autocommit still disabled)
    try:
        cursor.execute("INSERT INTO error_log VALUES (1, 'Query failed')")
        connection.commit()
    except Exception:
        connection.rollback()
finally:
    connection.autocommit = True  # Restore default state
```

## Querying Server State

By default, the `autocommit` property returns a cached value for performance. If you need to query the server each time (for instance, when strong consistency is required):

```python
connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/abc123",
    fetch_autocommit_from_server=True
)

# Each access queries the server
state = connection.autocommit  # Executes "SET AUTOCOMMIT" query
```

This is generally not needed for normal usage.

## Write Conflicts

### Within a Single Table

Databricks enforces **write serializability** within a single table. If two transactions try to modify the same table concurrently, one will fail:

```python
# Connection 1
conn1.autocommit = False
cursor1 = conn1.cursor()
cursor1.execute("INSERT INTO accounts VALUES (1, 100)")

# Connection 2 (concurrent)
conn2.autocommit = False
cursor2 = conn2.cursor()
cursor2.execute("INSERT INTO accounts VALUES (2, 200)")

# First commit succeeds
conn1.commit()  # OK

# Second commit fails with concurrent write conflict
try:
    conn2.commit()  # Raises error about concurrent writes
except Exception as e:
    conn2.rollback()
    print(f"Concurrent write detected: {e}")
```

This happens even when the rows being modified are different. The conflict detection is at the table level.

### Across Multiple Tables

Concurrent writes to *different* tables can succeed. Each table tracks its own write conflicts independently:

```python
# Connection 1: writes to table_a
conn1.autocommit = False
cursor1 = conn1.cursor()
cursor1.execute("INSERT INTO table_a VALUES (1, 'data')")

# Connection 2: writes to table_b (different table)
conn2.autocommit = False
cursor2 = conn2.cursor()
cursor2.execute("INSERT INTO table_b VALUES (1, 'data')")

# Both commits succeed (different tables)
conn1.commit()  # OK
conn2.commit()  # Also OK
```

## Best Practices

1. **Keep transactions short**: Long-running transactions can cause conflicts with other connections. Commit as soon as your atomic unit of work is complete.

2. **Always handle exceptions**: Wrap transaction code in try/except/finally and call `rollback()` on errors.

```python
connection.autocommit = False
cursor = connection.cursor()

try:
    cursor.execute("INSERT INTO table1 VALUES (1, 'data')")
    cursor.execute("UPDATE table2 SET status = 'updated'")
    connection.commit()
except Exception as e:
    connection.rollback()
    logger.error(f"Transaction failed: {e}")
    raise
finally:
    connection.autocommit = True  # Restore default state
```

3. **Use context managers**: If you're writing helper functions, consider using a context manager pattern:

```python
from contextlib import contextmanager

@contextmanager
def transaction(connection):
    connection.autocommit = False
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.autocommit = True

# Usage
with transaction(connection):
    cursor = connection.cursor()
    cursor.execute("INSERT INTO logs VALUES (1, 'message')")
    # Auto-commits on success, auto-rolls back on exception
```

4. **Reset autocommit when done**: Use a `finally` block to restore autocommit to `True`. This is especially important if the connection is reused or part of a connection pool:

```python
connection.autocommit = False
try:
    # ... transaction code ...
    connection.commit()
except Exception:
    connection.rollback()
    raise
finally:
    connection.autocommit = True  # Restore to default state
```

5. **Be aware of isolation semantics**: Remember that repeatable read means you see a snapshot from the start of your transaction. If you need to see recent changes from other transactions, commit your current transaction and start a new one.

## Requirements

To use transactions, you need:
- A Databricks SQL warehouse that supports Multi-Statement Transactions (MST)
- Tables created with the `delta.feature.catalogOwned-preview` table property:

```sql
CREATE TABLE my_table (id INT, value STRING)
USING DELTA
TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')
```

## Related APIs

- `connection.autocommit` - Get or set autocommit mode (boolean)
- `connection.commit()` - Commit the current transaction
- `connection.rollback()` - Roll back the current transaction
- `connection.get_transaction_isolation()` - Get the isolation level (returns `"REPEATABLE_READ"`)
- `connection.set_transaction_isolation(level)` - Validate/set isolation level (only `"REPEATABLE_READ"` supported)
- `sql.TransactionError` - Exception raised for transaction-specific errors

All of these are extensions to [PEP 249](https://www.python.org/dev/peps/pep-0249/) (Python Database API Specification v2.0).
