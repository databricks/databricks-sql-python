from databricks import sql
import os

with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
) as connection:

    # Disable autocommit to use explicit transactions
    connection.autocommit = False

    with connection.cursor() as cursor:
        try:
            # Create tables for demonstration
            cursor.execute("CREATE TABLE IF NOT EXISTS accounts (id int, balance int)")
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS transfers (from_id int, to_id int, amount int)"
            )
            connection.commit()

            # Start a new transaction - transfer money between accounts
            cursor.execute("INSERT INTO accounts VALUES (1, 1000), (2, 500)")
            cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
            cursor.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
            cursor.execute("INSERT INTO transfers VALUES (1, 2, 100)")

            # Commit the transaction - all changes succeed together
            connection.commit()
            print("Transaction committed successfully")

            # Verify the results
            cursor.execute("SELECT * FROM accounts ORDER BY id")
            print("Accounts:", cursor.fetchall())

            cursor.execute("SELECT * FROM transfers")
            print("Transfers:", cursor.fetchall())

        except Exception as e:
            # Roll back on error - all changes are discarded
            connection.rollback()
            print(f"Transaction rolled back due to error: {e}")
            raise

        finally:
            # Restore autocommit to default state
            connection.autocommit = True
