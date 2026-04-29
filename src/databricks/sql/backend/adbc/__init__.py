"""ADBC-Rust-kernel-backed backend for databricks-sql-python (POC).

Wraps the PyO3 binding `databricks_adbc_pyo3` and adapts it to the
`DatabricksClient` / `ResultSet` interfaces used by the rest of the connector.
"""

from databricks.sql.backend.adbc.client import AdbcDatabricksClient

__all__ = ["AdbcDatabricksClient"]
