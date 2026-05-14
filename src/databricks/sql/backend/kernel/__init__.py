"""Backend that delegates to the Databricks SQL Kernel (Rust) via PyO3.

Routed when ``use_sea=True`` is passed to ``databricks.sql.connect``.
The module's identity is "delegates to the kernel" — not the wire
protocol the kernel happens to use today (SEA REST). The kernel may
switch its default transport (SEA REST → SEA gRPC → …) without
renaming this module.

See ``docs/designs/pysql-kernel-integration.md`` in
``databricks-sql-kernel`` for the full integration design.
"""

from databricks.sql.backend.kernel.client import KernelDatabricksClient

__all__ = ["KernelDatabricksClient"]
