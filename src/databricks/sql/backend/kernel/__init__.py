"""Backend that delegates to the Databricks SQL Kernel (Rust) via PyO3.

Routed when ``use_kernel=True`` is passed to ``databricks.sql.connect``.
The module's identity is "delegates to the kernel" — not the wire
protocol the kernel happens to use today (SEA REST). The kernel may
switch its default transport (SEA REST → SEA gRPC → …) without
renaming this module.

This ``__init__`` deliberately does **not** re-export
``KernelDatabricksClient`` from ``.client``. Importing ``.client``
loads the ``databricks_sql_kernel`` PyO3 extension at module-import
time; doing that eagerly here would make ``import
databricks.sql.backend.kernel.type_mapping`` (used by tests / by
``KernelResultSet`` consumers) require the kernel wheel even when
the caller never plans to open a kernel-backed session. Callers
that need the client import it directly:

    from databricks.sql.backend.kernel.client import KernelDatabricksClient

``session.py::_create_backend`` already does this lazy import under
the ``use_kernel=True`` branch.

See ``docs/designs/pysql-kernel-integration.md`` in
``databricks-sql-kernel`` for the full integration design.
"""
