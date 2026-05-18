"""Shared error-mapping primitives for the kernel backend.

The PyO3 boundary can produce two flavours of exception:

- ``databricks_sql_kernel.KernelError`` — the kernel's own
  structured error type. Carries ``code`` / ``message`` /
  ``sql_state`` / ``query_id`` / ``http_status`` / ``retryable`` /
  ``vendor_code`` / ``error_code`` as attributes; mapped to a PEP
  249 exception class via ``_CODE_TO_EXCEPTION`` with the
  attributes forwarded onto the re-raised exception so callers can
  branch on ``err.code`` / ``err.sql_state`` without reaching
  through ``__cause__``.
- Anything else — ``TypeError`` / ``OverflowError`` /
  ``ValueError`` from PyO3 argument conversion, or arbitrary
  extension-internal Python errors. These would otherwise propagate
  raw to connector callers, breaking the DB-API contract that says
  "only PEP 249 exception types cross the boundary". Wrapped in
  ``OperationalError`` here.

These primitives live in their own module so both ``client.py``
(which orchestrates PyO3 calls) and ``result_set.py`` (which calls
``fetch_next_batch`` on the same kernel handles) can share them
without ``result_set.py`` importing from ``client.py``.

Usage at every PyO3 call site is a plain try/except:

    try:
        stmt.execute()
    except Exception as exc:
        raise wrap_kernel_exception("execute_command", exc) from exc

The helper returns the mapped exception; callers raise it. Plain
``try/except`` is preferred over a context manager: the control
flow is visible at the call site, the helper is a pure function
(trivial to test), and tracebacks don't carry an extra
``__exit__`` frame.
"""

from __future__ import annotations

from databricks.sql.exc import (
    DatabaseError,
    Error,
    OperationalError,
    ProgrammingError,
)


try:
    import databricks_sql_kernel as _kernel  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover - same hint as client.py
    raise ImportError(
        "use_kernel=True requires the databricks-sql-kernel extension, which "
        "is not yet published on PyPI. Build and install it locally from the "
        "databricks-sql-kernel repo:\n"
        "  cd databricks-sql-kernel/pyo3 && maturin develop --release\n"
        "(into the same venv as databricks-sql-connector)."
    ) from exc


# Map a kernel `code` slug to the PEP 249 exception class that best
# captures it. The match isn't a perfect 1:1 — PEP 249 has a
# narrower taxonomy than the kernel — so several kernel codes
# collapse onto the same Python exception. This table is the only
# place that mapping lives.
_CODE_TO_EXCEPTION = {
    "InvalidArgument": ProgrammingError,
    "Unauthenticated": OperationalError,
    "PermissionDenied": OperationalError,
    "NotFound": ProgrammingError,
    "ResourceExhausted": OperationalError,
    "Unavailable": OperationalError,
    "Timeout": OperationalError,
    "Cancelled": OperationalError,
    "DataLoss": DatabaseError,
    "Internal": DatabaseError,
    "InvalidStatementHandle": ProgrammingError,
    "NetworkError": OperationalError,
    "SqlError": DatabaseError,
    "Unknown": DatabaseError,
}


def reraise_kernel_error(exc: "_kernel.KernelError") -> "Error":
    """Convert a ``databricks_sql_kernel.KernelError`` to a PEP 249
    exception with the kernel's structured attributes forwarded onto
    the new instance."""
    code = getattr(exc, "code", "Unknown")
    cls = _CODE_TO_EXCEPTION.get(code, DatabaseError)
    new = cls(getattr(exc, "message", str(exc)))
    for attr in (
        "code",
        "sql_state",
        "error_code",
        "vendor_code",
        "http_status",
        "retryable",
        "query_id",
    ):
        setattr(new, attr, getattr(exc, attr, None))
    new.__cause__ = exc
    return new


def wrap_kernel_exception(what: str, exc: BaseException) -> "Error":
    """Map any exception from a PyO3 call site to a PEP 249 exception.

    - ``KernelError`` → mapped class with structured attrs forwarded.
    - Already-PEP-249 ``Error`` (e.g. raised by an inner caller that
      already mapped) → passed through unchanged.
    - Anything else (``TypeError`` / ``ValueError`` / etc. from PyO3
      argument conversion, extension-internal errors) → wrapped in
      ``OperationalError``.

    Returned, not raised — the caller decides whether to ``raise``
    or ``raise ... from exc``. ``what`` is a short tag (the calling
    method name) used only in the ``OperationalError`` message.
    """
    if isinstance(exc, _kernel.KernelError):
        return reraise_kernel_error(exc)
    if isinstance(exc, Error):
        return exc
    return OperationalError(
        f"Unexpected error from databricks_sql_kernel during {what}: {exc!r}"
    )
