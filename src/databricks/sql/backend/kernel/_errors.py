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
without ``result_set.py`` importing from ``client.py`` — that
direction would be a layering violation.
"""

from __future__ import annotations

import contextlib
from typing import Iterator

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


@contextlib.contextmanager
def kernel_call(what: str) -> Iterator[None]:
    """Context manager that wraps a span of PyO3 calls so any error
    crossing the Python/Rust boundary surfaces as a PEP 249
    exception.

    ``KernelError`` flows through ``reraise_kernel_error`` (the
    structured-attribute path). Anything else is wrapped in
    ``OperationalError`` so DB-API callers see a uniform exception
    surface and never have to catch native Python exceptions to
    handle a connector-level failure.

    ``what`` is a short tag used only in the ``OperationalError``
    message for the non-``KernelError`` path; keep it caller-named
    (e.g. ``"execute_command"``).
    """
    try:
        yield
    except _kernel.KernelError as exc:
        raise reraise_kernel_error(exc) from exc
    except Error:
        # Already a PEP 249 error (e.g. a nested ``kernel_call`` or
        # the cursor-side guard re-raising one); let it propagate
        # unchanged.
        raise
    except Exception as exc:
        raise OperationalError(
            f"Unexpected error from databricks_sql_kernel during {what}: {exc!r}"
        ) from exc
