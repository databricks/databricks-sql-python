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

import logging

from databricks.sql.exc import (
    DatabaseError,
    Error,
    OperationalError,
    ProgrammingError,
    ServerOperationError,
)

try:
    import databricks_sql_kernel as _kernel  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover - same hint as client.py
    raise ImportError(
        "use_kernel=True requires the optional databricks-sql-kernel "
        "extension, which is not installed. Install it with:\n"
        '  pip install "databricks-sql-connector[kernel]"\n'
        "The kernel wheel requires Python >= 3.10; on older interpreters "
        "use_kernel is unavailable. For local kernel development you can "
        "instead build it from the databricks-sql-kernel repo:\n"
        "  cd databricks-sql-kernel/pyo3 && maturin develop --release"
    ) from exc

# Route the kernel's Rust-side logs into Python's ``logging`` as soon as
# the extension loads. The kernel emits under the ``databricks.sql.kernel``
# logger (a child of the connector's ``databricks.sql`` namespace), so a
# customer who configures ``databricks.sql`` logging gets kernel logs for
# free with no extra setup.
#
# This is a best-effort, non-essential feature: it must never take down
# ``use_kernel=True`` for a process. ``getattr`` guards against an older
# kernel wheel that predates the function. The ``try`` guards against the
# call itself throwing — note ``except BaseException`` is deliberate: a
# panic raised across the PyO3 boundary surfaces as
# ``pyo3_runtime.PanicException``, which derives from ``BaseException``
# (not ``Exception``), so a narrower clause would let it escape module
# import and fail every kernel-backed connection. The kernel side is
# idempotent and returns rather than panics on a double install, but we
# do not rely on that here — the guard holds regardless of the Rust impl.
_kernel_init_logging = getattr(_kernel, "init_logging", None)
if _kernel_init_logging is not None:
    try:
        _kernel_init_logging()
    except BaseException as exc:  # noqa: BLE001 - see comment above re: PanicException
        logging.getLogger(__name__).debug(
            "kernel log bridge init failed; continuing without it: %r", exc
        )


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
    # `SqlError` is a server-side query failure (syntax error, missing
    # object, etc.) — exactly what the Thrift backend surfaces as
    # `ServerOperationError`. Match Thrift's contract so user code that
    # catches `ServerOperationError` (a subclass of `DatabaseError`)
    # works equivalently with `use_kernel=True`.
    "SqlError": ServerOperationError,
    "Unknown": DatabaseError,
}


def reraise_kernel_error(exc: "_kernel.KernelError") -> "Error":
    """Convert a ``databricks_sql_kernel.KernelError`` to a PEP 249
    exception with the kernel's structured attributes forwarded onto
    the new instance.

    The returned exception is raised by callers with ``raise ... from
    exc``; the ``from`` clause is what sets ``__cause__``, so we don't
    touch it here.
    """
    code = getattr(exc, "code", "Unknown")
    cls = _CODE_TO_EXCEPTION.get(code, DatabaseError)

    # For ServerOperationError, reproduce the Thrift backend's
    # ``context`` dict so callers that read
    # ``err.context["diagnostic-info"]`` (the Spark stack trace) /
    # ``err.context["operation-id"]`` get the same shape on the kernel
    # path. ``diagnostic_info`` is forwarded from the kernel error (it
    # now crosses the PyO3 boundary; older wheels return ``None`` via
    # ``getattr``, so this degrades gracefully). Matches
    # thrift_backend.py's ServerOperationError construction.
    context = None
    if cls is ServerOperationError:
        context = {
            "operation-id": getattr(exc, "query_id", None),
            "diagnostic-info": getattr(exc, "diagnostic_info", None),
        }
    new = cls(getattr(exc, "message", str(exc)), context)

    for attr in (
        "code",
        "sql_state",
        "error_code",
        "vendor_code",
        "http_status",
        "retryable",
        "query_id",
        # Extended server status now forwarded across the PyO3 boundary
        # (kernel #121). ``getattr(..., None)`` keeps this forward-safe
        # against an older wheel that doesn't set these attrs.
        "display_message",
        "diagnostic_info",
        "error_details_json",
    ):
        setattr(new, attr, getattr(exc, attr, None))
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
