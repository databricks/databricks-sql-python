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
  raw to connector callers. Wrapped in
  ``OperationalError`` here.

Local transfer failures use built-in ``OSError`` classes, including
``FileNotFoundError``, ``PermissionError`` and other platform subclasses.
The original OS error code and requested filename are preserved when available.

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
import os
from typing import Union

from databricks.sql.exc import (
    DatabaseError,
    Error,
    NotSupportedError,
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


def reraise_kernel_error(
    exc: "_kernel.KernelError",
) -> "Union[Error, OSError]":
    """Map a kernel error and forward its structured attributes.

    Local transfer failures use built-in ``OSError`` classes, including
    missing-file, permission, directory and disk-full errors. Non-local
    errors use PEP 249 classes.

    The returned exception is raised by callers with ``raise ... from
    exc``; the ``from`` clause is what sets ``__cause__``, so we don't
    touch it here.
    """
    code = getattr(exc, "code", "Unknown")
    cls = _CODE_TO_EXCEPTION.get(code, DatabaseError)
    error_code = getattr(exc, "error_code", None)
    if error_code == "FILE_TRANSFER_UNSUPPORTED_STREAM":
        cls = NotSupportedError
    file_transfer = error_code in (
        "FILE_TRANSFER_ERROR",
        "FILE_TRANSFER_PERMISSION_ERROR",
    )
    if file_transfer:
        cls = (
            ProgrammingError
            if code in ("PermissionDenied", "InvalidArgument")
            and getattr(exc, "http_status", None) is None
            else OperationalError
        )

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
    message = getattr(exc, "message", str(exc))
    os_error_code = getattr(exc, "os_error_code", None)
    os_error_filename = getattr(exc, "os_error_filename", None)
    file_error_args: tuple
    if os_error_code is None:
        file_error_args = (
            (None, message, os_error_filename)
            if os_error_filename is not None
            else (message,)
        )
    elif os.name == "nt":
        file_error_args = (0, message, os_error_filename, os_error_code)
    else:
        file_error_args = (os_error_code, message, os_error_filename)
    new: Union[Error, OSError]
    # Local missing files have no HTTP status; remote missing files carry 404.
    if (
        file_transfer
        and os_error_code is not None
        and getattr(exc, "http_status", None) is None
    ):
        new = OSError(*file_error_args)
    elif (
        error_code == "FILE_TRANSFER_PERMISSION_ERROR"
        and getattr(exc, "http_status", None) is None
    ):
        new = PermissionError(*file_error_args)
    elif (
        file_transfer
        and code == "NotFound"
        and getattr(exc, "http_status", None) is None
    ):
        new = FileNotFoundError(*file_error_args)
    else:
        new = cls(message, context)

    for attr in (
        "code",
        "sql_state",
        "error_code",
        "vendor_code",
        "os_error_code",
        "os_error_filename",
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


def wrap_kernel_exception(what: str, exc: BaseException) -> "Union[Error, OSError]":
    """Map a PyO3 exception to the connector's public error contract.

    - ``KernelError`` → mapped class with structured attrs forwarded.
      Local transfer failures map to built-in ``OSError`` classes with
      their original OS error code and requested filename when available.
    - Already-mapped PEP 249 or local transfer errors → passed through
      unchanged so nested callers preserve their class and attributes.
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
    if isinstance(exc, OSError) and getattr(exc, "error_code", None) in (
        "FILE_TRANSFER_ERROR",
        "FILE_TRANSFER_PERMISSION_ERROR",
    ):
        return exc
    return OperationalError(
        f"Unexpected error from databricks_sql_kernel during {what}: {exc!r}"
    )
