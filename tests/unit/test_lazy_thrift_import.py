"""Regression tests locking in lazy loading of the Apache Thrift runtime.

The Python SQL connector historically imported the PyPI ``thrift`` package
eagerly whenever ``connect()`` (i.e. ``databricks.sql.client``) was imported,
regardless of which backend the caller selected. Build systems that vendor
their own ``thrift`` (e.g. Meta's Buck) then hit a namespace collision even on
the SEA / kernel code paths, which never speak Thrift on the wire.

These tests assert that importing the connector and the non-Thrift backends
does NOT import ``thrift``, while the Thrift backend still does. The invariant
is easy to regress silently -- a single top-level ``from ...ttypes import X``
in any module on the ``client.py`` import chain re-poisons the whole path -- so
each check runs in a *fresh* subprocess interpreter (``sys.modules`` is
process-global; other tests in the same process may already have imported
thrift, which would mask a regression).
"""

import subprocess
import sys
import textwrap

import pytest

# Sentinel exit codes emitted by the child *only after* the import completes,
# so an interpreter crash / uncaught exception (Python's generic exit code 1)
# can never be misread as a definitive "thrift (not) loaded" answer.
_EXIT_IMPORTED_NO_THRIFT = 10
_EXIT_IMPORTED_WITH_THRIFT = 11
_EXIT_IMPORT_FAILED = 12


class _ImportProbeResult:
    """Outcome of importing a module in a clean subprocess."""

    def __init__(self, returncode: int, stderr: str):
        self.returncode = returncode
        self.stderr = stderr

    @property
    def imported(self) -> bool:
        return self.returncode in (
            _EXIT_IMPORTED_NO_THRIFT,
            _EXIT_IMPORTED_WITH_THRIFT,
        )

    @property
    def thrift_loaded(self) -> bool:
        return self.returncode == _EXIT_IMPORTED_WITH_THRIFT

    @property
    def import_failed(self) -> bool:
        return self.returncode == _EXIT_IMPORT_FAILED


def _probe_import(module_name: str) -> _ImportProbeResult:
    """Import ``module_name`` in a clean subprocess and report, via a dedicated
    sentinel exit code, whether the top-level ``thrift`` package ended up in
    ``sys.modules`` -- distinguishing that from an import failure (e.g. a
    missing optional dependency such as pyarrow), which is reported separately
    rather than being conflated with "thrift was imported"."""
    script = textwrap.dedent(
        f"""
        import sys

        try:
            import {module_name}  # noqa: F401
        except BaseException:
            import traceback
            traceback.print_exc()
            sys.exit({_EXIT_IMPORT_FAILED})

        sys.exit(
            {_EXIT_IMPORTED_WITH_THRIFT}
            if "thrift" in sys.modules
            else {_EXIT_IMPORTED_NO_THRIFT}
        )
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    if proc.returncode not in (
        _EXIT_IMPORTED_NO_THRIFT,
        _EXIT_IMPORTED_WITH_THRIFT,
        _EXIT_IMPORT_FAILED,
    ):
        raise AssertionError(
            f"subprocess importing {module_name!r} exited with unexpected code "
            f"{proc.returncode}. stderr:\n{proc.stderr}"
        )
    return _ImportProbeResult(proc.returncode, proc.stderr)


# Modules on the connect()/execute() path that must stay Thrift-free so the
# SEA and kernel backends can be used without the ``thrift`` package present.
THRIFT_FREE_MODULES = [
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sql.session",
    "databricks.sql.utils",
    "databricks.sql.parameters.native",
    "databricks.sql.backend.types",
    "databricks.sql.backend.databricks_client",
    "databricks.sql.backend.sea.backend",
    "databricks.sql.backend.sea.queue",
    "databricks.sql.backend.kernel.type_mapping",
    "databricks.sql.backend.kernel.client",
    "databricks.sql.cloudfetch.downloader",
    "databricks.sql.cloudfetch.download_manager",
]


@pytest.mark.parametrize("module_name", THRIFT_FREE_MODULES)
def test_module_does_not_import_thrift(module_name):
    """Importing the connector and its non-Thrift backends must not import the
    Apache Thrift runtime.

    This is what unblocks callers (e.g. Buck-based builds) that ship their own
    ``thrift`` package and use only the SEA or kernel backend.
    """
    result = _probe_import(module_name)

    if result.import_failed:
        # A module that can't even be imported in this environment (typically a
        # missing *optional* dependency, e.g. pyarrow in the "default deps" CI
        # job) can't leak thrift. Skip rather than fail so this test stays
        # focused on the thrift invariant and doesn't double as an
        # optional-dependency presence check.
        pytest.skip(
            f"{module_name!r} could not be imported in this environment "
            f"(likely a missing optional dependency); import error:\n"
            f"{result.stderr}"
        )

    assert not result.thrift_loaded, (
        f"Importing {module_name!r} pulled in the top-level 'thrift' package. "
        f"Something on this import chain grew a module-level "
        f"'from databricks.sql.thrift_api...' / 'import thrift' statement (or a "
        f"non-deferred type annotation). Move it under TYPE_CHECKING (with "
        f"'from __future__ import annotations') or into a function body on the "
        f"Thrift-only code path."
    )


def test_thrift_backend_still_imports_thrift():
    """Sanity check the counterpart invariant: the Thrift backend legitimately
    depends on the Thrift runtime, so it must still import it. This guards
    against a future 'fix' that hides thrift so aggressively the Thrift path
    breaks.

    A failure to import the module (as opposed to importing it without thrift)
    is surfaced explicitly rather than being treated as a pass."""
    result = _probe_import("databricks.sql.backend.thrift_backend")

    assert result.imported, (
        "The Thrift backend could not be imported at all -- the Thrift code "
        f"path is broken. Import error:\n{result.stderr}"
    )
    assert result.thrift_loaded, (
        "The Thrift backend no longer imports the 'thrift' package; the Thrift "
        "code path is likely broken."
    )
