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


def _thrift_loaded_after_importing(module_name: str) -> bool:
    """Import ``module_name`` in a clean subprocess and report whether the
    top-level ``thrift`` package ended up in ``sys.modules``."""
    script = textwrap.dedent(
        f"""
        import sys
        import {module_name}  # noqa: F401
        # Exit code 1 == thrift was imported, 0 == it was not.
        sys.exit(1 if "thrift" in sys.modules else 0)
        """
    )
    result = subprocess.run([sys.executable, "-c", script])
    if result.returncode not in (0, 1):
        raise AssertionError(
            f"subprocess importing {module_name} failed with exit code "
            f"{result.returncode}"
        )
    return result.returncode == 1


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
    assert not _thrift_loaded_after_importing(module_name), (
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
    breaks."""
    assert _thrift_loaded_after_importing("databricks.sql.backend.thrift_backend"), (
        "The Thrift backend no longer imports the 'thrift' package; the Thrift "
        "code path is likely broken."
    )
