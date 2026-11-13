"""pandas must load on the first row conversion, not when connect() is imported.

Each check runs in a fresh interpreter because ``sys.modules`` is process-global.
"""

import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("pyarrow")

CONNECT_PATH_MODULES = [
    "databricks.sql.client",
    "databricks.sql.result_set",
    "databricks.sql.backend.kernel.client",
    "databricks.sql.backend.kernel.result_set",
    "databricks.sql.backend.sea.backend",
]


def _run(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
    )


# Sentinel exit codes, distinct from the 1 of an uncaught exception.
_NO_PANDAS, _PANDAS_LOADED, _IMPORT_FAILED = 10, 11, 12


@pytest.mark.parametrize("module_name", CONNECT_PATH_MODULES)
def test_connect_path_does_not_import_pandas(module_name):
    proc = _run(f"""
        import sys
        try:
            import {module_name}  # noqa: F401
        except BaseException:
            import traceback
            traceback.print_exc()
            sys.exit({_IMPORT_FAILED})
        sys.exit({_PANDAS_LOADED} if "pandas" in sys.modules else {_NO_PANDAS})
        """)
    if proc.returncode == _IMPORT_FAILED:
        pytest.skip(f"{module_name} not importable here:\n{proc.stderr}")
    assert proc.returncode != _PANDAS_LOADED, f"importing {module_name} imported pandas"
    assert (
        proc.returncode == _NO_PANDAS
    ), f"unexpected exit {proc.returncode}:\n{proc.stderr}"


def test_row_conversion_imports_pandas_on_first_use():
    proc = _run("""
        import sys
        from unittest.mock import MagicMock
        import pyarrow
        from databricks.sql.result_set import ResultSet

        assert "pandas" not in sys.modules
        rs = MagicMock()
        rs.connection.disable_pandas = False
        rs.description = [("a",), ("b",)]
        table = pyarrow.table({"a": [1, None], "b": ["x", "y"]})
        rows = ResultSet._convert_arrow_table(rs, table)
        assert "pandas" in sys.modules
        assert [tuple(r) for r in rows] == [(1, "x"), (None, "y")], rows
        """)
    assert proc.returncode == 0, proc.stderr
