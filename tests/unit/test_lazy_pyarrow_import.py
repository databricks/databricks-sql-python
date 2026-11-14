"""pyarrow (and the numpy it imports) load on the first result, not in connect().

Each check runs in a fresh interpreter because ``sys.modules`` is process-global.
"""

import subprocess
import sys
import textwrap

import pytest

from databricks.sql.common import optional_pyarrow as optional_pyarrow_module
from databricks.sql.common.optional_pyarrow import optional_pyarrow

pytest.importorskip("pyarrow")

# Sentinel exit codes, distinct from the 1 of an uncaught exception.
_CLEAN, _LOADED = 10, 11
UNWANTED = ["pyarrow", "numpy", "pandas"]

# Stand-in for the kernel wheel so a kernel connect() runs without a server.
_FAKE_KERNEL = """
import sys
import types
from unittest.mock import MagicMock

fake = types.ModuleType("databricks_sql_kernel")
fake.KernelError = type("KernelError", (Exception,), {})
fake.Session = MagicMock()
fake.Session.return_value.session_id = "01ef-session"
sys.modules["databricks_sql_kernel"] = fake
"""


def _run(body: str) -> subprocess.CompletedProcess:
    script = _FAKE_KERNEL + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120
    )


@pytest.mark.parametrize(
    "module_name",
    [
        "databricks.sql",
        "databricks.sql.client",
        "databricks.sql.utils",
        "databricks.sql.result_set",
        "databricks.sql.backend.kernel.client",
    ],
)
def test_connect_path_modules_do_not_import_pyarrow(module_name):
    # The kernel wheel is faked and pyarrow is present, so any import error is
    # a real failure (exit 1), never a skip.
    proc = _run(f"""
        import {module_name}  # noqa: F401
        loaded = [m for m in {UNWANTED!r} if m in sys.modules]
        print(loaded)
        sys.exit({_LOADED} if loaded else {_CLEAN})
        """)
    assert proc.returncode != _LOADED, f"importing {module_name} loaded {proc.stdout}"
    assert (
        proc.returncode == _CLEAN
    ), f"unexpected exit {proc.returncode}:\n{proc.stderr}"


def test_kernel_connect_defers_pyarrow_to_the_first_result():
    proc = _run(f"""
        from databricks import sql

        conn = sql.connect(
            server_hostname="example.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            access_token="dapi-xyz",
            use_kernel=True,
        )
        loaded = [m for m in {UNWANTED!r} if m in sys.modules]
        assert not loaded, f"connect() loaded {{loaded}}"
        handle = MagicMock()
        handle.arrow_schema.side_effect = RuntimeError("stop after import")
        try:
            conn.session.backend._make_result_set(handle, conn.cursor(), None)
        except Exception as e:
            # Only the sentinel may stop it (the result set wraps it).
            cause = e
            while cause is not None and str(cause) != "stop after import":
                cause = cause.__cause__ or cause.__context__
            if cause is None:
                raise
        assert "pyarrow" in sys.modules, "the first result imports pyarrow"
        """)
    assert proc.returncode == 0, proc.stderr


def test_without_pyarrow_the_optional_globals_are_none():
    proc = _run("""
        sys.modules["pyarrow"] = None  # makes `import pyarrow` fail
        import databricks.sql.client as client
        import databricks.sql.result_set as result_set
        import databricks.sql.utils as utils

        assert client.pyarrow is None
        assert utils.pyarrow is None
        assert result_set.pyarrow is None
        """)
    assert proc.returncode == 0, proc.stderr


def test_lazy_pyarrow_resolves_to_the_real_module():
    proc = _run("""
        from databricks.sql.common.optional_pyarrow import optional_pyarrow

        lazy = optional_pyarrow()
        assert "pyarrow" not in sys.modules and lazy is not None
        assert lazy.Table is sys.modules["pyarrow"].Table
        import pyarrow

        assert isinstance(pyarrow.table({"a": [1]}), lazy.Table)
        assert optional_pyarrow() is pyarrow
        """)
    assert proc.returncode == 0, proc.stderr


def test_first_use_rebinds_the_modules_to_the_real_pyarrow():
    # After the first read, hot-path reads go to the real module, not the
    # stand-in's __getattr__.
    proc = _run("""
        import databricks.sql.client as client
        import databricks.sql.result_set as result_set
        import databricks.sql.utils as utils

        stand_in = utils.pyarrow
        assert "pyarrow" not in sys.modules
        assert all(m.pyarrow is stand_in for m in (client, result_set))
        stand_in.Table
        real = sys.modules["pyarrow"]
        assert all(m.pyarrow is real for m in (client, utils, result_set))
        assert stand_in.Table is real.Table
        """)
    assert proc.returncode == 0, proc.stderr


def test_rebinding_leaves_a_replaced_global_alone(monkeypatch):
    stand_in = optional_pyarrow_module._LazyPyarrow("pyarrow")
    replaced, holding = {"pyarrow": "patched"}, {"pyarrow": stand_in}
    monkeypatch.setattr(optional_pyarrow_module, "_NAMESPACES", [replaced, holding])
    import pyarrow

    assert stand_in.Table is pyarrow.Table
    assert holding["pyarrow"] is pyarrow
    assert replaced["pyarrow"] == "patched"


def test_stand_in_reads_the_live_module(monkeypatch):
    # No copies on the stand-in, so patching pyarrow applies through it too.
    import pyarrow

    stand_in = optional_pyarrow_module._LazyPyarrow("pyarrow")
    assert stand_in.concat_tables is pyarrow.concat_tables
    patched = object()
    monkeypatch.setattr(pyarrow, "concat_tables", patched)
    assert stand_in.concat_tables is patched


def test_optional_pyarrow_is_none_when_not_installed(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    monkeypatch.setattr(
        optional_pyarrow_module.importlib.util, "find_spec", lambda name: None
    )
    assert optional_pyarrow() is None


def test_unimportable_pyarrow_raises_attribute_error(monkeypatch):
    attempts = []

    def broken(name):
        attempts.append(name)
        raise ImportError("numpy ABI mismatch")

    monkeypatch.setattr(optional_pyarrow_module.importlib, "import_module", broken)
    lazy = optional_pyarrow_module._LazyPyarrow("pyarrow")
    assert not hasattr(lazy, "Table")
    assert getattr(lazy, "Table", None) is None
    with pytest.raises(AttributeError, match="numpy ABI mismatch") as raised:
        lazy.Table
    assert isinstance(raised.value.__cause__, ImportError)
    assert attempts == ["pyarrow"], "a failed import is not retried"
