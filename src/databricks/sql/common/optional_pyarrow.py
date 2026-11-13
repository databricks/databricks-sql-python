"""``pyarrow`` for modules that treat it as optional, imported on first use.

``connect()`` imports these modules, but pyarrow is only needed once results are
read. Importing it also imports numpy, which starts OpenBLAS's thread pool (one
busy-waiting thread per core), so it shouldn't be part of connecting.

One case differs from importing pyarrow eagerly: when pyarrow is installed but
fails to import (e.g. a numpy ABI mismatch). These modules then treat it as
available and fail when an attribute is first used, while the Thrift and SEA
backends, which import pyarrow when they load, treat it as missing as before.
"""

import importlib
import importlib.util
import sys
import types
from typing import Any, Dict, List, Optional

# Module globals whose ``pyarrow`` is the stand-in, rebound on first use.
_NAMESPACES: List[Dict[str, Any]] = []


class _LazyPyarrow(types.ModuleType):
    """Stands in for the ``pyarrow`` module until an attribute is used.

    The first use rebinds ``pyarrow`` to the real module in every registered
    namespace still holding the stand-in, so reads on the hot path afterwards
    are plain module attribute lookups.
    """

    def __getattr__(self, name: str) -> Any:
        try:
            module = importlib.import_module("pyarrow")
        except ImportError as e:
            # AttributeError, so hasattr() and getattr(..., default) still work.
            raise AttributeError(f"pyarrow could not be imported: {e}") from e
        for namespace in _NAMESPACES:
            if namespace.get("pyarrow") is self:
                namespace["pyarrow"] = module
        value = getattr(module, name)
        setattr(self, name, value)
        return value


_LAZY_PYARROW = _LazyPyarrow("pyarrow")


def optional_pyarrow(
    namespace: Optional[Dict[str, Any]] = None,
) -> Optional[types.ModuleType]:
    """``pyarrow``, or None when it isn't installed, without importing it.

    Returns the real module if it is already imported, otherwise a stand-in that
    imports it on first attribute access. Whether pyarrow is installed is decided
    with ``find_spec``, so ``pyarrow is None`` checks behave as before. Pass the
    caller's ``globals()`` to have its ``pyarrow`` replaced by the real module
    once the stand-in is first used.
    """
    module = sys.modules.get("pyarrow")
    if module is not None:
        return module
    try:
        installed = importlib.util.find_spec("pyarrow") is not None
    except (ImportError, ValueError):
        installed = False
    if not installed:
        return None
    if namespace is not None:
        _NAMESPACES.append(namespace)
    return _LAZY_PYARROW
