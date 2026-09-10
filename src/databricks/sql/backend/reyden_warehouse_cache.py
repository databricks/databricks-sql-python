"""Process-wide cache of warehouses known to reject the legacy Thrift protocol.

A Reyden / Real-Time SQL warehouse rejects a Thrift ``OpenSession`` — the SQL
Gateway proxy stamps SQLSTATE ``KP001`` on the rejection. When the driver
auto-recovers by re-opening on the kernel backend, it records the warehouse
here so later connections to the same warehouse skip the doomed Thrift attempt
and open on the kernel directly.

Keyed by ``(host, warehouse_id)``. Warehouse ids are globally unique, so the
warehouse id alone identifies the warehouse — even on a SPOG host shared by many
workspaces (where only the ``?o=<workspace-id>`` path param distinguishes them),
there is no cross-workspace collision. The host is kept in the key only as a
cheap optimization (scoping lookups) and defense-in-depth, not for correctness.
Entries expire after ``_TTL_SECONDS`` so a warehouse later reconfigured to accept
Thrift is eventually retried.
"""

import re
import threading
import time
from typing import Dict, Optional, Tuple

# A warehouse's Reyden membership can change (an id may be recreated on a
# Thrift-capable endpoint), so cached entries are re-validated after this long.
# Matches the ADBC driver's 6-hour horizon.
_TTL_SECONDS = 6 * 60 * 60

# Warehouse paths look like ``/sql/1.0/warehouses/<id>`` or
# ``.../endpoints/<id>``; the id stops at the next ``/``, ``?`` or ``&`` (e.g. a
# ``?o=`` SPOG routing param). All-purpose-compute cluster paths carry no
# warehouse id and never match — they are never Reyden warehouses.
_WAREHOUSE_PATH_RE = re.compile(r".*/(?:warehouses|endpoints)/([^?&/]+)")


def extract_warehouse_id(http_path: Optional[str]) -> Optional[str]:
    """Return the warehouse/endpoint id embedded in ``http_path``, or ``None``."""
    if not http_path:
        return None
    match = _WAREHOUSE_PATH_RE.match(http_path)
    return match.group(1) if match else None


class _ReydenWarehouseCache:
    def __init__(self, ttl_seconds: float = _TTL_SECONDS) -> None:
        self._ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        # (host_lowercased, warehouse_id) -> monotonic expiry deadline
        self._expiry: Dict[Tuple[str, str], float] = {}

    @staticmethod
    def _key(host: str, warehouse_id: str) -> Tuple[str, str]:
        return (host.lower(), warehouse_id)

    def mark_reyden(self, host: str, warehouse_id: str) -> None:
        now = time.monotonic()
        with self._lock:
            # Opportunistic sweep: mark_reyden only runs on an actual Thrift
            # rejection (rare), so purging every expired entry here is near-free
            # and bounds the cache to warehouses seen within the TTL window
            # rather than every warehouse ever seen (the per-key lazy eviction
            # in is_known_reyden never reclaims a warehouse that is not looked
            # up again).
            for key in [k for k, deadline in self._expiry.items() if deadline <= now]:
                del self._expiry[key]
            self._expiry[self._key(host, warehouse_id)] = now + self._ttl_seconds

    def is_known_reyden(self, host: str, warehouse_id: str) -> bool:
        key = self._key(host, warehouse_id)
        now = time.monotonic()
        with self._lock:
            deadline = self._expiry.get(key)
            if deadline is None:
                return False
            if deadline <= now:
                # Lazily evict so a reconfigured warehouse is retried over Thrift.
                del self._expiry[key]
                return False
            return True

    def clear(self) -> None:
        with self._lock:
            self._expiry.clear()


# Process-wide singleton; multi-tenant safe via the host component of the key.
_CACHE = _ReydenWarehouseCache()


def mark_reyden(host: str, warehouse_id: str) -> None:
    """Record that ``warehouse_id`` on ``host`` rejects the Thrift protocol."""
    _CACHE.mark_reyden(host, warehouse_id)


def is_known_reyden(host: str, warehouse_id: str) -> bool:
    """Whether ``warehouse_id`` on ``host`` is known (unexpired) to reject Thrift."""
    return _CACHE.is_known_reyden(host, warehouse_id)


def clear_cache() -> None:
    """Reset the cache. Intended for tests."""
    _CACHE.clear()
