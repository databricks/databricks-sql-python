import pytest

from databricks.sql.backend import reyden_warehouse_cache
from databricks.sql.backend.reyden_warehouse_cache import (
    _ReydenWarehouseCache,
    extract_warehouse_id,
)


class TestExtractWarehouseId:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/sql/1.0/warehouses/abc123", "abc123"),
            ("/sql/1.0/endpoints/def456", "def456"),
            ("/sql/1.0/warehouses/abc123?o=42", "abc123"),
            ("sql/1.0/warehouses/wh?param=1&o=2", "wh"),
            # All-purpose-compute cluster path — no warehouse id.
            ("/sql/protocolv1/o/1234567890/0101-cluster", None),
            ("", None),
            (None, None),
        ],
    )
    def test_extract(self, path, expected):
        assert extract_warehouse_id(path) == expected


class TestReydenWarehouseCacheClass:
    def test_mark_then_known(self):
        cache = _ReydenWarehouseCache()
        assert cache.is_known_reyden("host", "wh") is False
        cache.mark_reyden("host", "wh")
        assert cache.is_known_reyden("host", "wh") is True

    def test_host_case_insensitive(self):
        cache = _ReydenWarehouseCache()
        cache.mark_reyden("Host.Example.COM", "wh")
        assert cache.is_known_reyden("host.example.com", "wh") is True

    def test_distinct_hosts_do_not_collide(self):
        cache = _ReydenWarehouseCache()
        cache.mark_reyden("host-a", "wh")
        # Same warehouse id on a different host must not be treated as Reyden.
        assert cache.is_known_reyden("host-b", "wh") is False

    def test_distinct_warehouses_do_not_collide(self):
        cache = _ReydenWarehouseCache()
        cache.mark_reyden("host", "wh-a")
        assert cache.is_known_reyden("host", "wh-b") is False

    def test_entry_expires_and_is_evicted(self):
        cache = _ReydenWarehouseCache(ttl_seconds=0)
        cache.mark_reyden("host", "wh")
        # A zero TTL means the deadline is already in the past on read.
        assert cache.is_known_reyden("host", "wh") is False
        # Expired entry is evicted, not just reported absent.
        assert cache._expiry == {}

    def test_mark_sweeps_expired_entries(self):
        # A zero TTL makes the first entry expired by the time the second mark
        # runs, so the opportunistic sweep must purge it even though it was
        # never read back.
        cache = _ReydenWarehouseCache(ttl_seconds=0)
        cache.mark_reyden("host", "old")
        assert ("host", "old") in cache._expiry
        cache.mark_reyden("host", "new")
        assert ("host", "old") not in cache._expiry
        assert ("host", "new") in cache._expiry

    def test_mark_keeps_live_entries(self):
        # Live (unexpired) entries survive the sweep on a subsequent mark.
        cache = _ReydenWarehouseCache(ttl_seconds=3600)
        cache.mark_reyden("host", "a")
        cache.mark_reyden("host", "b")
        assert ("host", "a") in cache._expiry
        assert ("host", "b") in cache._expiry


class TestModuleSingleton:
    def test_mark_and_clear(self):
        reyden_warehouse_cache.clear_cache()
        reyden_warehouse_cache.mark_reyden("host", "wh")
        assert reyden_warehouse_cache.is_known_reyden("host", "wh") is True
        reyden_warehouse_cache.clear_cache()
        assert reyden_warehouse_cache.is_known_reyden("host", "wh") is False
