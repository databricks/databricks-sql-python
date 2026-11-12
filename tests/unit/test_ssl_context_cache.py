import os
import ssl
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from urllib3.util.ssl_ import create_urllib3_context

import databricks.sql.backend.thrift_backend  # noqa: F401  (loads thrift's THttpClient)
from databricks.sql.auth.common import ClientContext
from databricks.sql.auth.thrift_http_client import THttpClient
from databricks.sql.common import ssl_context_cache
from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.exc import ProgrammingError
from databricks.sql.types import SSLOptions


def bare_context():
    # Real but cheap: no CA bundle is loaded.
    return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


def counting_builder():
    built = []

    def build():
        context = bare_context()
        built.append(context)
        return context

    return build, built


class TestGetSslContext:
    def test_identical_options_share_one_context(self):
        build, built = counting_builder()

        first = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)
        second = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)

        assert first is second
        assert len(built) == 1

    @pytest.mark.parametrize(
        "options",
        [
            SSLOptions(tls_verify=False),
            SSLOptions(tls_verify_hostname=False),
            SSLOptions(tls_trusted_ca_file="/nonexistent/ca.pem"),
        ],
    )
    def test_different_options_do_not_share(self, options):
        build, built = counting_builder()

        default = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)
        other = ssl_context_cache.get_ssl_context("k", options, build)

        assert default is not other
        assert len(built) == 2

    def test_kinds_do_not_share(self):
        build, built = counting_builder()

        a = ssl_context_cache.get_ssl_context("a", SSLOptions(), build)
        b = ssl_context_cache.get_ssl_context("b", SSLOptions(), build)

        assert a is not b

    @pytest.mark.parametrize(
        "options",
        [
            SSLOptions(tls_client_cert_file="cert.pem"),
            SSLOptions(tls_client_cert_file="cert.pem", tls_client_cert_key_file="k"),
            SSLOptions(tls_client_cert_key_file="key.pem"),
        ],
    )
    def test_client_certificates_are_never_cached(self, options):
        build, built = counting_builder()

        first = ssl_context_cache.get_ssl_context("k", options, build)
        second = ssl_context_cache.get_ssl_context("k", options, build)

        assert first is not second
        assert len(built) == 2

    def test_rewritten_ca_file_gets_a_new_context(self, tmp_path):
        ca_file = tmp_path / "ca.pem"
        ca_file.write_text("one")
        options = SSLOptions(tls_trusted_ca_file=str(ca_file))
        build, built = counting_builder()

        before = ssl_context_cache.get_ssl_context("k", options, build)
        ca_file.write_text("rotated")
        os.utime(ca_file, ns=(0, 10**9))
        after = ssl_context_cache.get_ssl_context("k", options, build)

        assert before is not after
        assert ssl_context_cache.get_ssl_context("k", options, build) is after

    @pytest.mark.skipif(
        sys.platform == "win32", reason="st_ctime is the creation time on Windows"
    )
    def test_in_place_rewrite_keeping_mtime_gets_a_new_context(self, tmp_path):
        ca_file = tmp_path / "ca.pem"
        ca_file.write_text("old")
        original = ca_file.stat()
        options = SSLOptions(tls_trusted_ca_file=str(ca_file))
        build, built = counting_builder()
        before = ssl_context_cache.get_ssl_context("k", options, build)

        time.sleep(0.05)  # filesystem timestamps can be jiffy-granular
        ca_file.write_text("new")  # same inode and size, like `cp -p`
        os.utime(ca_file, ns=(original.st_atime_ns, original.st_mtime_ns))
        rewritten = ca_file.stat()

        assert (rewritten.st_ino, rewritten.st_size, rewritten.st_mtime_ns) == (
            original.st_ino,
            original.st_size,
            original.st_mtime_ns,
        )
        assert ssl_context_cache.get_ssl_context("k", options, build) is not before

    def test_default_bundle_env_change_gets_a_new_context(self, tmp_path, monkeypatch):
        bundle = tmp_path / "bundle.pem"
        bundle.write_text("x")
        build, built = counting_builder()

        default = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)
        monkeypatch.setenv("SSL_CERT_FILE", str(bundle))
        overridden = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)

        assert default is not overridden

    def test_failed_build_is_not_cached(self):
        build, built = counting_builder()
        failing = Mock(side_effect=FileNotFoundError("ca.pem"))

        with pytest.raises(FileNotFoundError):
            ssl_context_cache.get_ssl_context("k", SSLOptions(), failing)
        context = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)

        assert context is built[0]

    def test_concurrent_first_use_builds_once(self):
        built = []

        def slow_build():
            time.sleep(0.05)
            context = bare_context()
            built.append(context)
            return context

        threads = 32
        barrier = threading.Barrier(threads)
        results = []

        def worker():
            barrier.wait()
            results.append(
                ssl_context_cache.get_ssl_context("k", SSLOptions(), slow_build)
            )

        workers = [threading.Thread(target=worker) for _ in range(threads)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()

        assert len(built) == 1
        assert len(results) == threads
        assert all(r is built[0] for r in results)

    def test_slow_build_does_not_block_other_keys(self):
        build, built = counting_builder()
        cached = ssl_context_cache.get_ssl_context("cached", SSLOptions(), build)
        release = threading.Event()

        def stuck_build():
            release.wait(10)
            return bare_context()

        stuck = threading.Thread(
            target=ssl_context_cache.get_ssl_context,
            args=("stuck", SSLOptions(), stuck_build),
        )
        stuck.start()
        try:
            results = {}

            def lookups():
                results["hit"] = ssl_context_cache.get_ssl_context(
                    "cached", SSLOptions(), build
                )
                results["other"] = ssl_context_cache.get_ssl_context(
                    "other", SSLOptions(), build
                )

            other = threading.Thread(target=lookups)
            other.start()
            other.join(timeout=2)

            assert not other.is_alive()
            assert results["hit"] is cached
            assert results["other"] is built[1]
        finally:
            release.set()
            stuck.join()

    def test_expired_context_is_rebuilt(self, monkeypatch):
        now = [1000.0]
        monkeypatch.setattr(
            ssl_context_cache, "time", SimpleNamespace(monotonic=lambda: now[0])
        )
        build, built = counting_builder()

        first = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)
        now[0] += ssl_context_cache._TTL_SECONDS
        assert ssl_context_cache.get_ssl_context("k", SSLOptions(), build) is first
        now[0] += 1
        rebuilt = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)

        assert rebuilt is not first
        assert ssl_context_cache.get_ssl_context("k", SSLOptions(), build) is rebuilt
        assert len(built) == 2

    def test_cache_is_bounded(self, monkeypatch):
        monkeypatch.setattr(ssl_context_cache, "_MAX_ENTRIES", 2)
        build, built = counting_builder()

        for kind in ("a", "b", "c"):
            ssl_context_cache.get_ssl_context(kind, SSLOptions(), build)

        assert len(ssl_context_cache._contexts) == 2
        assert ssl_context_cache.get_ssl_context("a", SSLOptions(), build) is not (
            built[0]
        )

    def test_non_stdlib_contexts_are_not_cached(self):
        build = Mock(side_effect=lambda: Mock(spec=ssl.SSLContext))

        first = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)
        second = ssl_context_cache.get_ssl_context("k", SSLOptions(), build)

        assert first is not second
        assert not ssl_context_cache._contexts

    def test_shared_context_skips_same_value_writes(self):
        context = ssl_context_cache.get_ssl_context("k", SSLOptions(), bare_context)
        assert isinstance(context, ssl.SSLContext)

        with patch.object(ssl.SSLContext, "_set_alpn_protocols") as set_alpn:
            context.set_alpn_protocols(["http/1.1"])  # what urllib3 sends
            set_alpn.assert_not_called()
            context.set_alpn_protocols(["h2"])
            set_alpn.assert_called_once()

        real = ssl.SSLContext.verify_mode
        writes = []

        def spy_set(self, value):
            writes.append(value)
            real.fset(self, value)

        context.check_hostname = False
        with patch.object(ssl.SSLContext, "verify_mode", property(real.fget, spy_set)):
            context.verify_mode = ssl.CERT_REQUIRED
            assert writes == []
            context.verify_mode = ssl.CERT_NONE
            assert writes == [ssl.CERT_NONE]
        assert context.verify_mode == ssl.CERT_NONE

    def test_reset_after_fork_releases_locks(self):
        held = threading.Event()
        release = threading.Event()

        def hold_locks():
            with ssl_context_cache._lock:
                held.set()
                release.wait(10)

        holder = threading.Thread(target=hold_locks)
        holder.start()
        held.wait(5)
        stuck_build_lock = threading.Lock()
        stuck_build_lock.acquire()
        key = ssl_context_cache._cache_key("k", SSLOptions())
        ssl_context_cache._build_locks[key] = stuck_build_lock
        try:
            ssl_context_cache._reset_after_fork()  # as in a forked child

            done = threading.Thread(
                target=ssl_context_cache.get_ssl_context,
                args=("k", SSLOptions(), bare_context),
            )
            done.start()
            done.join(timeout=2)
            assert not done.is_alive()
        finally:
            release.set()
            holder.join()


class TestThriftTransportContext:
    @staticmethod
    def pool_context(ssl_options):
        client = THttpClient(
            auth_provider=None,
            uri_or_host="https://example.com/sql/1.0/warehouses/abc",
            ssl_options=ssl_options,
        )
        client.open()
        pool = client._THttpClient__pool
        assert pool.ca_certs is None and pool.cert_file is None
        return pool.conn_kw["ssl_context"], pool

    @pytest.mark.parametrize(
        "tls_verify, cert_reqs", [(True, ssl.CERT_REQUIRED), (False, ssl.CERT_NONE)]
    )
    def test_context_matches_what_urllib3_builds(self, tls_verify, cert_reqs):
        context, pool = self.pool_context(SSLOptions(tls_verify=tls_verify))
        expected = create_urllib3_context(cert_reqs=cert_reqs)
        expected.load_default_certs()

        assert pool.cert_reqs == cert_reqs
        assert context.verify_mode == expected.verify_mode
        assert context.check_hostname == expected.check_hostname
        assert context.options == expected.options
        assert context.verify_flags == expected.verify_flags
        assert context.minimum_version == expected.minimum_version
        assert context.hostname_checks_common_name == (
            expected.hostname_checks_common_name
        )
        assert context.cert_store_stats() == expected.cert_store_stats()

    def test_connections_share_the_context(self):
        first, _ = self.pool_context(SSLOptions())
        second, _ = self.pool_context(SSLOptions())
        unverified, _ = self.pool_context(SSLOptions(tls_verify=False))

        assert first is second
        assert unverified is not first
        assert unverified.verify_mode == ssl.CERT_NONE
        assert first.verify_mode == ssl.CERT_REQUIRED

    def test_invalid_client_identity_fails_at_construction(self):
        with pytest.raises(ProgrammingError, match="requires tls_client_cert_file"):
            THttpClient(
                auth_provider=None,
                uri_or_host="https://example.com/sql/1.0/warehouses/abc",
                ssl_options=SSLOptions(tls_client_cert_key_file="key.pem"),
            )

    def test_construction_without_client_identity_loads_no_context(self):
        with patch("databricks.sql.types.create_default_context") as create:
            THttpClient(
                auth_provider=None,
                uri_or_host="https://example.com/sql/1.0/warehouses/abc",
                ssl_options=SSLOptions(),
            )

        create.assert_not_called()

    def test_proxied_pool_shares_the_context(self):
        direct, _ = self.pool_context(SSLOptions())
        with patch(
            "databricks.sql.auth.thrift_http_client.detect_and_parse_proxy",
            return_value=("http://proxy.example.com:3128", None),
        ):
            proxied, pool = self.pool_context(SSLOptions())

        assert pool.proxy is not None and pool.proxy.host == "proxy.example.com"
        assert proxied is direct

    def test_custom_ca_file_replaces_default_certs(self, tmp_path):
        ca_file = tmp_path / "ca.pem"
        ca_file.write_text("pem")
        context = MagicMock()
        with patch(
            "databricks.sql.auth.thrift_http_client.create_urllib3_context",
            return_value=context,
        ) as create:
            pooled, _ = self.pool_context(SSLOptions(tls_trusted_ca_file=str(ca_file)))

        assert pooled is context
        create.assert_called_once_with(cert_reqs=ssl.CERT_REQUIRED)
        context.load_verify_locations.assert_called_once_with(str(ca_file))
        context.load_default_certs.assert_not_called()


class TestUnifiedHttpClientContext:
    @staticmethod
    def client(ssl_options):
        return UnifiedHttpClient(
            ClientContext(hostname="https://example.com", ssl_options=ssl_options)
        )

    @staticmethod
    def pool_context(client):
        return client._direct_pool_manager.connection_pool_kw["ssl_context"]

    def test_clients_share_the_context(self):
        first = self.pool_context(self.client(SSLOptions()))
        second = self.pool_context(self.client(SSLOptions()))

        assert first is second
        assert first.verify_mode == ssl.CERT_REQUIRED and first.check_hostname

    def test_different_options_get_their_own_context(self):
        verified = self.pool_context(self.client(SSLOptions()))
        no_hostname = self.pool_context(
            self.client(SSLOptions(tls_verify_hostname=False))
        )
        unverified = self.pool_context(self.client(SSLOptions(tls_verify=False)))

        assert len({id(verified), id(no_hostname), id(unverified)}) == 3
        assert not no_hostname.check_hostname
        assert no_hostname.verify_mode == ssl.CERT_REQUIRED
        assert unverified.verify_mode == ssl.CERT_NONE
