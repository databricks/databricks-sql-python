import os
import ssl
import threading
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from urllib3.util.ssl_ import create_urllib3_context

import databricks.sql.backend.thrift_backend  # noqa: F401  (loads thrift's THttpClient)
from databricks.sql.auth.common import ClientContext
from databricks.sql.auth.thrift_http_client import THttpClient
from databricks.sql.common import ssl_context_cache
from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.types import SSLOptions


def counting_builder():
    built = []

    def build():
        context = Mock(spec=ssl.SSLContext)
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
            context = Mock(spec=ssl.SSLContext)
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

    def test_cache_is_bounded(self, monkeypatch):
        monkeypatch.setattr(ssl_context_cache, "_MAX_ENTRIES", 2)
        build, built = counting_builder()

        for kind in ("a", "b", "c"):
            ssl_context_cache.get_ssl_context(kind, SSLOptions(), build)

        assert len(ssl_context_cache._contexts) == 2
        assert ssl_context_cache.get_ssl_context("a", SSLOptions(), build) is not (
            built[0]
        )


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

        assert pool.cert_reqs == cert_reqs
        assert context.verify_mode == expected.verify_mode
        assert context.check_hostname == expected.check_hostname
        assert context.options == expected.options
        assert context.verify_flags == expected.verify_flags
        assert context.minimum_version == expected.minimum_version
        assert context.hostname_checks_common_name == (
            expected.hostname_checks_common_name
        )
        assert context.cert_store_stats()["x509_ca"] > 0

    def test_connections_share_the_context(self):
        first, _ = self.pool_context(SSLOptions())
        second, _ = self.pool_context(SSLOptions())
        unverified, _ = self.pool_context(SSLOptions(tls_verify=False))

        assert first is second
        assert unverified is not first
        assert unverified.verify_mode == ssl.CERT_NONE
        assert first.verify_mode == ssl.CERT_REQUIRED

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
