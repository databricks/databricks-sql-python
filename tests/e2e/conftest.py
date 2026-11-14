"""Shared e2e test fixtures.

The most important fixture here is ``_disable_telemetry_for_e2e``, which
eliminates a long-standing source of flakiness in the retry tests.

Background
----------
The retry tests in ``tests/e2e/common/retry_test_mixins.py`` patch urllib3
*globally* (``HTTPSConnectionPool._get_conn`` / ``._validate_conn``) and assert
that the connection's own request was retried exactly N times (e.g.
``assert call_count == 6``). The connector's telemetry feature is a
process-global singleton (``TelemetryClientFactory``) with a shared
``ThreadPoolExecutor`` and a daemon periodic-flush thread. Every ordinary e2e
test (``test_dates``, ``test_decimals``, ...) opens a connection with telemetry
enabled by default, which leaves a real ``TelemetryClient`` plus background
threads/futures registered in that global factory.

Under pytest-xdist, those ordinary tests run in the SAME worker process as the
retry tests. When a retry test installs its global urllib3 mock, an in-flight
or subsequently-fired telemetry POST to ``/telemetry-ext`` from a *previous*
test's lingering client goes through the same mocked pool and inflates the
mock's ``call_count`` past the expected value -> flaky ``AssertionError``. CI
logs confirm ``/telemetry-ext`` retries appearing *inside* the failing retry
test even after that test's own connection disabled telemetry.

Fix
---
Disable telemetry for *every* e2e connection (not just the retry tests) by
force-installing ``NoopTelemetryClient`` for the duration of each test. With no
real ``TelemetryClient`` ever created in the worker, there are no background
telemetry threads/futures to leak into any test's urllib3 mock. This removes
the flake at its source deterministically, rather than trying to drain racy
background threads.

The telemetry suite (``tests/e2e/test_telemetry_e2e.py``) is the one place that
asserts real telemetry behavior; it is marked ``@pytest.mark.serial`` and
manages its own ``TelemetryClientFactory`` state, so this fixture explicitly
skips ``serial``-marked tests.
"""

import pytest

from databricks.sql.telemetry.telemetry_client import (
    NoopTelemetryClient,
    TelemetryClientFactory,
    _TelemetryClientHolder,
)


def _drain_telemetry_factory():
    """Hard-reset the process-global telemetry factory: close clients, stop the
    flush thread, shut the executor down, and clear all state."""
    with TelemetryClientFactory._lock:
        for holder in list(TelemetryClientFactory._clients.values()):
            try:
                holder.client.close()
            except Exception:
                pass
        try:
            TelemetryClientFactory._stop_flush_thread()
        except Exception:
            pass
        if TelemetryClientFactory._executor is not None:
            try:
                TelemetryClientFactory._executor.shutdown(wait=True)
            except Exception:
                pass
        TelemetryClientFactory._clients = {}
        TelemetryClientFactory._executor = None
        TelemetryClientFactory._initialized = False


@pytest.fixture(autouse=True)
def _disable_telemetry_for_e2e(request, monkeypatch):
    """Force every e2e connection to use NoopTelemetryClient so no real
    telemetry background work runs in the worker process.

    Skipped for ``serial``-marked tests (the telemetry suite), which assert
    real telemetry behavior and manage their own factory state.
    """
    if request.node.get_closest_marker("serial") is not None:
        # Let the telemetry suite exercise real telemetry.
        yield
        return

    # Clear any real telemetry state left by an earlier test in this worker,
    # then make all subsequent telemetry initialization a no-op.
    _drain_telemetry_factory()

    def _install_noop(*args, host_url=None, **kwargs):
        host_url = TelemetryClientFactory.getHostUrlSafely(host_url)
        with TelemetryClientFactory._lock:
            TelemetryClientFactory._clients[host_url] = _TelemetryClientHolder(
                NoopTelemetryClient()
            )

    monkeypatch.setattr(
        TelemetryClientFactory,
        "initialize_telemetry_client",
        staticmethod(_install_noop),
    )

    try:
        yield
    finally:
        # Leave the factory clean for the next test regardless of what ran.
        _drain_telemetry_factory()
