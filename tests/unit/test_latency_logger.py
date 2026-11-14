import itertools
import logging
from unittest.mock import MagicMock, patch

import pytest

from databricks.sql.telemetry.latency_logger import log_latency
from databricks.sql.telemetry.models.enums import StatementType

LOGGER = "databricks.sql.telemetry.latency_logger"


class Cursor:
    """Stand-in for databricks.sql.client.Cursor (telemetry keys on the class name)."""

    def __init__(self, telemetry_enabled):
        self.connection = MagicMock()
        self.connection.telemetry_enabled = telemetry_enabled
        self.connection.get_session_id_hex.return_value = "session-hex"
        self.connection.session.host = "host"
        self.connection.lz4_compression = False
        self.query_id = "statement-id"
        self.active_result_set = None
        self.backend = MagicMock(retry_policy=None)

    @log_latency(StatementType.QUERY)
    def execute(self, value):
        return value

    @log_latency(StatementType.QUERY)
    def fail(self):
        raise ValueError("boom")


@pytest.fixture
def telemetry_client():
    client = MagicMock()
    with patch(
        "databricks.sql.telemetry.latency_logger.TelemetryClientFactory.get_telemetry_client",
        return_value=client,
    ):
        yield client


@pytest.fixture
def monotonic():
    # Each call advances one second, so every timed call measures exactly 1000 ms.
    with patch("databricks.sql.telemetry.latency_logger.time") as fake_time:
        fake_time.monotonic.side_effect = itertools.count(0.0, 1.0)
        yield fake_time.monotonic


@pytest.fixture
def no_debug():
    logger = logging.getLogger(LOGGER)
    level = logger.level
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(level)


def test_disabled_telemetry_without_debug_does_not_time_the_call(
    telemetry_client, monotonic, no_debug, caplog
):
    cursor = Cursor(telemetry_enabled=False)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert cursor.execute(42) == 42
        with pytest.raises(ValueError, match="boom"):
            cursor.fail()
    monotonic.assert_not_called()
    telemetry_client.export_latency_log.assert_not_called()
    assert not caplog.records


def test_debug_logging_records_latency_without_telemetry(
    telemetry_client, monotonic, caplog
):
    cursor = Cursor(telemetry_enabled=False)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        assert cursor.execute(1) == 1
    assert monotonic.call_count == 2
    assert [r.getMessage() for r in caplog.records] == ["execute completed in 1000ms"]
    telemetry_client.export_latency_log.assert_not_called()


@pytest.mark.parametrize("debug", [False, True])
def test_enabled_telemetry_exports_latency_even_on_error(
    telemetry_client, monotonic, no_debug, debug, caplog
):
    cursor = Cursor(telemetry_enabled=True)
    with caplog.at_level(logging.DEBUG if debug else logging.INFO, logger=LOGGER):
        assert cursor.execute("ok") == "ok"
        with pytest.raises(ValueError, match="boom"):
            cursor.fail()
    assert monotonic.call_count == 4
    assert telemetry_client.export_latency_log.call_count == 2
    kwargs = telemetry_client.export_latency_log.call_args.kwargs
    assert kwargs["latency_ms"] == 1000
    assert kwargs["session_id"] == "session-hex"
    assert kwargs["sql_statement_id"] == "statement-id"
    assert kwargs["sql_execution_event"].statement_type == StatementType.QUERY
    assert len(caplog.records) == (2 if debug else 0)


def test_object_without_connection_is_not_exported(
    telemetry_client, monotonic, no_debug
):
    class Handler:
        @log_latency(StatementType.QUERY)
        def run(self):
            return "done"

    assert Handler().run() == "done"
    monotonic.assert_not_called()
    telemetry_client.export_latency_log.assert_not_called()


def test_wrapper_keeps_the_method_metadata():
    assert Cursor.execute.__name__ == "execute"
    assert Cursor.execute.__wrapped__(Cursor(False), 3) == 3
