"""unsafe_logger payload records are skipped only when nothing could receive them."""

import contextlib
import io
import logging
from unittest.mock import MagicMock, Mock, patch

import pytest

from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.backend import thrift_backend
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions

UNSAFE = thrift_backend.unsafe_logger
PARENT = logging.getLogger("databricks.sql")
ROOT = logging.getLogger()
FORMAT = "%(name)s|%(levelname)s|%(funcName)s|%(message)s"
REQUEST = ttypes.TExecuteStatementReq(statement="SELECT 'é' AS x")
RESPONSE = ttypes.TExecuteStatementResp(
    status=ttypes.TStatus(statusCode=ttypes.TStatusCode.SUCCESS_STATUS)
)


class _Recorder(logging.Handler):
    def __init__(self, out, level=logging.DEBUG):
        super().__init__(level)
        self.out = out
        self.setFormatter(logging.Formatter(FORMAT))

    def emit(self, record):
        self.out.append(("handler", self.format(record)))


class _RecordingNullHandler(logging.NullHandler):
    def __init__(self, out):
        super().__init__()
        self.out = out

    def handle(self, record):
        self.out.append(("null-subclass", record.getMessage()))


@contextlib.contextmanager
def _isolated_logging():
    """Start from the logger setup thrift_backend creates and restore everything after."""
    saved = [
        (lg, list(lg.handlers), list(lg.filters), lg.level, lg.propagate, lg.disabled)
        for lg in (UNSAFE, PARENT, ROOT)
    ]
    saved_globals = (
        logging.lastResort,
        logging.raiseExceptions,
        logging.getLogRecordFactory(),
        UNSAFE.manager.emittedNoHandlerWarning,
        logging.root.manager.disable,
    )
    for lg in (UNSAFE, PARENT, ROOT):
        lg.handlers[:] = []
        lg.filters[:] = []
        lg.disabled = False
    UNSAFE.addHandler(logging.NullHandler())
    UNSAFE.setLevel(logging.DEBUG)
    UNSAFE.propagate = False
    UNSAFE.manager.emittedNoHandlerWarning = False
    logging.disable(logging.NOTSET)
    try:
        yield
    finally:
        for lg, handlers, filters, level, propagate, disabled in saved:
            lg.handlers[:] = handlers
            lg.filters[:] = filters
            lg.setLevel(level)
            lg.propagate = propagate
            lg.disabled = disabled
        logging.lastResort, logging.raiseExceptions = saved_globals[:2]
        logging.setLogRecordFactory(saved_globals[2])
        UNSAFE.manager.emittedNoHandlerWarning = saved_globals[3]
        logging.disable(saved_globals[4])


def _propagate_to(logger, level):
    def setup(out):
        UNSAFE.propagate = True
        logger.addHandler(_Recorder(out, level))

    return setup


def _no_handlers(last_resort_level):
    def setup(out):
        UNSAFE.handlers[:] = []
        logging.lastResort = (
            None if last_resort_level is None else _Recorder(out, last_resort_level)
        )
        logging.raiseExceptions = True

    return setup


def _logger_filter(out):
    def record(r):
        out.append(("filter", r.getMessage()))
        return True

    UNSAFE.addFilter(record)


def _record_factory(out):
    def factory(*args, **kwargs):
        out.append(("factory", args[0]))
        return logging.LogRecord(*args, **kwargs)

    logging.setLogRecordFactory(factory)


# Each case configures logging the way a user might, with the number of payload
# records the gated path should still build (main always builds 2 when enabled).
CASES = {
    "default_null_handler_only": (lambda out: None, 0),
    "handler_on_logger": (lambda out: UNSAFE.addHandler(_Recorder(out)), 2),
    "handler_on_logger_at_info": (
        lambda out: UNSAFE.addHandler(_Recorder(out, logging.INFO)),
        0,
    ),
    "propagate_to_root_debug": (_propagate_to(ROOT, logging.DEBUG), 2),
    "propagate_to_parent_debug": (_propagate_to(PARENT, logging.DEBUG), 2),
    "propagate_to_root_warning": (_propagate_to(ROOT, logging.WARNING), 0),
    "root_debug_without_propagation": (
        lambda out: ROOT.addHandler(_Recorder(out)),
        0,
    ),
    "logger_filter": (_logger_filter, 2),
    "null_handler_subclass": (
        lambda out: UNSAFE.addHandler(_RecordingNullHandler(out)),
        2,
    ),
    "logger_level_info": (
        lambda out: (UNSAFE.setLevel(logging.INFO), UNSAFE.addHandler(_Recorder(out))),
        0,
    ),
    "logging_disable": (
        lambda out: (UNSAFE.addHandler(_Recorder(out)), logging.disable(logging.DEBUG)),
        0,
    ),
    "logger_disabled": (
        lambda out: (UNSAFE.addHandler(_Recorder(out)), setattr(UNSAFE, "disabled", 1)),
        0,
    ),
    "no_handlers_last_resort_debug": (_no_handlers(logging.DEBUG), 2),
    "no_handlers_last_resort_warning": (_no_handlers(logging.WARNING), 0),
    # The one-time "No handlers could be found" warning consumes only the first.
    "no_handlers_no_last_resort": (_no_handlers(None), 1),
    "custom_record_factory": (_record_factory, 2),
}


@patch("databricks.sql.auth.thrift_http_client.THttpClient")
def _make_request(_t_http_client_class):
    client = ThriftDatabricksClient(
        "foobar",
        443,
        "path",
        [],
        auth_provider=AuthProvider(),
        ssl_options=SSLOptions(),
        http_client=MagicMock(),
    )
    method = Mock(return_value=RESPONSE)
    method.__name__ = "ExecuteStatement"
    client.make_request(method, REQUEST)


def _observe(case, gate):
    """Run one request with ``gate`` deciding whether to log, and collect what any
    handler, filter, record factory, lastResort or the no-handler warning saw."""
    out = []
    stderr = io.StringIO()
    with _isolated_logging():
        CASES[case][0](out)
        with patch.object(
            thrift_backend, "_unsafe_logger_has_receiver", gate
        ), patch.object(
            UNSAFE, "makeRecord", wraps=UNSAFE.makeRecord
        ) as make_record, patch(
            "sys.stderr", stderr
        ):
            _make_request()
        out.append(("stderr", stderr.getvalue()))
        out.append(("warned", UNSAFE.manager.emittedNoHandlerWarning))
    return out, make_record.call_count


@pytest.mark.parametrize("case", list(CASES))
def test_gated_logging_matches_unconditional_logging(case):
    # ``lambda: True`` is main's behavior: always call unsafe_logger.debug.
    main_out, main_records = _observe(case, lambda: True)
    gated_out, gated_records = _observe(
        case, thrift_backend._unsafe_logger_has_receiver
    )

    assert gated_out == main_out
    assert gated_records == CASES[case][1]
    nothing_received = main_out == [("stderr", ""), ("warned", False)]
    assert nothing_received == (gated_records == 0)
    if gated_records:
        assert main_records == 2


def test_handler_on_logger_gets_both_payload_records():
    out = []
    with _isolated_logging():
        UNSAFE.addHandler(_Recorder(out))
        _make_request()
    prefix = "databricks.sql.unsafe|DEBUG|attempt_request|"
    assert out == [
        ("handler", prefix + "Sending request: " + str(REQUEST)),
        ("handler", prefix + "Received response: " + str(RESPONSE)),
    ]


def test_custom_logger_class_is_always_called():
    class CustomLogger(logging.Logger):
        def debug(self, msg, *args, **kwargs):
            calls.append(msg % args)

    calls = []
    custom = CustomLogger("custom")
    custom.setLevel(logging.CRITICAL)
    with patch.object(thrift_backend, "unsafe_logger", custom):
        _make_request()
    assert calls == [
        "Sending request: " + str(REQUEST),
        "Received response: " + str(RESPONSE),
    ]
