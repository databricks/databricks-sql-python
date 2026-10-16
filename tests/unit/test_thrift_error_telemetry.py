"""Failure events retain Thrift statement identity without exporting SQL context."""

import json
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from databricks.sql.auth.authenticators import AuthProvider
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.backend.types import CommandId, SessionId
from databricks.sql.exc import (
    DatabaseError,
    DataError,
    OperationalError,
    RequestError,
    ServerOperationError,
)
from databricks.sql.telemetry.telemetry_client import (
    NoopTelemetryClient,
    TelemetryClient,
    TelemetryClientFactory,
)
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions

STATEMENT_ID = "12345678-1234-5678-1234-567812345678"
SESSION_ID = "87654321-4321-8765-4321-876543218765"
SQL_TEXT = "SELECT 'private-statement-value' FROM missing_table"
OK = ttypes.TStatus(statusCode=ttypes.TStatusCode.SUCCESS_STATUS)
ERROR = ttypes.TStatus(
    statusCode=ttypes.TStatusCode.ERROR_STATUS, errorMessage="Statement failed"
)
OP_HANDLE = ttypes.TOperationHandle(
    operationId=ttypes.THandleIdentifier(
        guid=UUID(STATEMENT_ID).bytes, secret=b"private-operation-secret"
    ),
    operationType=ttypes.TOperationType.EXECUTE_STATEMENT,
)


@pytest.fixture
def failure_events():
    with patch("databricks.sql.telemetry.telemetry_client.UnifiedHttpClient"):
        client = TelemetryClient(
            telemetry_enabled=True,
            session_id_hex="another-session-on-this-host",
            auth_provider=AuthProvider(),
            host_url="test-host",
            executor=MagicMock(),
            batch_size=100,
            client_context=MagicMock(),
        )
    with (
        patch.object(
            TelemetryClientFactory, "get_telemetry_client", return_value=client
        ),
        patch.object(client, "_export_event") as export,
    ):
        yield export


@pytest.fixture
def backend():
    with patch(
        "databricks.sql.backend.thrift_backend.TCLIService.Client", autospec=True
    ):
        client = ThriftDatabricksClient(
            "test-host",
            443,
            "/test-path",
            [],
            auth_provider=AuthProvider(),
            ssl_options=SSLOptions(),
            http_client=MagicMock(),
        )
    client._session_id_hex = SESSION_ID
    return client


def execute(backend, async_op=False):
    session = SessionId.from_thrift_handle(
        ttypes.TSessionHandle(
            sessionId=ttypes.THandleIdentifier(
                guid=UUID(SESSION_ID).bytes, secret=b"secret"
            )
        )
    )
    return backend.execute_command(
        SQL_TEXT, session, 100, 100, False, MagicMock(), async_op=async_op
    )


def assert_failure_event(export, statement_id=STATEMENT_ID, session_id=SESSION_ID):
    export.assert_called_once()
    log = export.call_args.args[0]
    event = log.entry.sql_driver_log
    assert event.sql_statement_id == statement_id
    assert event.session_id == session_id
    serialized = log.to_json()
    assert SQL_TEXT not in serialized
    assert "private-statement-value" not in serialized
    assert "private-operation-secret" not in serialized
    assert event.error_info is not None
    return json.loads(event.to_json())


@pytest.mark.parametrize("direct", [True, False], ids=["direct", "polled"])
@pytest.mark.parametrize("display_message", [None, "Statement failed"])
def test_statement_error_has_statement_id(
    backend, failure_events, direct, display_message
):
    status = ttypes.TGetOperationStatusResp(
        status=OK,
        operationState=ttypes.TOperationState.ERROR_STATE,
        errorMessage="Statement failed",
        displayMessage=display_message,
        diagnosticInfo=SQL_TEXT,
    )
    backend._client.ExecuteStatement.return_value = ttypes.TExecuteStatementResp(
        status=OK,
        operationHandle=OP_HANDLE,
        directResults=(
            ttypes.TSparkDirectResults(operationStatus=status) if direct else None
        ),
    )
    backend._client.GetOperationStatus.return_value = status
    with pytest.raises(ServerOperationError, match="Statement failed"):
        execute(backend)
    assert_failure_event(failure_events)


@pytest.mark.parametrize("async_op", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    "field, response_type",
    [
        ("operationStatus", ttypes.TGetOperationStatusResp),
        ("resultSetMetadata", ttypes.TGetResultSetMetadataResp),
        ("resultSet", ttypes.TFetchResultsResp),
        ("closeOperation", ttypes.TCloseOperationResp),
    ],
)
def test_direct_result_error_has_statement_id(
    backend, failure_events, async_op, field, response_type
):
    backend._client.ExecuteStatement.return_value = ttypes.TExecuteStatementResp(
        status=OK,
        operationHandle=OP_HANDLE,
        directResults=ttypes.TSparkDirectResults(
            **{field: response_type(status=ERROR)}
        ),
    )
    with pytest.raises(DatabaseError, match="Statement failed"):
        execute(backend, async_op=async_op)
    assert_failure_event(failure_events)


@pytest.mark.parametrize("has_handle", [True, False])
def test_execute_rpc_error_uses_available_response_handle(
    backend, failure_events, has_handle
):
    backend._client.ExecuteStatement.return_value = ttypes.TExecuteStatementResp(
        status=ERROR, operationHandle=OP_HANDLE if has_handle else None
    )
    with pytest.raises(DatabaseError, match="Statement failed"):
        execute(backend)
    event = assert_failure_event(failure_events, STATEMENT_ID if has_handle else None)
    if not has_handle:
        assert "sql_statement_id" not in event


def test_poll_rpc_error_uses_request_handle(backend, failure_events):
    backend._client.GetOperationStatus.return_value = ttypes.TGetOperationStatusResp(
        status=ERROR
    )
    with pytest.raises(DatabaseError, match="Statement failed"):
        backend._poll_for_status(OP_HANDLE)
    assert_failure_event(failure_events)


def test_poll_request_failure_uses_request_handle(backend, failure_events):
    backend._client.GetOperationStatus.side_effect = RuntimeError("Connection failed")
    with pytest.raises(RequestError):
        backend._poll_for_status(OP_HANDLE)
    assert_failure_event(failure_events)


def test_open_session_error_has_no_statement_id(backend, failure_events):
    backend._session_id_hex = None
    backend._client.OpenSession.return_value = ttypes.TOpenSessionResp(status=ERROR)
    with pytest.raises(DatabaseError, match="Statement failed"):
        backend.open_session({}, None, None)
    event = assert_failure_event(failure_events, None, "another-session-on-this-host")
    assert "sql_statement_id" not in event


def test_statement_error_with_disabled_telemetry(backend):
    backend._client.ExecuteStatement.return_value = ttypes.TExecuteStatementResp(
        status=ERROR, operationHandle=OP_HANDLE
    )
    with (
        patch.object(
            TelemetryClientFactory,
            "get_telemetry_client",
            return_value=NoopTelemetryClient(),
        ),
        patch.object(TelemetryClient, "_export_event") as export,
    ):
        with pytest.raises(DatabaseError, match="Statement failed"):
            execute(backend)
        export.assert_not_called()


def test_execute_request_failure_has_no_statement_id(backend, failure_events):
    backend._client.ExecuteStatement.side_effect = RuntimeError("Connection failed")
    with pytest.raises(RequestError):
        execute(backend)
    event = assert_failure_event(failure_events, None)
    assert "sql_statement_id" not in event


def test_closed_statement_has_statement_id(backend, failure_events):
    status = ttypes.TGetOperationStatusResp(
        status=OK, operationState=ttypes.TOperationState.CLOSED_STATE
    )
    with pytest.raises(DatabaseError, match="unexpectedly closed"):
        backend._check_command_not_in_error_or_closed_state(OP_HANDLE, status)
    assert_failure_event(failure_events)


def test_unexpected_result_format_has_statement_id(backend, failure_events):
    backend._client.ExecuteStatement.return_value = ttypes.TExecuteStatementResp(
        status=OK,
        operationHandle=OP_HANDLE,
        directResults=ttypes.TSparkDirectResults(
            operationStatus=ttypes.TGetOperationStatusResp(
                status=OK, operationState=ttypes.TOperationState.FINISHED_STATE
            ),
            resultSetMetadata=ttypes.TGetResultSetMetadataResp(
                status=OK, resultFormat=ttypes.TSparkRowSetType.ROW_BASED_SET
            ),
        ),
    )
    with pytest.raises(OperationalError, match="Expected results to be"):
        execute(backend)
    assert_failure_event(failure_events)


def test_fetch_offset_error_has_statement_id(backend, failure_events):
    backend._client.FetchResults.return_value = ttypes.TFetchResultsResp(
        status=OK, results=ttypes.TRowSet(startRowOffset=1)
    )
    with pytest.raises(DataError, match="Expected results to start from"):
        backend.fetch_results(
            CommandId.from_thrift_handle(OP_HANDLE),
            max_rows=100,
            max_bytes=100,
            expected_row_start_offset=0,
            lz4_compressed=False,
            arrow_schema_bytes=None,
            description=[],
            chunk_id=0,
        )
    assert_failure_event(failure_events)
