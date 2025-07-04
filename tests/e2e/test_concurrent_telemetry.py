import threading
from unittest.mock import patch, MagicMock

from databricks.sql.client import Connection
from databricks.sql.telemetry.telemetry_client import TelemetryClientFactory, TelemetryClient
from databricks.sql.thrift_backend import ThriftBackend
from databricks.sql.utils import ExecuteResponse
from databricks.sql.thrift_api.TCLIService.ttypes import TSessionHandle, TOperationHandle, TOperationState, THandleIdentifier

try:
    import pyarrow as pa
except ImportError:
    pa = None


def run_in_threads(target, num_threads, pass_index=False):
    """Helper to run target function in multiple threads."""
    threads = [
        threading.Thread(target=target, args=(i,) if pass_index else ())
        for i in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


class MockArrowQueue:
    """Mock queue that behaves like ArrowQueue but returns empty results."""
    
    def __init__(self):
        # Create an empty arrow table if pyarrow is available, otherwise use None
        if pa is not None:
            self.empty_table = pa.table({'column': pa.array([])})
        else:
            # Create a simple mock table-like object
            self.empty_table = MagicMock()
            self.empty_table.num_rows = 0
            self.empty_table.num_columns = 0
    
    def next_n_rows(self, num_rows: int):
        """Return empty results."""
        return self.empty_table
    
    def remaining_rows(self):
        """Return empty results."""  
        return self.empty_table


def test_concurrent_queries_with_telemetry_capture():
    """
    Test showing concurrent threads executing queries with real telemetry capture.
    Uses the actual Connection and Cursor classes, mocking only the ThriftBackend.
    """
    num_threads = 5
    captured_telemetry = []
    connections = []  # Store connections to close them later
    connections_lock = threading.Lock()  # Thread safety for connections list

    def mock_send_telemetry(self, events):
        """Capture telemetry events instead of sending them over network."""
        captured_telemetry.extend(events)

    # Clean up any existing state
    if TelemetryClientFactory._executor:
        TelemetryClientFactory._executor.shutdown(wait=True)
    TelemetryClientFactory._clients.clear()
    TelemetryClientFactory._executor = None
    TelemetryClientFactory._initialized = False

    with patch.object(TelemetryClient, '_send_telemetry', mock_send_telemetry):
        # Mock the ThriftBackend to avoid actual network calls
        with patch.object(ThriftBackend, 'open_session') as mock_open_session, \
                patch.object(ThriftBackend, 'execute_command') as mock_execute_command, \
                patch.object(ThriftBackend, 'close_session') as mock_close_session, \
                patch.object(ThriftBackend, 'fetch_results') as mock_fetch_results, \
                patch.object(ThriftBackend, 'close_command') as mock_close_command, \
                patch.object(ThriftBackend, 'handle_to_hex_id') as mock_handle_to_hex_id, \
                patch('databricks.sql.auth.thrift_http_client.THttpClient.open') as mock_transport_open:
            
            # Mock transport.open() to prevent actual network connection
            mock_transport_open.return_value = None
            
            # Set up mock responses with proper structure
            mock_handle_identifier = THandleIdentifier()
            mock_handle_identifier.guid = b'1234567890abcdef' 
            mock_handle_identifier.secret = b'test_secret_1234'
            
            mock_session_handle = TSessionHandle()
            mock_session_handle.sessionId = mock_handle_identifier
            mock_session_handle.serverProtocolVersion = 1
            
            mock_open_session.return_value = MagicMock(
                sessionHandle=mock_session_handle,
                serverProtocolVersion=1
            )
            
            mock_handle_to_hex_id.return_value = "test-session-id-12345678"
            
            mock_op_handle = TOperationHandle()
            mock_op_handle.operationId = THandleIdentifier()
            mock_op_handle.operationId.guid = b'abcdef1234567890'  
            mock_op_handle.operationId.secret = b'op_secret_abcd'
            
            # Create proper mock arrow_queue with required methods
            mock_arrow_queue = MockArrowQueue()
            
            mock_execute_response = ExecuteResponse(
                arrow_queue=mock_arrow_queue,
                description=[],
                command_handle=mock_op_handle,
                status=TOperationState.FINISHED_STATE,
                has_been_closed_server_side=False,
                has_more_rows=False,
                lz4_compressed=False,
                arrow_schema_bytes=b'',
                is_staging_operation=False
            )
            mock_execute_command.return_value = mock_execute_response
            
            # Mock fetch_results to return empty results
            mock_fetch_results.return_value = (mock_arrow_queue, False)
            
            # Mock close_command to do nothing
            mock_close_command.return_value = None
            
            # Mock close_session to do nothing
            mock_close_session.return_value = None

            def execute_query_worker(thread_id):
                """Each thread creates a connection and executes a query."""
                
                # Create real Connection and Cursor objects
                conn = Connection(
                    server_hostname="test-host",
                    http_path="/test/path",
                    access_token="test-token",
                    enable_telemetry=True
                )
                
                # Thread-safe storage of connection
                with connections_lock:
                    connections.append(conn)
                
                cursor = conn.cursor()
                # This will trigger the @log_latency decorator naturally
                cursor.execute(f"SELECT {thread_id} as thread_id")
                result = cursor.fetchall()
                conn.close()
                    

            run_in_threads(execute_query_worker, num_threads, pass_index=True)

            # We expect at least 2 events per thread (one for open_session and one for execute_command)
            assert len(captured_telemetry) >= num_threads*2
            print(f"Captured telemetry: {captured_telemetry}")
            
            # Verify the decorator was used (check some telemetry events have latency measurement)
            events_with_latency = [
                e for e in captured_telemetry 
                if hasattr(e, 'entry') and hasattr(e.entry, 'sql_driver_log') 
                and e.entry.sql_driver_log.operation_latency_ms is not None
            ]
            assert len(events_with_latency) >= num_threads
            
            # Verify we have events with statement IDs (indicating @log_latency decorator worked)
            events_with_statements = [
                e for e in captured_telemetry 
                if hasattr(e, 'entry') and hasattr(e.entry, 'sql_driver_log') 
                and e.entry.sql_driver_log.sql_statement_id is not None
            ]
            assert len(events_with_statements) >= num_threads
    
    