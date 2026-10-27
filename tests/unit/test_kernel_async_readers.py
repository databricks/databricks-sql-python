"""Public async cursor reads through the native kernel and a local HTTP server."""

import base64
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

pa = pytest.importorskip("pyarrow")
pytestmark = pytest.mark.realkernel


@pytest.fixture(params=[False, True], ids=["standard", "realtime"])
def async_connection(request):
    kernel = pytest.importorskip("databricks_sql_kernel")
    assert getattr(
        kernel, "__file__", None
    ), "A fake module is shadowing the native kernel"
    assert callable(
        getattr(kernel.ExecutedAsyncStatement, "reader", None)
    ), "These tests require the kernel source revision in KERNEL_REV"

    from databricks import sql

    sessions_path = "/api/2.0/sql/warehouses/test-wh/sessions"
    base_path = f"{sessions_path}/test-session" if request.param else "/api/2.0/sql"
    statement_path = f"{base_path}/statements/test-statement"
    state = SimpleNamespace(
        requests=[],
        statement_path=statement_path,
        hold_next=False,
        started=threading.Event(),
        release=threading.Event(),
        lock=threading.Lock(),
        errors=[],
    )
    chunks = []
    for index, values in enumerate(([1, 2], [3, 4])):
        batch = pa.record_batch([pa.array(values, type=pa.int64())], names=["n"])
        buffer = pa.BufferOutputStream()
        with pa.ipc.new_stream(buffer, batch.schema) as writer:
            writer.write_batch(batch)
        chunk = {
            "chunk_index": index,
            "row_offset": index * 2,
            "row_count": 2,
            "attachment": base64.b64encode(buffer.getvalue()).decode(),
        }
        if index == 0:
            chunk["next_chunk_index"] = 1
        chunks.append(chunk)
    result = {
        "statement_id": "test-statement",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 1,
                "columns": [
                    {
                        "name": "n",
                        "type_name": "BIGINT",
                        "type_text": "BIGINT",
                        "position": 0,
                    }
                ],
            },
            "total_chunk_count": 2,
            "total_row_count": 4,
        },
        "result": chunks[0],
    }

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            payload = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            body = json.loads(payload) if payload else None
            path = urlsplit(self.path).path
            with state.lock:
                state.requests.append((self.command, path, body))
                hold = (
                    self.command == "GET" and path == statement_path and state.hold_next
                )
                if hold:
                    state.hold_next = False
            if hold:
                state.started.set()
                if not state.release.wait(10):
                    state.errors.append("Timed out waiting to release the owner poll")

            status = 200
            realtime = False
            if self.command == "GET" and path.startswith("/api/2.0/connector-service/"):
                response = {"flags": []}
            elif self.command == "POST" and path == sessions_path:
                response = {"session_id": "test-session"}
                realtime = request.param
            elif self.command == "POST" and path == f"{base_path}/statements":
                response = {
                    "statement_id": "test-statement",
                    "status": {"state": "PENDING"},
                }
            elif self.command == "GET" and path == statement_path:
                response = result
            elif self.command == "GET" and path == f"{statement_path}/result/chunks/1":
                response = chunks[1]
            elif self.command == "DELETE":
                response = {}
            else:
                status = 404
                response = {
                    "error_code": "NOT_FOUND",
                    "message": "statement unavailable",
                }
            data = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Content-Type", "application/json")
            if realtime:
                self.send_header("x-databricks-rt-session", "true")
            self.end_headers()
            self.wfile.write(data)

        do_POST = handle_request
        do_GET = handle_request
        do_DELETE = handle_request

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        conn = None
        try:
            conn = sql.connect(
                server_hostname=f"http://127.0.0.1:{server.server_port}",
                http_path="/sql/1.0/warehouses/test-wh",
                access_token="test-token",
                use_kernel=True,
                enable_telemetry=False,
            )
            yield conn, state
        finally:
            state.release.set()
            if conn is not None:
                conn.close()
            server.shutdown()
            worker.join(timeout=5)
        assert not state.errors
        assert (
            sum(
                method == "DELETE" and path == statement_path
                for method, path, _ in state.requests
            )
            == 1
        )


def _assert_read_routes(state):
    assert {
        path
        for method, path, _ in state.requests
        if method == "GET" and path.startswith("/api/2.0/sql/")
    } == {
        state.statement_path,
        f"{state.statement_path}/result/chunks/1",
    }
    assert not any(method == "DELETE" for method, _, _ in state.requests)
    execute_bodies = [
        body
        for method, path, body in state.requests
        if method == "POST" and path.endswith("/statements")
    ]
    assert len(execute_bodies) == 1
    assert execute_bodies[0]["wait_timeout"] == "0s"


def test_async_cursor_refetch_preserves_session(async_connection):
    from databricks.sql.backend.types import CommandState

    conn, state = async_connection
    cur = conn.cursor()
    cur.execute_async("SELECT id AS n FROM range(1, 5)")
    for _ in range(2):
        cur.get_async_execution_result()
        assert cur.fetchall_arrow().to_pydict() == {"n": [1, 2, 3, 4]}
        assert cur.get_query_state() == CommandState.SUCCEEDED
    _assert_read_routes(state)


def test_async_cursor_reads_overlap_owner_poll(async_connection):
    from databricks.sql.backend.types import CommandState

    conn, state = async_connection
    cur = conn.cursor()
    cur.execute_async("SELECT id AS n FROM range(1, 5)")
    state.hold_next = True
    with ThreadPoolExecutor(max_workers=1) as pool:
        monitor = pool.submit(cur.get_query_state)
        try:
            assert state.started.wait(5), "Owner poll did not reach the server"
            assert cur.get_query_state() == CommandState.SUCCEEDED
            cur.get_async_execution_result()
            assert cur.fetchall_arrow().to_pydict() == {"n": [1, 2, 3, 4]}
            _assert_read_routes(state)
        finally:
            state.release.set()
        assert monitor.result(timeout=5) == CommandState.SUCCEEDED
