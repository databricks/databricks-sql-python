"""
Background heartbeat for the Thrift backend.

Why this exists
---------------
The warehouse evicts an operation/command handle after roughly 20-25 minutes
of driver idleness. Once that happens, any subsequent TFetchResults against
the handle returns HTTP 404 / RESOURCE_DOES_NOT_EXIST and the result set is
permanently broken — the driver's retry policy classifies the error as
non-retryable.

This manager keeps the handle alive while a consumer is slowly draining
results. While a ThriftResultSet has rows still pending on the server, a
daemon thread issues a periodic GetOperationStatus against the operation
handle. The keepalive stops as soon as the server has finished delivering
data (last TFetchResults returns hasMoreRows=False) or the result set is
closed.

Design mirrors the C# ADBC driver's DatabricksOperationStatusPoller.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

from databricks.sql.thrift_api.TCLIService import ttypes

logger = logging.getLogger(__name__)


class ResultHeartbeatManager:
    """Per-ResultSet background keepalive against operation-handle eviction."""

    DEFAULT_INTERVAL_SECONDS = 60
    DEFAULT_STOP_TIMEOUT_SECONDS = 5.0
    MAX_CONSECUTIVE_FAILURES = 10

    # Operation states that indicate the server has released the handle (or
    # is about to). No point continuing to heartbeat against any of these.
    # FINISHED_STATE is intentionally NOT terminal: it means query execution
    # finished but the handle is still alive for result streaming.
    _TERMINAL_STATES = frozenset(
        {
            ttypes.TOperationState.CANCELED_STATE,
            ttypes.TOperationState.CLOSED_STATE,
            ttypes.TOperationState.ERROR_STATE,
            ttypes.TOperationState.TIMEDOUT_STATE,
            ttypes.TOperationState.UKNOWN_STATE,
        }
    )

    def __init__(
        self,
        *,
        backend,
        op_handle,
        interval_seconds: int,
        statement_id_hex: str,
    ) -> None:
        self._backend = backend
        self._op_handle = op_handle
        self._interval_seconds = interval_seconds
        self._statement_id_hex = statement_id_hex
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._consecutive_failures = 0
        # Successful poll count — exposed for tests / ad-hoc debugging.
        # Intentionally not surfaced through the telemetry pipeline; see the
        # plan's "Telemetry: not added" section for why.
        self._poll_count = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        """
        Spawn the daemon thread. Calling twice is a no-op with a warning —
        not an exception, because this guard sits in ResultSet construction
        and a defensive failure should not abort the user's query.
        """
        with self._lock:
            if self._thread is not None:
                logger.warning(
                    "ResultHeartbeatManager.start() called twice for "
                    "statement %s; ignoring",
                    self._statement_id_hex,
                )
                return
            self._thread = threading.Thread(
                target=self._run,
                name="databricks-sql-heartbeat-%s" % self._statement_id_hex,
                daemon=True,
            )
            self._thread.start()
            logger.debug(
                "heartbeat manager started for statement %s " "(interval=%ss)",
                self._statement_id_hex,
                self._interval_seconds,
            )

    def stop(self, timeout: float = DEFAULT_STOP_TIMEOUT_SECONDS) -> None:
        """
        Signal the loop to exit, then join with a bounded timeout.

        Idempotent. If the join elapses without the thread terminating
        (e.g. wedged in a blocking socket rea_fill_results_bufferd), emit a single DEBUG log
        line and return — the daemon thread will die with the interpreter.
        """
        with self._lock:
            self._stop_event.set()
            thread = self._thread
        if thread is None:
            return
        thread.join(timeout=timeout)
        if thread.is_alive():
            logger.debug(
                "heartbeat thread for statement %s did not terminate "
                "within %ss; letting daemon thread die with interpreter",
                self._statement_id_hex,
                timeout,
            )

    def _run(self) -> None:
        # Event.wait returns True if the event was set during the wait
        # (i.e. stop was signaled), in which case we exit cleanly.
        while not self._stop_event.wait(self._interval_seconds):
            if not self._poll_once():
                return

    def _poll_once(self) -> bool:
        """
        Issue a single GetOperationStatus. Return True to keep polling,
        False to self-stop the manager.
        """
        try:
            resp = self._backend._heartbeat_poll(self._op_handle)
        except Exception as e:
            self._consecutive_failures += 1
            logger.debug(
                "heartbeat poll failed for statement %s "
                "(consecutive_failures=%d): %s",
                self._statement_id_hex,
                self._consecutive_failures,
                e,
            )
            if self._consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                logger.warning(
                    "heartbeat manager stopping after %d consecutive "
                    "failures for statement %s",
                    self._consecutive_failures,
                    self._statement_id_hex,
                )
                return False
            return True

        self._consecutive_failures = 0
        self._poll_count += 1
        state = getattr(resp, "operationState", None)
        state_name = (
            ttypes.TOperationState._VALUES_TO_NAMES.get(state, str(state))
            if state is not None
            else "None"
        )
        logger.debug(
            "heartbeat poll ok for statement %s (state=%s)",
            self._statement_id_hex,
            state_name,
        )
        if state in self._TERMINAL_STATES:
            logger.debug(
                "heartbeat poll for statement %s observed terminal "
                "operation state %s; stopping",
                self._statement_id_hex,
                state_name,
            )
            return False
        return True
