"""Unit tests for ResultHeartbeatManager."""

import threading
import time
import unittest
from unittest.mock import Mock

from databricks.sql.backend.thrift_result_heartbeat_manager import (
    ResultHeartbeatManager,
)
from databricks.sql.thrift_api.TCLIService import ttypes


def _resp(state):
    return Mock(operationState=state)


def _wait_until(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return False


class ResultHeartbeatManagerTest(unittest.TestCase):
    def _make_manager(self, backend, interval_seconds=0.01):
        return ResultHeartbeatManager(
            backend=backend,
            op_handle=Mock(name="op_handle"),
            interval_seconds=interval_seconds,
            statement_id_hex="test-stmt",
        )

    # ---- lifecycle ----------------------------------------------------

    def test_start_spawns_daemon_thread_that_polls(self):
        backend = Mock()
        backend._heartbeat_poll.return_value = _resp(
            ttypes.TOperationState.RUNNING_STATE
        )
        mgr = self._make_manager(backend)
        mgr.start()

        try:
            self.assertTrue(
                _wait_until(lambda: backend._heartbeat_poll.call_count >= 2),
                "heartbeat thread did not poll twice in 2s",
            )
            self.assertTrue(mgr._thread.daemon)
            self.assertTrue(mgr._thread.is_alive())
        finally:
            mgr.stop()

        self.assertFalse(mgr._thread.is_alive())

    def test_stop_before_start_is_safe(self):
        mgr = self._make_manager(Mock())
        mgr.stop()  # must not raise

    def test_stop_twice_is_idempotent(self):
        backend = Mock()
        backend._heartbeat_poll.return_value = _resp(
            ttypes.TOperationState.RUNNING_STATE
        )
        mgr = self._make_manager(backend)
        mgr.start()
        mgr.stop()
        mgr.stop()  # must not raise

    def test_double_start_is_logged_warning_no_op(self):
        backend = Mock()
        backend._heartbeat_poll.return_value = _resp(
            ttypes.TOperationState.RUNNING_STATE
        )
        mgr = self._make_manager(backend)
        mgr.start()
        first_thread = mgr._thread
        try:
            mgr.start()  # must not raise
            self.assertIs(mgr._thread, first_thread)
        finally:
            mgr.stop()

    # ---- terminal-state self-stop ------------------------------------

    def test_terminal_state_self_stops_the_thread(self):
        for state in (
            ttypes.TOperationState.CLOSED_STATE,
            ttypes.TOperationState.CANCELED_STATE,
            ttypes.TOperationState.ERROR_STATE,
            ttypes.TOperationState.TIMEDOUT_STATE,
            ttypes.TOperationState.UKNOWN_STATE,
        ):
            with self.subTest(state=state):
                backend = Mock()
                backend._heartbeat_poll.return_value = _resp(state)
                mgr = self._make_manager(backend)
                mgr.start()
                self.assertTrue(
                    _wait_until(lambda: not mgr._thread.is_alive()),
                    f"thread did not self-stop on terminal state {state}",
                )

    def test_finished_state_keeps_polling(self):
        backend = Mock()
        backend._heartbeat_poll.return_value = _resp(
            ttypes.TOperationState.FINISHED_STATE
        )
        mgr = self._make_manager(backend)
        mgr.start()
        try:
            self.assertTrue(
                _wait_until(lambda: backend._heartbeat_poll.call_count >= 3),
                "FINISHED_STATE should NOT terminate the heartbeat",
            )
            self.assertTrue(mgr._thread.is_alive())
        finally:
            mgr.stop()

    # ---- failure counter ---------------------------------------------

    def test_self_stops_after_max_consecutive_failures(self):
        backend = Mock()
        backend._heartbeat_poll.side_effect = RuntimeError("boom")
        mgr = self._make_manager(backend)
        mgr.start()
        self.assertTrue(
            _wait_until(lambda: not mgr._thread.is_alive(), timeout=3.0),
            "thread did not self-stop after consecutive failures",
        )
        self.assertEqual(
            backend._heartbeat_poll.call_count,
            ResultHeartbeatManager.MAX_CONSECUTIVE_FAILURES,
        )

    def test_success_resets_failure_counter(self):
        backend = Mock()
        seq = [RuntimeError("x")] * 5 + [_resp(ttypes.TOperationState.RUNNING_STATE)]
        backend._heartbeat_poll.side_effect = (
            seq + [_resp(ttypes.TOperationState.RUNNING_STATE)] * 10
        )

        mgr = self._make_manager(backend)
        mgr.start()
        try:
            self.assertTrue(
                _wait_until(lambda: backend._heartbeat_poll.call_count >= len(seq) + 2),
                "polling did not continue past the recovery point",
            )
            self.assertTrue(mgr._thread.is_alive())
            self.assertEqual(mgr._consecutive_failures, 0)
        finally:
            mgr.stop()


if __name__ == "__main__":
    unittest.main()
