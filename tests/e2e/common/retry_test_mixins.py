class Client429ResponseMixin:
    def test_client_should_retry_automatically_when_getting_429(self):
        with self.cursor() as cursor:
            for _ in range(10):
                cursor.execute("SELECT 1")
                rows = cursor.fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][0], 1)

    def test_client_should_not_retry_429_if_RateLimitRetry_is_0(self):
        with self.assertRaises(self.error_type) as cm:
            with self.cursor(self.conf_to_disable_rate_limit_retries) as cursor:
                for _ in range(10):
                    cursor.execute("SELECT 1")
                    rows = cursor.fetchall()
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0][0], 1)
        expected = "Maximum rate of 1 requests per SECOND has been exceeded. " \
                   "Please reduce the rate of requests and try again after 1 seconds."
        exception_str = str(cm.exception)

        # FIXME (Ali Smesseim, 7-Jul-2020): ODBC driver does not always return the
        #  X-Thriftserver-Error-Message as-is. Re-enable once Simba resolves this flakiness.
        #  Simba support ticket: https://magnitudesoftware.force.com/support/5001S000018RlaD
        # self.assertIn(expected, exception_str)


class Client503ResponseMixin:
    def test_wait_cluster_startup(self):
        with self.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchall()

    def _test_retry_disabled_with_message(self, error_msg_substring, exception_type):
        with self.assertRaises(exception_type) as cm:
            with self.connection(self.conf_to_disable_temporarily_unavailable_retries):
                pass
        self.assertIn(error_msg_substring, str(cm.exception))
