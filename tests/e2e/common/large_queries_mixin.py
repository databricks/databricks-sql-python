import logging
import math
import time

log = logging.getLogger(__name__)


class LargeQueriesMixin:
    """
    This mixin expects to be mixed with a CursorTest-like class
    """

    def fetch_rows(self, cursor, row_count, fetchmany_size):
        """
        A generator for rows. Fetches until the end or up to 5 minutes.
        """
        # TODO: Remove fetchmany_size when we have fixed the performance issues with fetchone
        # in the Python client
        max_fetch_time = 5 * 60  # Fetch for at most 5 minutes

        rows = self.get_some_rows(cursor, fetchmany_size)
        start_time = time.time()
        n = 0
        while rows:
            for row in rows:
                n += 1
                yield row
            if time.time() - start_time >= max_fetch_time:
                log.warning("Fetching rows timed out")
                break
            rows = self.get_some_rows(cursor, fetchmany_size)
        if not rows:
            # Read all the rows, row_count should match
            self.assertEqual(n, row_count)

        num_fetches = max(math.ceil(n / 10000), 1)
        latency_ms = int((time.time() - start_time) * 1000 / num_fetches), 1
        print(
            "Fetched {} rows with an avg latency of {} per fetch, ".format(n, latency_ms)
            + "assuming 10K fetch size."
        )

    def test_query_with_large_wide_result_set(self):
        resultSize = 300 * 1000 * 1000  # 300 MB
        width = 8192  # B
        rows = resultSize // width
        cols = width // 36

        # Set the fetchmany_size to get 10MB of data a go
        fetchmany_size = 10 * 1024 * 1024 // width
        # This is used by PyHive tests to determine the buffer size
        self.arraysize = 1000
        with self.cursor() as cursor:
            for lz4_compression in [False, True]:
                cursor.connection.lz4_compression = lz4_compression
                uuids = ", ".join(["uuid() uuid{}".format(i) for i in range(cols)])
                cursor.execute(
                    "SELECT id, {uuids} FROM RANGE({rows})".format(uuids=uuids, rows=rows)
                )
                assert lz4_compression == cursor.active_result_set.lz4_compressed
                for row_id, row in enumerate(self.fetch_rows(cursor, rows, fetchmany_size)):
                    assert row[0] == row_id  # Verify no rows are dropped in the middle.
                    assert len(row[1]) == 36

    def test_query_with_large_narrow_result_set(self):
        resultSize = 300 * 1000 * 1000  # 300 MB
        width = 8  # sizeof(long)
        rows = resultSize / width

        # Set the fetchmany_size to get 10MB of data a go
        fetchmany_size = 10 * 1024 * 1024 // width
        # This is used by PyHive tests to determine the buffer size
        self.arraysize = 10000000
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM RANGE({rows})".format(rows=rows))
            for row_id, row in enumerate(self.fetch_rows(cursor, rows, fetchmany_size)):
                assert row[0] == row_id

    def test_long_running_query(self):
        """Incrementally increase query size until it takes at least 5 minutes,
        and asserts that the query completes successfully.
        """
        minutes = 60
        min_duration = 5 * minutes

        duration = -1
        scale0 = 10000
        scale_factor = 1
        with self.cursor() as cursor:
            while duration < min_duration:
                assert scale_factor < 512, "Detected infinite loop"
                start = time.time()

                cursor.execute(
                    """SELECT count(*)
                        FROM RANGE({scale}) x
                        JOIN RANGE({scale0}) y
                        ON from_unixtime(x.id * y.id, "yyyy-MM-dd") LIKE "%not%a%date%" 
                        """.format(
                        scale=scale_factor * scale0, scale0=scale0
                    )
                )

                (n,) = cursor.fetchone()
                assert n == 0

                duration = time.time() - start
                current_fraction = duration / min_duration
                print("Took {} s with scale factor={}".format(duration, scale_factor))
                # Extrapolate linearly to reach 5 min and add 50% padding to push over the limit
                scale_factor = math.ceil(1.5 * scale_factor / current_fraction)
