import logging
import math
import time

log = logging.getLogger(__name__)


def fetch_rows(test_case, cursor, row_count, fetchmany_size):
    """
    A generator for rows. Fetches until the end or up to 5 minutes.
    """
    max_fetch_time = 5 * 60  # Fetch for at most 5 minutes

    rows = _get_some_rows(cursor, fetchmany_size)
    start_time = time.time()
    n = 0
    while rows:
        for row in rows:
            n += 1
            yield row
        if time.time() - start_time >= max_fetch_time:
            log.warning("Fetching rows timed out")
            break
        rows = _get_some_rows(cursor, fetchmany_size)
    if not rows:
        # Read all the rows, row_count should match
        test_case.assertEqual(n, row_count)

    num_fetches = max(math.ceil(n / 10000), 1)
    latency_ms = int((time.time() - start_time) * 1000 / num_fetches), 1
    print(
        "Fetched {} rows with an avg latency of {} per fetch, ".format(
            n, latency_ms
        )
        + "assuming 10K fetch size."
    )


def _get_some_rows(cursor, fetchmany_size):
    row = cursor.fetchone()
    if row:
        return [row]
    else:
        return None
