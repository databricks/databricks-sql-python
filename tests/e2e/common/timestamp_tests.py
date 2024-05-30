import datetime

import pytest

from .predicates import compare_dbr_versions, is_thrift_v5_plus, pysql_has_version


class TimestampTestsMixin:
    date_and_expected_results = [
        ("2021-09-30", datetime.date(2021, 9, 30)),
        ("2021-09", datetime.date(2021, 9, 1)),
        ("2021", datetime.date(2021, 1, 1)),
        ("9999-12-31", datetime.date(9999, 12, 31)),
        ("9999-99-31", None),
    ]

    timestamp_and_expected_results = [
        ("2021-09-30 11:27:35.123+04:00", datetime.datetime(2021, 9, 30, 7, 27, 35, 123000)),
        ("2021-09-30 11:27:35+04:00", datetime.datetime(2021, 9, 30, 7, 27, 35)),
        ("2021-09-30 11:27:35.123", datetime.datetime(2021, 9, 30, 11, 27, 35, 123000)),
        ("2021-09-30 11:27:35", datetime.datetime(2021, 9, 30, 11, 27, 35)),
        ("2021-09-30 11:27", datetime.datetime(2021, 9, 30, 11, 27)),
        ("2021-09-30 11", datetime.datetime(2021, 9, 30, 11)),
        ("2021-09-30", datetime.datetime(2021, 9, 30)),
        ("2021-09", datetime.datetime(2021, 9, 1)),
        ("2021", datetime.datetime(2021, 1, 1)),
        ("9999-12-31T15:59:59", datetime.datetime(9999, 12, 31, 15, 59, 59)),
        ("9999-99-31T15:59:59", None),
    ]

    def should_add_timezone(self):
        return pysql_has_version(">=", 2) and is_thrift_v5_plus(self.arguments)

    def maybe_add_timezone_to_timestamp(self, ts):
        """If we're using DBR >= 10.2, then we expect back aware timestamps, so add timezone to `ts`
        Otherwise we have naive timestamps, so no change is needed
        """
        if ts and self.should_add_timezone():
            return ts.replace(tzinfo=datetime.timezone.utc)
        else:
            return ts

    def assertTimestampsEqual(self, result, expected):
        assert result == self.maybe_add_timezone_to_timestamp(expected)

    def multi_query(self, n_rows=10):
        row_sql = "SELECT " + ", ".join(
            ["TIMESTAMP('{}')".format(ts) for (ts, _) in self.timestamp_and_expected_results]
        )
        query = " UNION ALL ".join([row_sql for _ in range(n_rows)])
        expected_matrix = [
            [dt for (_, dt) in self.timestamp_and_expected_results] for _ in range(n_rows)
        ]
        return query, expected_matrix

    def test_timestamps(self):
        with self.cursor({"session_configuration": {"ansi_mode": False}}) as cursor:
            for timestamp, expected in self.timestamp_and_expected_results:
                cursor.execute("SELECT TIMESTAMP('{timestamp}')".format(timestamp=timestamp))
                result = cursor.fetchone()[0]
                self.assertTimestampsEqual(result, expected)

    def test_multi_timestamps(self):
        with self.cursor({"session_configuration": {"ansi_mode": False}}) as cursor:
            query, expected = self.multi_query()
            cursor.execute(query)
            result = cursor.fetchall()
            # We list-ify the rows because PyHive will return a tuple for a row
            assert [list(r) for r in result] == [
                [self.maybe_add_timezone_to_timestamp(ts) for ts in r] for r in expected
            ]

    @pytest.mark.parametrize("date, expected", date_and_expected_results)
    def test_dates(self, date, expected):
        with self.cursor({"session_configuration": {"ansi_mode": False}}) as cursor:
            for date, expected in self.date_and_expected_results:
                cursor.execute("SELECT DATE('{date}')".format(date=date))
                result = cursor.fetchone()[0]
                assert result == expected
