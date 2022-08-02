from contextlib import contextmanager
from collections import OrderedDict
import datetime
import io
import logging
import os
import sys
import threading
import time
from unittest import loader, skipIf, skipUnless, TestCase, mock
from uuid import uuid4

import numpy as np
import pyarrow
import pytz
import thrift

import databricks.sql as sql
from databricks.sql import STRING, BINARY, NUMBER, DATETIME, DATE, DatabaseError, Error, OperationalError
from tests.e2e.common.predicates import pysql_has_version, pysql_supports_arrow, compare_dbr_versions, is_thrift_v5_plus
from tests.e2e.common.core_tests import CoreTestMixin, SmokeTestMixin
from tests.e2e.common.large_queries_mixin import LargeQueriesMixin
from tests.e2e.common.timestamp_tests import TimestampTestsMixin
from tests.e2e.common.decimal_tests import DecimalTestsMixin
from tests.e2e.common.retry_test_mixins import Client429ResponseMixin, Client503ResponseMixin

log = logging.getLogger(__name__)

# manually decorate DecimalTestsMixin to need arrow support
for name in loader.getTestCaseNames(DecimalTestsMixin, 'test_'):
    fn = getattr(DecimalTestsMixin, name)
    decorated = skipUnless(pysql_supports_arrow(), 'Decimal tests need arrow support')(fn)
    setattr(DecimalTestsMixin, name, decorated)

get_args_from_env = True


class PySQLTestCase(TestCase):
    error_type = Error
    conf_to_disable_rate_limit_retries = {"_retry_stop_after_attempts_count": 1}
    conf_to_disable_temporarily_unavailable_retries = {"_retry_stop_after_attempts_count": 1}

    def __init__(self, method_name):
        super().__init__(method_name)
        # If running in local mode, just use environment variables for params.
        self.arguments = os.environ if get_args_from_env else {}
        self.arraysize = 1000

    def connection_params(self, arguments):
        params = {
            "server_hostname": arguments["host"],
            "http_path": arguments["http_path"],
            **self.auth_params(arguments)
        }

        return params

    def auth_params(self, arguments):
        return {
            "_username": arguments.get("rest_username"),
            "_password": arguments.get("rest_password"),
            "access_token": arguments.get("access_token")
        }

    @contextmanager
    def connection(self, extra_params=()):
        connection_params = dict(self.connection_params(self.arguments), **dict(extra_params))

        log.info("Connecting with args: {}".format(connection_params))
        conn = sql.connect(**connection_params)

        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def cursor(self, extra_params=()):
        with self.connection(extra_params) as conn:
            cursor = conn.cursor(arraysize=self.arraysize)
            try:
                yield cursor
            finally:
                cursor.close()

    def assertEqualRowValues(self, actual, expected):
        self.assertEqual(len(actual) if actual else 0, len(expected) if expected else 0)
        for act, exp in zip(actual, expected):
            self.assertSequenceEqual(act, exp)


class PySQLLargeQueriesSuite(PySQLTestCase, LargeQueriesMixin):
    def get_some_rows(self, cursor, fetchmany_size):
        row = cursor.fetchone()
        if row:
            return [row]
        else:
            return None


# Exclude Retry tests because they require specific setups, and LargeQueries too slow for core
# tests
class PySQLCoreTestSuite(SmokeTestMixin, CoreTestMixin, DecimalTestsMixin, TimestampTestsMixin,
                         PySQLTestCase):
    validate_row_value_type = True
    validate_result = True

    # An output column in description evaluates to equal to multiple types
    # - type code returned by the client as string.
    # - also potentially a PEP-249 object like NUMBER, DATETIME etc.
    def expected_column_types(self, type_):
        type_mappings = {
            'boolean': ['boolean', NUMBER],
            'byte': ['tinyint', NUMBER],
            'short': ['smallint', NUMBER],
            'integer': ['int', NUMBER],
            'long': ['bigint', NUMBER],
            'decimal': ['decimal', NUMBER],
            'timestamp': ['timestamp', DATETIME],
            'date': ['date', DATE],
            'binary': ['binary', BINARY],
            'string': ['string', STRING],
            'array': ['array'],
            'struct': ['struct'],
            'map': ['map'],
            'double': ['double', NUMBER],
            'null': ['null']
        }
        return type_mappings[type_]

    def test_queries(self):
        if not self._should_have_native_complex_types():
            array_type = str
            array_val = "[1,2,3]"
            struct_type = str
            struct_val = "{\"a\":1,\"b\":2}"
            map_type = str
            map_val = "{1:2,3:4}"
        else:
            array_type = np.ndarray
            array_val = np.array([1, 2, 3])
            struct_type = dict
            struct_val = {"a": 1, "b": 2}
            map_type = list
            map_val = [(1, 2), (3, 4)]

        null_type = "null" if float(sql.__version__[0:2]) < 2.0 else "string"
        self.range_queries = CoreTestMixin.range_queries + [
            ("NULL", null_type, type(None), None),
            ("array(1, 2, 3)", 'array', array_type, array_val),
            ("struct(1 as a, 2 as b)", 'struct', struct_type, struct_val),
            ("map(1, 2, 3, 4)", 'map', map_type, map_val),
        ]

        self.run_tests_on_queries({})

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_incorrect_query_throws_exception(self):
        with self.cursor({}) as cursor:
            # Syntax errors should contain the invalid SQL
            with self.assertRaises(DatabaseError) as cm:
                cursor.execute("^ FOO BAR")
            self.assertIn("FOO BAR", str(cm.exception))

            # Database error should contain the missing database
            with self.assertRaises(DatabaseError) as cm:
                cursor.execute("USE foo234823498ydfsiusdhf")
            self.assertIn("foo234823498ydfsiusdhf", str(cm.exception))

            # SQL with Extraneous input should send back the extraneous input
            with self.assertRaises(DatabaseError) as cm:
                cursor.execute("CREATE TABLE IF NOT EXISTS TABLE table_234234234")
            self.assertIn("table_234234234", str(cm.exception))

    def test_create_table_will_return_empty_result_set(self):
        with self.cursor({}) as cursor:
            table_name = 'table_{uuid}'.format(uuid=str(uuid4()).replace('-', '_'))
            try:
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS {} AS (SELECT 1 AS col_1, '2' AS col_2)".format(
                        table_name))
                self.assertEqual(cursor.fetchall(), [])
            finally:
                cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))

    def test_get_tables(self):
        with self.cursor({}) as cursor:
            table_name = 'table_{uuid}'.format(uuid=str(uuid4()).replace('-', '_'))
            table_names = [table_name + '_1', table_name + '_2']

            try:
                for table in table_names:
                    cursor.execute(
                        "CREATE TABLE IF NOT EXISTS {} AS (SELECT 1 AS col_1, '2' AS col_2)".format(
                            table))
                cursor.tables(schema_name="defa%")
                tables = cursor.fetchall()
                tables_desc = cursor.description

                for table in table_names:
                    # Test only schema name and table name.
                    # From other columns, what is supported depends on DBR version.
                    self.assertIn(['default', table], [list(table[1:3]) for table in tables])
                self.assertEqual(
                    tables_desc,
                    [('TABLE_CAT', 'string', None, None, None, None, None),
                     ('TABLE_SCHEM', 'string', None, None, None, None, None),
                     ('TABLE_NAME', 'string', None, None, None, None, None),
                     ('TABLE_TYPE', 'string', None, None, None, None, None),
                     ('REMARKS', 'string', None, None, None, None, None),
                     ('TYPE_CAT', 'string', None, None, None, None, None),
                     ('TYPE_SCHEM', 'string', None, None, None, None, None),
                     ('TYPE_NAME', 'string', None, None, None, None, None),
                     ('SELF_REFERENCING_COL_NAME', 'string', None, None, None, None, None),
                     ('REF_GENERATION', 'string', None, None, None, None, None)])
            finally:
                for table in table_names:
                    cursor.execute('DROP TABLE IF EXISTS {}'.format(table))

    def test_get_columns(self):
        with self.cursor({}) as cursor:
            table_name = 'table_{uuid}'.format(uuid=str(uuid4()).replace('-', '_'))
            table_names = [table_name + '_1', table_name + '_2']

            try:
                for table in table_names:
                    cursor.execute("CREATE TABLE IF NOT EXISTS {} AS (SELECT "
                                   "1 AS col_1, "
                                   "'2' AS col_2, "
                                   "named_struct('name', 'alice', 'age', 28) as col_3, "
                                   "map('items', 45, 'cost', 228) as col_4, "
                                   "array('item1', 'item2', 'item3') as col_5)".format(table))

                cursor.columns(schema_name="defa%", table_name=table_name + '%')
                cols = cursor.fetchall()
                cols_desc = cursor.description

                # Catalogue name not consistent across DBR versions, so we skip that
                cleaned_response = [list(col[1:6]) for col in cols]
                # We also replace ` as DBR changes how it represents struct names
                for col in cleaned_response:
                    col[4] = col[4].replace("`", "")

                self.assertEqual(cleaned_response, [
                    ['default', table_name + '_1', 'col_1', 4, 'INT'],
                    ['default', table_name + '_1', 'col_2', 12, 'STRING'],
                    ['default', table_name + '_1', 'col_3', 2002, 'STRUCT<name: STRING, age: INT>'],
                    ['default', table_name + '_1', 'col_4', 2000, 'MAP<STRING, INT>'],
                    ['default', table_name + '_1', 'col_5', 2003, 'ARRAY<STRING>'],
                    ['default', table_name + '_2', 'col_1', 4, 'INT'],
                    ['default', table_name + '_2', 'col_2', 12, 'STRING'],
                    ['default', table_name + '_2', 'col_3', 2002, 'STRUCT<name: STRING, age: INT>'],
                    ['default', table_name + '_2', 'col_4', 2000, 'MAP<STRING, INT>'],
                    [
                        'default',
                        table_name + '_2',
                        'col_5',
                        2003,
                        'ARRAY<STRING>',
                    ]
                ])

                self.assertEqual(cols_desc,
                                 [('TABLE_CAT', 'string', None, None, None, None, None),
                                  ('TABLE_SCHEM', 'string', None, None, None, None, None),
                                  ('TABLE_NAME', 'string', None, None, None, None, None),
                                  ('COLUMN_NAME', 'string', None, None, None, None, None),
                                  ('DATA_TYPE', 'int', None, None, None, None, None),
                                  ('TYPE_NAME', 'string', None, None, None, None, None),
                                  ('COLUMN_SIZE', 'int', None, None, None, None, None),
                                  ('BUFFER_LENGTH', 'tinyint', None, None, None, None, None),
                                  ('DECIMAL_DIGITS', 'int', None, None, None, None, None),
                                  ('NUM_PREC_RADIX', 'int', None, None, None, None, None),
                                  ('NULLABLE', 'int', None, None, None, None, None),
                                  ('REMARKS', 'string', None, None, None, None, None),
                                  ('COLUMN_DEF', 'string', None, None, None, None, None),
                                  ('SQL_DATA_TYPE', 'int', None, None, None, None, None),
                                  ('SQL_DATETIME_SUB', 'int', None, None, None, None, None),
                                  ('CHAR_OCTET_LENGTH', 'int', None, None, None, None, None),
                                  ('ORDINAL_POSITION', 'int', None, None, None, None, None),
                                  ('IS_NULLABLE', 'string', None, None, None, None, None),
                                  ('SCOPE_CATALOG', 'string', None, None, None, None, None),
                                  ('SCOPE_SCHEMA', 'string', None, None, None, None, None),
                                  ('SCOPE_TABLE', 'string', None, None, None, None, None),
                                  ('SOURCE_DATA_TYPE', 'smallint', None, None, None, None, None),
                                  ('IS_AUTO_INCREMENT', 'string', None, None, None, None, None)])
            finally:
                for table in table_names:
                    cursor.execute('DROP TABLE IF EXISTS {}'.format(table))

    def test_get_schemas(self):
        with self.cursor({}) as cursor:
            database_name = 'db_{uuid}'.format(uuid=str(uuid4()).replace('-', '_'))
            try:
                cursor.execute('CREATE DATABASE IF NOT EXISTS {}'.format(database_name))
                cursor.schemas()
                schemas = cursor.fetchall()
                schemas_desc = cursor.description
                # Catalogue name not consistent across DBR versions, so we skip that
                self.assertIn(database_name, [schema[0] for schema in schemas])
                self.assertEqual(schemas_desc,
                                 [('TABLE_SCHEM', 'string', None, None, None, None, None),
                                  ('TABLE_CATALOG', 'string', None, None, None, None, None)])
            finally:
                cursor.execute('DROP DATABASE IF EXISTS {}'.format(database_name))

    def test_get_catalogs(self):
        with self.cursor({}) as cursor:
            cursor.catalogs()
            cursor.fetchall()
            catalogs_desc = cursor.description
            self.assertEqual(catalogs_desc, [('TABLE_CAT', 'string', None, None, None, None, None)])

    @skipUnless(pysql_supports_arrow(), 'arrow test need arrow support')
    def test_get_arrow(self):
        # These tests are quite light weight as the arrow fetch methods are used internally
        # by everything else
        with self.cursor({}) as cursor:
            cursor.execute("SELECT * FROM range(10)")
            table_1 = cursor.fetchmany_arrow(1).to_pydict()
            self.assertEqual(table_1, OrderedDict([("id", [0])]))

            table_2 = cursor.fetchall_arrow().to_pydict()
            self.assertEqual(table_2, OrderedDict([("id", [1, 2, 3, 4, 5, 6, 7, 8, 9])]))

    def test_unicode(self):
        unicode_str = "数据砖"
        with self.cursor({}) as cursor:
            cursor.execute("SELECT '{}'".format(unicode_str))
            results = cursor.fetchall()
            self.assertTrue(len(results) == 1 and len(results[0]) == 1)
            self.assertEqual(results[0][0], unicode_str)

    def test_cancel_during_execute(self):
        with self.cursor({}) as cursor:

            def execute_really_long_query():
                cursor.execute("SELECT SUM(A.id - B.id) " +
                               "FROM range(1000000000) A CROSS JOIN range(100000000) B " +
                               "GROUP BY (A.id - B.id)")

            exec_thread = threading.Thread(target=execute_really_long_query)

            exec_thread.start()
            # Make sure the query has started before cancelling
            time.sleep(15)
            cursor.cancel()
            exec_thread.join(5)
            self.assertFalse(exec_thread.is_alive())

            # Fetching results should throw an exception
            with self.assertRaises((Error, thrift.Thrift.TException)):
                cursor.fetchall()
            with self.assertRaises((Error, thrift.Thrift.TException)):
                cursor.fetchone()
            with self.assertRaises((Error, thrift.Thrift.TException)):
                cursor.fetchmany(10)

            # We should be able to execute a new command on the cursor
            cursor.execute("SELECT * FROM range(3)")
            self.assertEqual(len(cursor.fetchall()), 3)

    def test_retry_after_connection_timeout(self):
        # We only retry a request that failed because of a socket timeout in this case
        # when the timeout occurred while trying to connect with the thrift server.
        # In this situation, we know that a command was not sent to the server because
        # no connection was made.
        
        import socket
        from unittest.mock import Mock
        from databricks.sql.thrift_api.TCLIService import ttypes

       # First let's check the non-retry behavior
       # Given the client has successfully connected to the server already
       # When a socket.timeout exception is raised
       # Then the request is not retried
        with self.assertRaises(OperationalError) as cm:
            
            # No mocks at this point. If calling self.cursor() succeeds
            # that means there is an open / working connection to thrift server.
            with self.cursor({}) as cursor:

                # Next apply a patch to the transport which raises a socket.timeout
                # whenever any data is sent over the wire
                with mock.patch("http.client.HTTPConnection.send") as mock_send:
                    mock_send.side_effect = socket.timeout
                    cursor.execute("I AM A VERY DANGEROUS, NOT IDEMPOTENT QUERY!!!")
            self.assertIn("non-retryable error", cm.exception.message_with_context())


        # Second, let's check whether a request is retried if it fails during
        # the connection attempt, instead of the send attempt.
        
        ATTEMPTS_TO_TRY = 2

        # This is a normal query execution
        # with self.cursor({}) as cursor:
        #     thrift_backend = cursor.thrift_backend

        
        with self.assertRaises(OperationalError) as cm:
            with mock.patch("socket.socket.connect") as mock_connect:
                mock_connect.side_effect = OSError("[Errno 110]: Connection timed out")
                with self.cursor() as cursor:
                    # Connection will fail
                    cursor.execute("SOME RANDOM STATEMENT")
                    pass

            self.assertIn("{0}/{0}".format(ATTEMPTS_TO_TRY), cm.exception.message_with_context())

        with self.assertRaises(OperationalError) as cm:
            with mock.patch("socket.socket.connect") as mock_connect:
                mock_connect.side_effect = socket.timeout
                with self.cursor() as cursor:
                    # Connection will fail
                    cursor.execute("SOME RANDOM STATEMENT")
                    pass

            self.assertIn("{0}/{0}".format(ATTEMPTS_TO_TRY), cm.exception.message_with_context())



    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_can_execute_command_after_failure(self):
        with self.cursor({}) as cursor:
            with self.assertRaises(DatabaseError):
                cursor.execute("this is a sytnax error")

            cursor.execute("SELECT 1;")

            res = cursor.fetchall()
            self.assertEqualRowValues(res, [[1]])

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_can_execute_command_after_success(self):
        with self.cursor({}) as cursor:
            cursor.execute("SELECT 1;")
            cursor.execute("SELECT 2;")

            res = cursor.fetchall()
            self.assertEqualRowValues(res, [[2]])

    def generate_multi_row_query(self):
        query = "SELECT * FROM range(3);"
        return query

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_fetchone(self):
        with self.cursor({}) as cursor:
            query = self.generate_multi_row_query()
            cursor.execute(query)

            self.assertSequenceEqual(cursor.fetchone(), [0])
            self.assertSequenceEqual(cursor.fetchone(), [1])
            self.assertSequenceEqual(cursor.fetchone(), [2])

            self.assertEqual(cursor.fetchone(), None)

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_fetchall(self):
        with self.cursor({}) as cursor:
            query = self.generate_multi_row_query()
            cursor.execute(query)

            self.assertEqualRowValues(cursor.fetchall(), [[0], [1], [2]])

            self.assertEqual(cursor.fetchone(), None)

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_fetchmany_when_stride_fits(self):
        with self.cursor({}) as cursor:
            query = "SELECT * FROM range(4)"
            cursor.execute(query)

            self.assertEqualRowValues(cursor.fetchmany(2), [[0], [1]])
            self.assertEqualRowValues(cursor.fetchmany(2), [[2], [3]])

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_fetchmany_in_excess(self):
        with self.cursor({}) as cursor:
            query = "SELECT * FROM range(4)"
            cursor.execute(query)

            self.assertEqualRowValues(cursor.fetchmany(3), [[0], [1], [2]])
            self.assertEqualRowValues(cursor.fetchmany(3), [[3]])

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_iterator_api(self):
        with self.cursor({}) as cursor:
            query = "SELECT * FROM range(4)"
            cursor.execute(query)

            expected_results = [[0], [1], [2], [3]]
            for (i, row) in enumerate(cursor):
                self.assertSequenceEqual(row, expected_results[i])

    def test_temp_view_fetch(self):
        with self.cursor({}) as cursor:
            query = "create temporary view f as select * from range(10)"
            cursor.execute(query)
            # TODO assert on a result
            # once what is being returned has stabilised

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_socket_timeout(self):
        #  We we expect to see a BlockingIO error when the socket is opened
        #  in non-blocking mode, since no poll is done before the read
        with self.assertRaises(OperationalError) as cm:
            with self.cursor({"_socket_timeout": 0}):
                pass

        self.assertIsInstance(cm.exception.args[1], io.BlockingIOError)

    def test_ssp_passthrough(self):
        for enable_ansi in (True, False):
            with self.cursor({"session_configuration": {"ansi_mode": enable_ansi}}) as cursor:
                cursor.execute("SET ansi_mode")
                self.assertEqual(list(cursor.fetchone()), ["ansi_mode", str(enable_ansi)])

    @skipUnless(pysql_supports_arrow(), 'arrow test needs arrow support')
    def test_timestamps_arrow(self):
        with self.cursor({"session_configuration": {"ansi_mode": False}}) as cursor:
            for (timestamp, expected) in self.timestamp_and_expected_results:
                cursor.execute("SELECT TIMESTAMP('{timestamp}')".format(timestamp=timestamp))
                arrow_table = cursor.fetchmany_arrow(1)
                if self.should_add_timezone():
                    ts_type = pyarrow.timestamp("us", tz="Etc/UTC")
                else:
                    ts_type = pyarrow.timestamp("us")
                self.assertEqual(arrow_table.field(0).type, ts_type)
                result_value = arrow_table.column(0).combine_chunks()[0].value
                # To work consistently across different local timezones, we specify the timezone
                # of the expected result to
                # be UTC (what it should be by default on the server)
                aware_timestamp = expected and expected.replace(tzinfo=datetime.timezone.utc)
                self.assertEqual(result_value, aware_timestamp and
                                 aware_timestamp.timestamp() * 1000000,
                                 "timestamp {} did not match {}".format(timestamp, expected))

    @skipUnless(pysql_supports_arrow(), 'arrow test needs arrow support')
    def test_multi_timestamps_arrow(self):
        with self.cursor({"session_configuration": {"ansi_mode": False}}) as cursor:
            query, expected = self.multi_query()
            expected = [[self.maybe_add_timezone_to_timestamp(ts) for ts in row]
                        for row in expected]
            cursor.execute(query)
            table = cursor.fetchall_arrow()
            # Transpose columnar result to list of rows
            list_of_cols = [c.to_pylist() for c in table]
            result = [[col[row_index] for col in list_of_cols]
                      for row_index in range(table.num_rows)]
            self.assertEqual(result, expected)

    @skipUnless(pysql_supports_arrow(), 'arrow test needs arrow support')
    def test_timezone_with_timestamp(self):
        if self.should_add_timezone():
            with self.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'Europe/Amsterdam'")
                cursor.execute("select CAST('2022-03-02 12:54:56' as TIMESTAMP)")
                amsterdam = pytz.timezone("Europe/Amsterdam")
                expected = amsterdam.localize(datetime.datetime(2022, 3, 2, 12, 54, 56))
                result = cursor.fetchone()[0]
                self.assertEqual(result, expected)

                cursor.execute("select CAST('2022-03-02 12:54:56' as TIMESTAMP)")
                arrow_result_table = cursor.fetchmany_arrow(1)
                arrow_result_value = arrow_result_table.column(0).combine_chunks()[0].value
                ts_type = pyarrow.timestamp("us", tz="Europe/Amsterdam")

                self.assertEqual(arrow_result_table.field(0).type, ts_type)
                self.assertEqual(arrow_result_value, expected.timestamp() * 1000000)

    def _should_have_native_complex_types(self):
        return pysql_has_version(">=", 2) and is_thrift_v5_plus(self.arguments)

    @skipUnless(pysql_supports_arrow(), 'arrow test needs arrow support')
    def test_arrays_are_not_returned_as_strings_arrow(self):
        if self._should_have_native_complex_types():
            with self.cursor() as cursor:
                cursor.execute("SELECT array(1,2,3,4)")
                arrow_df = cursor.fetchall_arrow()

                list_type = arrow_df.field(0).type
                self.assertTrue(pyarrow.types.is_list(list_type))
                self.assertTrue(pyarrow.types.is_integer(list_type.value_type))

    @skipUnless(pysql_supports_arrow(), 'arrow test needs arrow support')
    def test_structs_are_not_returned_as_strings_arrow(self):
        if self._should_have_native_complex_types():
            with self.cursor() as cursor:
                cursor.execute("SELECT named_struct('foo', 42, 'bar', 'baz')")
                arrow_df = cursor.fetchall_arrow()

                struct_type = arrow_df.field(0).type
                self.assertTrue(pyarrow.types.is_struct(struct_type))

    @skipUnless(pysql_supports_arrow(), 'arrow test needs arrow support')
    def test_decimal_not_returned_as_strings_arrow(self):
        if self._should_have_native_complex_types():
            with self.cursor() as cursor:
                cursor.execute("SELECT 5E3BD")
                arrow_df = cursor.fetchall_arrow()

                decimal_type = arrow_df.field(0).type
                self.assertTrue(pyarrow.types.is_decimal(decimal_type))


# use a RetrySuite to encapsulate these tests which we'll typically want to run together; however keep
# the 429/503 subsuites separate since they execute under different circumstances.
class PySQLRetryTestSuite:
    class HTTP429Suite(Client429ResponseMixin, PySQLTestCase):
        pass  # Mixin covers all

    class HTTP503Suite(Client503ResponseMixin, PySQLTestCase):
        # 503Response suite gets custom error here vs PyODBC
        def test_retry_disabled(self):
            self._test_retry_disabled_with_message("TEMPORARILY_UNAVAILABLE", OperationalError)


class PySQLUnityCatalogTestSuite(PySQLTestCase):
    """Simple namespace tests that should be run against a unity-catalog-enabled cluster"""

    @skipIf(pysql_has_version('<', '2'), 'requires pysql v2')
    def test_initial_namespace(self):
        table_name = 'table_{uuid}'.format(uuid=str(uuid4()).replace('-', '_'))
        with self.cursor() as cursor:
            cursor.execute("USE CATALOG {}".format(self.arguments["catA"]))
            cursor.execute("CREATE TABLE table_{} (col1 int)".format(table_name))
        with self.connection({
                "catalog": self.arguments["catA"],
                "schema": table_name
        }) as connection:
            cursor = connection.cursor()
            cursor.execute("select current_catalog()")
            self.assertEqual(cursor.fetchone()[0], self.arguments["catA"])
            cursor.execute("select current_database()")
            self.assertEqual(cursor.fetchone()[0], table_name)


def main(cli_args):
    global get_args_from_env
    get_args_from_env = True
    print(f"Running tests with version: {sql.__version__}")
    logging.getLogger("databricks.sql").setLevel(logging.INFO)
    unittest.main(module=__file__, argv=sys.argv[0:1] + cli_args)


if __name__ == "__main__":
    main(sys.argv[1:])