from contextlib import contextmanager
from collections import OrderedDict
import datetime
import io
import logging
import os
import sys
import tempfile
import threading
import time
from unittest import loader, skipIf, skipUnless, TestCase
from uuid import uuid4

import numpy as np
import pyarrow
import pytz
import thrift
import pytest

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

    def test_escape_single_quotes(self):
        with self.cursor({}) as cursor:
            table_name = 'table_{uuid}'.format(uuid=str(uuid4()).replace('-', '_'))
            # Test escape syntax directly
            cursor.execute("CREATE TABLE IF NOT EXISTS {} AS (SELECT 'you\\'re' AS col_1)".format(table_name))
            cursor.execute("SELECT * FROM {} WHERE col_1 LIKE 'you\\'re'".format(table_name))
            rows = cursor.fetchall()
            assert rows[0]["col_1"] == "you're"

            # Test escape syntax in parameter
            cursor.execute("SELECT * FROM {} WHERE {}.col_1 LIKE %(var)s".format(table_name, table_name), parameters={"var": "you're"})
            rows = cursor.fetchall()
            assert rows[0]["col_1"] == "you're"

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

    @skipUnless(pysql_supports_arrow(), 'arrow test needs arrow support')
    def test_can_flip_compression(self):
        with self.cursor() as cursor:
            cursor.execute("SELECT array(1,2,3,4)")
            cursor.fetchall()
            lz4_compressed = cursor.active_result_set.lz4_compressed
            #The endpoint should support compression
            self.assertEqual(lz4_compressed, True)
            cursor.connection.lz4_compression=False
            cursor.execute("SELECT array(1,2,3,4)")
            cursor.fetchall()
            lz4_compressed = cursor.active_result_set.lz4_compressed
            self.assertEqual(lz4_compressed, False)

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

    def test_close_connection_closes_cursors(self):

        from databricks.sql.thrift_api.TCLIService import ttypes

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, id `id2`, id `id3` FROM RANGE(1000000) order by RANDOM()')
            ars = cursor.active_result_set

            # We must manually run this check because thrift_backend always forces `has_been_closed_server_side` to True

            # Cursor op state should be open before connection is closed
            status_request = ttypes.TGetOperationStatusReq(operationHandle=ars.command_id, getProgressUpdate=False)
            op_status_at_server = ars.thrift_backend._client.GetOperationStatus(status_request)
            assert op_status_at_server.operationState != ttypes.TOperationState.CLOSED_STATE

            conn.close()
            
            # When connection closes, any cursor operations should no longer exist at the server
            with self.assertRaises(thrift.Thrift.TApplicationException) as cm:
                op_status_at_server = ars.thrift_backend._client.GetOperationStatus(status_request)
                if hasattr(cm, "exception"):
                    assert "RESOURCE_DOES_NOT_EXIST" in cm.exception.message




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

class PySQLStagingIngestionTestSuite(PySQLTestCase):
    """Simple namespace for ingestion tests. These should be run against DBR >12.x

    In addition to connection credentials (host, path, token) this suite requires an env var
    named staging_ingestion_user"""

    staging_ingestion_user = os.getenv("staging_ingestion_user")

    if staging_ingestion_user is None:
        raise ValueError(
            "To run these tests you must designate a `staging_ingestion_user` environment variable. This will the user associated with the personal access token."
        )

    def test_staging_ingestion_life_cycle(self):
        """PUT a file into the staging location
        GET the file from the staging location
        REMOVE the file from the staging location
        Try to GET the file again expecting to raise an exception
        """

        # PUT should succeed

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        with self.connection(extra_params={"uploads_base_path": temp_path}) as conn:

            cursor = conn.cursor()
            query = f"PUT '{temp_path}' INTO 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' OVERWRITE"
            cursor.execute(query)

        # GET should succeed

        new_fh, new_temp_path = tempfile.mkstemp()

        with self.connection(extra_params={"uploads_base_path": new_temp_path}) as conn:
            cursor = conn.cursor()
            query = f"GET 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' TO '{new_temp_path}'"
            cursor.execute(query)

        with open(new_fh, "rb") as fp:
            fetched_text = fp.read()

        assert fetched_text == original_text

        # REMOVE should succeed

        remove_query = (
            f"REMOVE 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv'"
        )

        with self.connection(extra_params={"uploads_base_path": "/"}) as conn:
            cursor = conn.cursor()
            cursor.execute(remove_query)

        # GET after REMOVE should fail

            with pytest.raises(Error):
                cursor = conn.cursor()
                query = f"GET 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' TO '{new_temp_path}'"
                cursor.execute(query)

        os.remove(temp_path)
        os.remove(new_temp_path)


    def test_staging_ingestion_put_fails_without_uploadsbasepath(self):
        """PUT operations are not supported unless the connection was built with
        a parameter called uploads_base_path
        """

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        with pytest.raises(Error):
            with self.connection() as conn:
                cursor = conn.cursor()
                query = f"PUT '{temp_path}' INTO 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_staging_ingestion_put_fails_if_localFile_not_in_uploads_base_path(self):


        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        base_path, filename = os.path.split(temp_path)

        # Add junk to base_path
        base_path = os.path.join(base_path, "temp")

        with pytest.raises(Error):
            with self.connection(extra_params={"uploads_base_path": base_path}) as conn:
                cursor = conn.cursor()
                query = f"PUT '{temp_path}' INTO 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_staging_ingestion_put_fails_if_file_exists_and_overwrite_not_set(self):
        """PUT a file into the staging location twice. First command should succeed. Second should fail.
        """

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        def perform_put():
            with self.connection(extra_params={"uploads_base_path": temp_path}) as conn:
                cursor = conn.cursor()
                query = f"PUT '{temp_path}' INTO 'stage://tmp/{self.staging_ingestion_user}/tmp/12/15/file1.csv'"
                cursor.execute(query)

        def perform_remove():
            remove_query = (
                f"REMOVE 'stage://tmp/{self.staging_ingestion_user}/tmp/12/15/file1.csv'"
            )

            with self.connection(extra_params={"uploads_base_path": "/"}) as conn:
                cursor = conn.cursor()
                cursor.execute(remove_query)


        # Make sure file does not exist
        perform_remove()

        # Put the file
        perform_put()

        # Try to put it again
        with pytest.raises(sql.exc.ServerOperationError, match="FILE_IN_STAGING_PATH_ALREADY_EXISTS"):
            perform_put()

        # Clean up after ourselves
        perform_remove()
        
    def test_staging_ingestion_fails_to_modify_another_staging_user(self):
        """The server should only allow modification of the staging_ingestion_user's files
        """

        some_other_user = "mary.poppins@databricks.com"

        fh, temp_path = tempfile.mkstemp()

        original_text = "hello world!".encode("utf-8")

        with open(fh, "wb") as fp:
            fp.write(original_text)

        def perform_put():
            with self.connection(extra_params={"uploads_base_path": temp_path}) as conn:
                cursor = conn.cursor()
                query = f"PUT '{temp_path}' INTO 'stage://tmp/{some_other_user}/tmp/12/15/file1.csv' OVERWRITE"
                cursor.execute(query)

        def perform_remove():
            remove_query = (
                f"REMOVE 'stage://tmp/{some_other_user}/tmp/12/15/file1.csv'"
            )

            with self.connection(extra_params={"uploads_base_path": "/"}) as conn:
                cursor = conn.cursor()
                cursor.execute(remove_query)

        def perform_get():
            with self.connection(extra_params={"uploads_base_path": temp_path}) as conn:
                cursor = conn.cursor()
                query = f"GET 'stage://tmp/{some_other_user}/tmp/11/15/file1.csv' TO '{temp_path}'"
                cursor.execute(query)

        # PUT should fail with permissions error
        with pytest.raises(sql.exc.ServerOperationError, match="PERMISSION_DENIED"):
            perform_put()

        # REMOVE should fail with permissions error
        with pytest.raises(sql.exc.ServerOperationError, match="PERMISSION_DENIED"):
            perform_remove()

        # GET should fail with permissions error
        with pytest.raises(sql.exc.ServerOperationError, match="PERMISSION_DENIED"):
            perform_get()

    def test_staging_ingestion_put_fails_if_absolute_localFile_not_in_uploads_base_path(self):
        """
        This test confirms that uploads_base_path and target_file are resolved into absolute paths.
        """

        # If these two paths are not resolved absolutely, they appear to share a common path of /var/www/html
        # after resolution their common path is only /var/www which should raise an exception
        # Because the common path must always be equal to uploads_base_path
        uploads_base_path = "/var/www/html"
        target_file = "/var/www/html/../html1/not_allowed.html"

        with pytest.raises(Error):
            with self.connection(extra_params={"uploads_base_path": uploads_base_path}) as conn:
                cursor = conn.cursor()
                query = f"PUT '{target_file}' INTO 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_staging_ingestion_empty_local_path_fails_to_parse_at_server(self):
        uploads_base_path = "/var/www/html"
        target_file = ""

        with pytest.raises(Error, match="EMPTY_LOCAL_FILE_IN_STAGING_ACCESS_QUERY"):
            with self.connection(extra_params={"uploads_base_path": uploads_base_path}) as conn:
                cursor = conn.cursor()
                query = f"PUT '{target_file}' INTO 'stage://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' OVERWRITE"
                cursor.execute(query)

    def test_staging_ingestion_invalid_staging_path_fails_at_server(self):
        uploads_base_path = "/var/www/html"
        target_file = "index.html"

        with pytest.raises(Error, match="INVALID_STAGING_PATH_IN_STAGING_ACCESS_QUERY"):
            with self.connection(extra_params={"uploads_base_path": uploads_base_path}) as conn:
                cursor = conn.cursor()
                query = f"PUT '{target_file}' INTO 'stageRANDOMSTRINGOFCHARACTERS://tmp/{self.staging_ingestion_user}/tmp/11/15/file1.csv' OVERWRITE"
                cursor.execute(query)



def main(cli_args):
    global get_args_from_env
    get_args_from_env = True
    print(f"Running tests with version: {sql.__version__}")
    logging.getLogger("databricks.sql").setLevel(logging.INFO)
    unittest.main(module=__file__, argv=sys.argv[0:1] + cli_args)


if __name__ == "__main__":
    main(sys.argv[1:])