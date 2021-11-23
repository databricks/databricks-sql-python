import databricks.sql

from pythontesting.pyhive_cursor_mixin import PyHiveThriftTestCase
from pythontesting.tests.common.test_spark_sqlquerytests import SQLQueryTestSuite, retry_error


# Base classes are defined in runtime:sql/hive-thriftserver/pythontesting/
# They are to be combined in the test-docker-image (//cmdexec/client/dbsql-test-image)
class DBSQLSQLQueryTestSuite(PyHiveThriftTestCase, SQLQueryTestSuite):
    error_type = databricks.sql.Error
    # 1 (\S+): exception type
    # 2 (.+?): exception message
    # 3 (:?;\n.+)?: elaborate explanation in AnalysisException (not captured)
    exception_re = r"Error running query: (?:\[\S+\])?\s*(\S+):\s(.+?)(?:;\n.+)?$"

    def parse_column(self, name, datatype):
        return f"{name}:{datatype}"

    def get_error_from_error_type(self, e):
        return str(e)

    @retry_error(90, databricks.sql.Error)
    def execute_single_file(self, test, code, set_commands):
        return super().execute_single_file(test, code, set_commands)


if __name__ == "__main__":
    import unittest
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
