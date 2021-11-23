import databricks.sql

from pythontesting.pyhive_cursor_mixin import PyHiveThriftTestCase
from pythontesting.tests.common.test_spark_basictests import SparkBasicSuiteBase


# Base classes are defined in runtime:sql/hive-thriftserver/pythontesting/
# They are to be combined in the test-docker-image (//cmdexec/client/dbsql-test-image)
class DBSQLBasicSuite(PyHiveThriftTestCase, SparkBasicSuiteBase):
    error_type = databricks.sql.Error


if __name__ == "__main__":
    import unittest
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
