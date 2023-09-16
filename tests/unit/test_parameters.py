from databricks.sql.utils import (
    named_parameters_to_tsparkparams,
    infer_types,
    named_parameters_to_dbsqlparams,
)
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TSparkParameterValue,
)
from databricks.sql.utils import DbSqlParameter, DbSqlType
import pytest


class TestTSparkParameterConversion(object):
    def test_conversion_e2e(self):
        """This behaviour falls back to Python's default string formatting of numbers"""
        assert named_parameters_to_tsparkparams(
            ["a", 1, True, 1.0, DbSqlParameter(value="1.0", type=DbSqlType.DECIMAL)]
        ) == [
            TSparkParameter(
                name="", type="STRING", value=TSparkParameterValue(stringValue="a")
            ),
            TSparkParameter(
                name="", type="INTEGER", value=TSparkParameterValue(stringValue="1")
            ),
            TSparkParameter(
                name="", type="BOOLEAN", value=TSparkParameterValue(stringValue="True")
            ),
            TSparkParameter(
                name="", type="FLOAT", value=TSparkParameterValue(stringValue="1.0")
            ),
            TSparkParameter(
                name="", type="DECIMAL", value=TSparkParameterValue(stringValue="1.0")
            ),
        ]

    def test_basic_conversions(self):
        # Test legacy codepath
        assert named_parameters_to_dbsqlparams({"1": 1, "2": "foo", "3": 2.0}) == [
            DbSqlParameter("1", 1),
            DbSqlParameter("2", "foo"),
            DbSqlParameter("3", 2.0),
        ]
        # Test interspersing named params with unnamed
        assert named_parameters_to_dbsqlparams(
            [DbSqlParameter("1", 1.0, DbSqlType.DECIMAL), 5, DbSqlParameter("3", "foo")]
        ) == [
            DbSqlParameter("1", 1.0, DbSqlType.DECIMAL),
            DbSqlParameter("", 5),
            DbSqlParameter("3", "foo"),
        ]

    def test_type_inference(self):
        with pytest.raises(ValueError):
            infer_types([DbSqlParameter("", None)])
        with pytest.raises(ValueError):
            infer_types([DbSqlParameter("", {1: 1})])
        assert infer_types([DbSqlParameter("", 1)]) == [
            DbSqlParameter("", "1", DbSqlType.INTEGER)
        ]
        assert infer_types([DbSqlParameter("", True)]) == [
            DbSqlParameter("", "True", DbSqlType.BOOLEAN)
        ]
        assert infer_types([DbSqlParameter("", 1.0)]) == [
            DbSqlParameter("", "1.0", DbSqlType.FLOAT)
        ]
        assert infer_types([DbSqlParameter("", "foo")]) == [
            DbSqlParameter("", "foo", DbSqlType.STRING)
        ]
        assert infer_types([DbSqlParameter("", 1.0, DbSqlType.DECIMAL)]) == [
            DbSqlParameter("", "1.0", DbSqlType.DECIMAL)
        ]
