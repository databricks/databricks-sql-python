import unittest

from databricks.sql.thrift_api.TCLIService.ttypes import TSparkParameter, TSparkParameterValue
from databricks.sql.utils import ParamConverter


class TestIndividualParameterTypes(unittest.TestCase):
    def test_null_value(self):
        assert ParamConverter.convert_to_spark_parameters({"key": None}) == \
               [TSparkParameter(name="key", type="VOID", value=TSparkParameterValue())]

    def test_string_value(self):
        assert ParamConverter.convert_to_spark_parameters({"key": "value"}) == \
               [TSparkParameter(name="key", type="STRING", value=TSparkParameterValue(stringValue="value"))]

    def test_int_value(self):
        assert ParamConverter.convert_to_spark_parameters({"key": 1}) == \
               [TSparkParameter(name="key", type="INT", value=TSparkParameterValue(doubleValue=1))]

    def test_float_value(self):
        assert ParamConverter.convert_to_spark_parameters({"key": 1.0}) == \
               [TSparkParameter(name="key", type="DOUBLE", value=TSparkParameterValue(doubleValue=1.0))]

    def test_boolean_value(self):
        assert ParamConverter.convert_to_spark_parameters({"key": True}) == \
               [TSparkParameter(name="key", type="BOOLEAN", value=TSparkParameterValue(booleanValue=True))]

    def test_multiple_named_parameters(self):
        assert ParamConverter.convert_to_spark_parameters({"key_0": "value", "key_1": 1, "key_2": 1.0, "key_3": True}) \
               == [
                   TSparkParameter(name="key_0", type="STRING", value=TSparkParameterValue(stringValue="value")),
                   TSparkParameter(name="key_1", type="INT", value=TSparkParameterValue(doubleValue=1)),
                   TSparkParameter(name="key_2", type="DOUBLE", value=TSparkParameterValue(doubleValue=1.0)),
                   TSparkParameter(name="key_3", type="BOOLEAN", value=TSparkParameterValue(booleanValue=True))
               ]
