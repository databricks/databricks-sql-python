from databricks.sql.utils import named_parameters_to_tsparkparams
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TSparkParameterValue
)
from databricks.sql.utils import DbSqlParameter, DbSqlType

class TestTSparkParameterConversion(object):
    def test_conversion(self):
        """This behaviour falls back to Python's default string formatting of numbers
        """
        assert named_parameters_to_tsparkparams(["a",1,True, 1.0, DbSqlParameter(value="1.0",type=DbSqlType.DECIMAL)]) == [TSparkParameter(name='',type="STRING", value=TSparkParameterValue(stringValue="a")), 
                                                           TSparkParameter(name='',type="INTEGER", value=TSparkParameterValue(stringValue="1")), 
                                                           TSparkParameter(name='',type="BOOLEAN", value=TSparkParameterValue(stringValue="True")),
                                                           TSparkParameter(name='',type="FLOAT", value=TSparkParameterValue(stringValue="1.0")),
                                                           TSparkParameter(name='',type="DECIMAL", value=TSparkParameterValue(stringValue="1.0"))]