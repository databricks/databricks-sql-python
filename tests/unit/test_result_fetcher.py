from tests.e2e.test_driver import PySQLPytestTestCase
import pytest

from typing import Tuple
from decimal import Decimal
import datetime
from datetime import timedelta
import pickle
import enum


class DatabricksDataType(enum.Enum):
    """https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html"""

    # copped this from the sqlalchemy test suite

    BIGINT = enum.auto()
    BINARY = enum.auto()
    BOOLEAN = enum.auto()
    DATE = enum.auto()
    DECIMAL = enum.auto()
    DOUBLE = enum.auto()
    FLOAT = enum.auto()
    INT = enum.auto()
    INTERVAL = enum.auto()
    VOID = enum.auto()
    SMALLINT = enum.auto()
    STRING = enum.auto()
    TIMESTAMP = enum.auto()
    TIMESTAMP_NTZ = enum.auto()
    TINYINT = enum.auto()
    ARRAY = enum.auto()
    MAP = enum.auto()
    STRUCT = enum.auto()


# this is a sample query


from databricks.sql.thrift_api.TCLIService.ttypes import TRowSet, TTableSchema


class TestSparkRowConversion(PySQLPytestTestCase):
    """
    Test the conversion of Spark Rows to Python dictionaries.
    """

    # TODO: Implement support for interval types
    # TODO: Implement deserialize complex types into something other than strings
    QUERY = """
    SELECT 
        CAST(1 AS BIGINT) AS bigint_value,
        CAST('binary' AS BINARY) AS binary_value,
        CAST(TRUE AS BOOLEAN) AS boolean_value,
        CAST(DATE '2022-01-01' AS DATE) AS date_value,
        CAST(3.14 AS DECIMAL(10, 2)) AS decimal_value,
        CAST(3.14 AS DOUBLE) AS double_value,
        CAST(3.14 AS FLOAT) AS float_value,
        CAST(42 AS INT) AS int_value,
        -- CAST(INTERVAL '1' DAY AS INTERVAL) AS interval_value,
        CAST(NULL AS VOID) AS void_value,
        CAST(42 AS SMALLINT) AS smallint_value,
        CAST('string' AS STRING) AS string_value,
        CAST(TIMESTAMP '2022-01-01 00:00:00' AS TIMESTAMP) AS timestamp_value,
        CAST(TIMESTAMP '2022-01-01 00:00:00' AS TIMESTAMP_NTZ) AS timestamp_ntz_value,
        CAST(42 AS TINYINT) AS tinyint_value,
        CAST(ARRAY(1, 2, 3) AS ARRAY<INT>) AS array_value,
        CAST(MAP(1, 'one', 2, 'two') AS MAP<INT, STRING>) AS map_value,
        CAST(NAMED_STRUCT('field1', 1, 'field2', 'two') AS STRUCT<field1: INT, field2: STRING>) AS struct_value
    """

    expected_values = {
        "bigint_value": 1,
        "binary_value": b"binary",
        "boolean_value": True,
        "date_value": datetime.date(2022, 1, 1),
        "decimal_value": Decimal("3.14"),
        "double_value": 3.14,
        "float_value": 3.14,
        "int_value": 42,
        # 'interval_value': timedelta(days=1),
        "void_value": None,
        "smallint_value": 42,
        "string_value": "string",
        "timestamp_value": datetime.datetime(2022, 1, 1, 0, 0),
        "timestamp_ntz_value": datetime.datetime(2022, 1, 1, 0, 0),
        "tinyint_value": 42,
        "array_value": [1, 2, 3],
        "map_value": {1: "one", 2: "two"},
        "struct_value": {"field1": 1, "field2": "two"},
    }

    @pytest.fixture(scope="class")
    def thrift_data(self) -> Tuple[TTableSchema, TRowSet]:
        # this query returns all databricks types
        pass

    @pytest.fixture(scope="class")
    def hive_schema(self) -> TTableSchema:
        bytestring = b'\x80\x04\x95\x15\x08\x00\x00\x00\x00\x00\x00\x8c,databricks.sql.thrift_api.TCLIService.ttypes\x94\x8c\x0cTTableSchema\x94\x93\x94)\x81\x94}\x94\x8c\x07columns\x94]\x94(h\x00\x8c\x0bTColumnDesc\x94\x93\x94)\x81\x94}\x94(\x8c\ncolumnName\x94\x8c\x0cbigint_value\x94\x8c\x08typeDesc\x94h\x00\x8c\tTTypeDesc\x94\x93\x94)\x81\x94}\x94\x8c\x05types\x94]\x94h\x00\x8c\nTTypeEntry\x94\x93\x94)\x81\x94}\x94(\x8c\x0eprimitiveEntry\x94h\x00\x8c\x1cTTAllowedParameterValueEntry\x94\x93\x94)\x81\x94}\x94(\x8c\x04type\x94K\x04\x8c\x0etypeQualifiers\x94Nub\x8c\narrayEntry\x94N\x8c\x08mapEntry\x94N\x8c\x0bstructEntry\x94N\x8c\nunionEntry\x94N\x8c\x14userDefinedTypeEntry\x94Nubasb\x8c\x08position\x94K\x01\x8c\x07comment\x94\x8c\x00\x94ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0cbinary_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\th\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x02h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\rboolean_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x00h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x03h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\ndate_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x11h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x04h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\rdecimal_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x0fh\x1eh\x00\x8c\x0fTTypeQualifiers\x94\x93\x94)\x81\x94}\x94\x8c\nqualifiers\x94}\x94(\x8c\x05scale\x94h\x00\x8c\x13TTypeQualifierValue\x94\x93\x94)\x81\x94}\x94(\x8c\x08i32Value\x94K\x02\x8c\x0bstringValue\x94Nub\x8c\tprecision\x94hW)\x81\x94}\x94(hZK\nh[Nubusbubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x05h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0cdouble_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x06h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x06h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0bfloat_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x05h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x07h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\tint_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x03h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x08h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\nvoid_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x10h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\th%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0esmallint_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x02h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\nh%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0cstring_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x07h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x0bh%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0ftimestamp_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x08h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x0ch%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x13timestamp_ntz_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x08h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\rh%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\rtinyint_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x01h\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x0eh%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0barray_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\nh\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x0fh%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\tmap_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x0bh\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x10h%h&ubh\x08)\x81\x94}\x94(h\x0b\x8c\x0cstruct_value\x94h\rh\x0f)\x81\x94}\x94h\x12]\x94h\x15)\x81\x94}\x94(h\x18h\x1a)\x81\x94}\x94(h\x1dK\x0ch\x1eNubh\x1fNh Nh!Nh"Nh#Nubasbh$K\x11h%h&ubesb.'
        object = pickle.loads(bytestring)
        return object

    @pytest.fixture(scope="class")
    def row_set(self) -> TRowSet:
        bytestring = b'\x80\x04\x95\xc5\x05\x00\x00\x00\x00\x00\x00\x8c,databricks.sql.thrift_api.TCLIService.ttypes\x94\x8c\x07TRowSet\x94\x93\x94)\x81\x94}\x94(\x8c\x0estartRowOffset\x94K\x00\x8c\x04rows\x94]\x94\x8c\x07columns\x94]\x94(h\x00\x8c\x07TColumn\x94\x93\x94)\x81\x94}\x94(\x8c\x07boolVal\x94N\x8c\x07byteVal\x94N\x8c\x06i16Val\x94N\x8c\x06i32Val\x94N\x8c\x06i64Val\x94h\x00\x8c\nTI64Column\x94\x93\x94)\x81\x94}\x94(\x8c\x06values\x94]\x94K\x01a\x8c\x05nulls\x94C\x01\x00\x94ub\x8c\tdoubleVal\x94N\x8c\tstringVal\x94N\x8c\tbinaryVal\x94Nubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1cNh\x1dh\x00\x8c\rTBinaryColumn\x94\x93\x94)\x81\x94}\x94(h\x17]\x94C\x06binary\x94ah\x19h\x1aububh\x0b)\x81\x94}\x94(h\x0eh\x00\x8c\x0bTBoolColumn\x94\x93\x94)\x81\x94}\x94(h\x17]\x94\x88ah\x19h\x1aubh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1cNh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch\x00\x8c\rTStringColumn\x94\x93\x94)\x81\x94}\x94(h\x17]\x94\x8c\n2022-01-01\x94ah\x19h\x1aubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x043.14\x94ah\x19h\x1aubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bh\x00\x8c\rTDoubleColumn\x94\x93\x94)\x81\x94}\x94(h\x17]\x94G@\t\x1e\xb8Q\xeb\x85\x1fah\x19h\x1aubh\x1cNh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bh>)\x81\x94}\x94(h\x17]\x94G@\t\x1e\xb8Q\xeb\x85\x1fah\x19h\x1aubh\x1cNh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11h\x00\x8c\nTI32Column\x94\x93\x94)\x81\x94}\x94(h\x17]\x94K*ah\x19h\x1aubh\x12Nh\x1bNh\x1cNh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x00\x94ah\x19C\x01\x01\x94ubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10h\x00\x8c\nTI16Column\x94\x93\x94)\x81\x94}\x94(h\x17]\x94K*ah\x19h\x1aubh\x11Nh\x12Nh\x1bNh\x1cNh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x06string\x94ah\x19h\x1aubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x132022-01-01 00:00:00\x94ah\x19h\x1aubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x132022-01-01 00:00:00\x94ah\x19h\x1aubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fh\x00\x8c\x0bTByteColumn\x94\x93\x94)\x81\x94}\x94(h\x17]\x94K*ah\x19h\x1aubh\x10Nh\x11Nh\x12Nh\x1bNh\x1cNh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x07[1,2,3]\x94ah\x19h\x1aubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x11{1:"one",2:"two"}\x94ah\x19h\x1aubh\x1dNubh\x0b)\x81\x94}\x94(h\x0eNh\x0fNh\x10Nh\x11Nh\x12Nh\x1bNh\x1ch0)\x81\x94}\x94(h\x17]\x94\x8c\x1b{"field1":1,"field2":"two"}\x94ah\x19h\x1aubh\x1dNube\x8c\rbinaryColumns\x94N\x8c\x0bcolumnCount\x94N\x8c\x0carrowBatches\x94N\x8c\x0bresultLinks\x94N\x8c\x11cloudFetchResults\x94Nub.'
        object = pickle.loads(bytestring)
        return object

    def test_foo(self):
        with self.cursor() as cursor:
            cursor.execute(self.QUERY)
            result = cursor.fetchone()
            assert result

    def test_rowset_type(self, row_set: TRowSet):
        assert isinstance(row_set, TRowSet)

    def test_hive_schema_type(self, hive_schema: TTableSchema):
        assert isinstance(hive_schema, TTableSchema)
