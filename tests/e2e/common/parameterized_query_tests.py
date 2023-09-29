import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Tuple, Union

import pytz

from databricks.sql.exc import DatabaseError
from databricks.sql.utils import (
    DbSqlParameter,
    DbSqlType,
    calculate_decimal_cast_string,
)


class MyCustomDecimalType(Enum):
    DECIMAL_38_0 = "DECIMAL(38,0)"
    DECIMAL_38_2 = "DECIMAL(38,2)"
    DECIMAL_18_9 = "DECIMAL(18,9)"


class PySQLParameterizedQueryTestSuiteMixin:
    """Namespace for tests of server-side parameterized queries"""

    QUERY = "SELECT :p AS col"

    def _get_one_result(self, query: str, parameters: Union[Dict, List[Dict]]) -> Tuple:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters=parameters)
                return cursor.fetchone()

    def _quantize(self, input: Union[float, int], place_value=2) -> Decimal:

        return Decimal(str(input)).quantize(Decimal("0." + "0" * place_value))

    def test_primitive_inferred_bool(self):

        params = {"p": True}
        result = self._get_one_result(self.QUERY, params)
        assert result.col == True

    def test_primitive_inferred_integer(self):

        params = {"p": 1}
        result = self._get_one_result(self.QUERY, params)
        assert result.col == 1

    def test_primitive_inferred_double(self):

        params = {"p": 3.14}
        result = self._get_one_result(self.QUERY, params)
        assert self._quantize(result.col) == self._quantize(3.14)

    def test_primitive_inferred_date(self):

        # DATE in Databricks is mapped into a datetime.date object in Python
        date_value = datetime.date(2023, 9, 6)
        params = {"p": date_value}
        result = self._get_one_result(self.QUERY, params)
        assert result.col == date_value

    def test_primitive_inferred_timestamp(self):

        # TIMESTAMP in Databricks is mapped into a datetime.datetime object in Python
        date_value = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
        params = {"p": date_value}
        result = self._get_one_result(self.QUERY, params)
        assert result.col == date_value

    def test_primitive_inferred_string(self):

        params = {"p": "Hello"}
        result = self._get_one_result(self.QUERY, params)
        assert result.col == "Hello"

    def test_primitive_inferred_decimal(self):
        params = {"p": Decimal("1234.56")}
        result = self._get_one_result(self.QUERY, params)
        assert result.col == Decimal("1234.56")

    def test_dbsqlparam_inferred_bool(self):

        params = [DbSqlParameter(name="p", value=True, type=None)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == True

    def test_dbsqlparam_inferred_integer(self):

        params = [DbSqlParameter(name="p", value=1, type=None)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == 1

    def test_dbsqlparam_inferred_double(self):

        params = [DbSqlParameter(name="p", value=3.14, type=None)]
        result = self._get_one_result(self.QUERY, params)
        assert self._quantize(result.col) == self._quantize(3.14)

    def test_dbsqlparam_inferred_date(self):

        # DATE in Databricks is mapped into a datetime.date object in Python
        date_value = datetime.date(2023, 9, 6)
        params = [DbSqlParameter(name="p", value=date_value, type=None)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == date_value

    def test_dbsqlparam_inferred_timestamp(self):

        # TIMESTAMP in Databricks is mapped into a datetime.datetime object in Python
        date_value = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
        params = [DbSqlParameter(name="p", value=date_value, type=None)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == date_value

    def test_dbsqlparam_inferred_string(self):

        params = [DbSqlParameter(name="p", value="Hello", type=None)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == "Hello"

    def test_dbsqlparam_inferred_decimal(self):
        params = [DbSqlParameter(name="p", value=Decimal("1234.56"), type=None)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == Decimal("1234.56")

    def test_dbsqlparam_explicit_bool(self):

        params = [DbSqlParameter(name="p", value=True, type=DbSqlType.BOOLEAN)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == True

    def test_dbsqlparam_explicit_integer(self):

        params = [DbSqlParameter(name="p", value=1, type=DbSqlType.INTEGER)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == 1

    def test_dbsqlparam_explicit_double(self):

        params = [DbSqlParameter(name="p", value=3.14, type=DbSqlType.FLOAT)]
        result = self._get_one_result(self.QUERY, params)
        assert self._quantize(result.col) == self._quantize(3.14)

    def test_dbsqlparam_explicit_date(self):

        # DATE in Databricks is mapped into a datetime.date object in Python
        date_value = datetime.date(2023, 9, 6)
        params = [DbSqlParameter(name="p", value=date_value, type=DbSqlType.DATE)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == date_value

    def test_dbsqlparam_explicit_timestamp(self):

        # TIMESTAMP in Databricks is mapped into a datetime.datetime object in Python
        date_value = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
        params = [DbSqlParameter(name="p", value=date_value, type=DbSqlType.TIMESTAMP)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == date_value

    def test_dbsqlparam_explicit_string(self):

        params = [DbSqlParameter(name="p", value="Hello", type=DbSqlType.STRING)]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == "Hello"

    def test_dbsqlparam_explicit_decimal(self):
        params = [
            DbSqlParameter(name="p", value=Decimal("1234.56"), type=DbSqlType.DECIMAL)
        ]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == Decimal("1234.56")

    def test_dbsqlparam_custom_explicit_decimal_38_0(self):

        # This DECIMAL can be contained in a DECIMAL(38,0) column in Databricks
        value = Decimal("12345678912345678912345678912345678912")
        params = [
            DbSqlParameter(name="p", value=value, type=MyCustomDecimalType.DECIMAL_38_0)
        ]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == value

    def test_dbsqlparam_custom_explicit_decimal_38_2(self):

        # This DECIMAL can be contained in a DECIMAL(38,2) column in Databricks
        value = Decimal("123456789123456789123456789123456789.12")
        params = [
            DbSqlParameter(name="p", value=value, type=MyCustomDecimalType.DECIMAL_38_2)
        ]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == value

    def test_dbsqlparam_custom_explicit_decimal_18_9(self):

        # This DECIMAL can be contained in a DECIMAL(18,9) column in Databricks
        value = Decimal("123456789.123456789")
        params = [
            DbSqlParameter(name="p", value=value, type=MyCustomDecimalType.DECIMAL_18_9)
        ]
        result = self._get_one_result(self.QUERY, params)
        assert result.col == value

    def test_calculate_decimal_cast_string(self):

        assert calculate_decimal_cast_string(Decimal("10.00")) == "DECIMAL(4,2)"
        assert (
            calculate_decimal_cast_string(
                Decimal("123456789123456789.123456789123456789")
            )
            == "DECIMAL(36,18)"
        )
