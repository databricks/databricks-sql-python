import datetime
from decimal import Decimal
from typing import Dict, List, Tuple, Union

import pytz

from databricks.sql.client import Connection
from databricks.sql.utils import DbSqlParameter, DbSqlType


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
