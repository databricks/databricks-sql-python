import pytest
import databricks.sql as sql
from databricks.sql import Error
import pytz

from decimal import Decimal

from databricks.sql.utils import DbSqlParameter, DbSqlType
from datetime import datetime

from databricks.sql.client import Connection


class PySQLParameterizedQueryTestSuiteMixin:
    """Namespace for tests of server-side parameterized queries"""

    def test_parameterized_query_named_and_inferred_e2e(self):
        """Verify that named parameters passed to the database as a dict are returned of the correct type
        All types are inferred.
        """

        conn: Connection

        query = """
        SELECT
            :p_bool AS col_bool,
            :p_int AS col_int,
            :p_double AS col_double,
            :p_date as col_date,
            :p_timestamp as col_timestamp,
            :p_str AS col_str
        """

        named_parameters = {
            "p_bool": True,
            "p_int": 1234,
            "p_double": 3.14,
            "p_date": datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC),
            "p_timestamp": datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC),
            "p_str": "Hello",
        }
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, parameters=named_parameters)
            result = cursor.fetchone()

            assert result.col_bool == True
            assert result.col_int == 1234

            # For equality, we use Decimal to quantize these values
            assert Decimal(result.col_double).quantize(Decimal("0.00")) == Decimal(
                3.14
            ).quantize(Decimal("0.00"))

            assert result.col_date == datetime(
                2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC
            )
            assert result.col_timestamp == datetime(
                2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC
            )
            assert result.col_str == "Hello"

    def test_parameterized_query_named_dict_and_inferred_e2e(self):
        """Verify that named parameters passed to the database as a list are returned of the correct type
        All types are inferred.
        """

        conn: Connection

        query = """
        SELECT
            :p_bool AS col_bool,
            :p_int AS col_int,
            :p_double AS col_double,
            :p_date as col_date,
            :p_timestamp as col_timestamp,
            :p_str AS col_str
        """

        named_parameters = [
            DbSqlParameter(
                name="p_bool",
                value=True,
            ),
            DbSqlParameter(
                name="p_int",
                value=1234,
            ),
            DbSqlParameter(
                name="p_double",
                value=3.14,
            ),
            DbSqlParameter(
                name="p_date",
                value=datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC),
            ),
            DbSqlParameter(
                name="p_timestamp",
                value=datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC),
            ),
            DbSqlParameter(name="p_str", value="Hello"),
        ]

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, parameters=named_parameters)
            result = cursor.fetchone()

            assert result.col_bool == True
            assert result.col_int == 1234

            # For equality, we use Decimal to quantize these values
            assert Decimal(result.col_double).quantize(Decimal("0.00")) == Decimal(
                3.14
            ).quantize(Decimal("0.00"))

            assert result.col_date == datetime(
                2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC
            )
            assert result.col_timestamp == datetime(
                2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC
            )
            assert result.col_str == "Hello"

    def test_parameterized_query_named_dict_and_not_inferred_e2e(self):
        """Verify that named parameters passed to the database are returned of the correct type
        All types are explicitly set.
        """

        conn: Connection

        query = """
        SELECT
            :p_bool AS col_bool,
            :p_int AS col_int,
            :p_double AS col_double,
            :p_date as col_date,
            :p_timestamp as col_timestamp,
            :p_str AS col_str
        """

        named_parameters = [
            DbSqlParameter(name="p_bool", value=True, type=DbSqlType.BOOLEAN),
            DbSqlParameter(name="p_int", value=1234, type=DbSqlType.INTEGER),
            DbSqlParameter(name="p_double", value=3.14, type=DbSqlType.FLOAT),
            DbSqlParameter(
                name="p_date",
                value=datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC),
                type=DbSqlType.DATE,
            ),
            DbSqlParameter(
                name="p_timestamp",
                value=datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC),
                type=DbSqlType.TIMESTAMP,
            ),
            DbSqlParameter(name="p_str", value="Hello", type=DbSqlType.STRING),
        ]

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, parameters=named_parameters)
            result = cursor.fetchone()

            assert result.col_bool == True
            assert result.col_int == 1234

            # For equality, we use Decimal to quantize these values
            assert Decimal(result.col_double).quantize(Decimal("0.00")) == Decimal(
                3.14
            ).quantize(Decimal("0.00"))

            # Observe that passing a datetime object with timezone information and the type set to DbSqlType.DATE
            # strips away the time and tz info
            assert (
                result.col_date
                == datetime(
                    2023,
                    9,
                    6,
                ).date()
            )
            assert result.col_timestamp == datetime(
                2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC
            )
            assert result.col_str == "Hello"
