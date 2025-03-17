import decimal
import datetime
from datetime import timezone, timedelta

from databricks.sql.utils import convert_to_assigned_datatypes_in_column_table


class TestUtils:
    def get_column_table_and_description(self):
        table_description = [
            ("id", "int", None, None, None, None, None),
            ("varchar_column", "string", None, None, None, None, None),
            ("boolean_column", "boolean", None, None, None, None, None),
            ("integer_column", "int", None, None, None, None, None),
            ("bigint_column", "bigint", None, None, None, None, None),
            ("smallint_column", "smallint", None, None, None, None, None),
            ("tinyint_column", "tinyint", None, None, None, None, None),
            ("float_column", "float", None, None, None, None, None),
            ("double_column", "double", None, None, None, None, None),
            ("decimal_column", "decimal", None, None, 10, 2, None),
            ("date_column", "date", None, None, None, None, None),
            ("timestamp_column", "timestamp", None, None, None, None, None),
            ("timestamp_ntz_column", "timestamp", None, None, None, None, None),
            ("timestamp_column_2", "timestamp", None, None, None, None, None),
            ("timestamp_column_3", "timestamp", None, None, None, None, None),
            ("timestamp_column_4", "timestamp", None, None, None, None, None),
            ("timestamp_column_5", "timestamp", None, None, None, None, None),
            ("timestamp_column_6", "timestamp", None, None, None, None, None),
            ("timestamp_column_7", "timestamp", None, None, None, None, None),
            ("binary_column", "binary", None, None, None, None, None),
            ("array_column", "array", None, None, None, None, None),
            ("map_column", "map", None, None, None, None, None),
            ("struct_column", "struct", None, None, None, None, None),
            ("variant_column", "string", None, None, None, None, None),
        ]

        column_table = [
            (9,),
            ("Test Varchar",),
            (True,),
            (123,),
            (9876543210,),
            (32000,),
            (120,),
            (1.23,),
            (4.56,),
            ("7890.12",),
            ("2023-12-31",),
            ("2023-12-31 12:30:00",),
            ("2023-12-31 12:30:00",),
            ("2021-09-30 11:27:35.123",),
            ("03/08/2024 02:30:15 PM",),
            ("08-Mar-2024 14:30:15",),
            ("2024-03-16T14:30:25.123",),
            ("2025-03-16T12:30:45+0530",),
            ("2025-03-16 12:30:45 +0530",),
            (b"\xde\xad\xbe\xef",),
            ('["item1","item2"]',),
            ('{"key1":"value1","key2":"value2"}',),
            ('{"name":"John","age":30}',),
            ('"semi-structured data"',),
        ]

        return column_table, table_description

    def test_convert_to_assigned_datatypes_in_column_table(self):
        column_table, description = self.get_column_table_and_description()
        converted_column_table = convert_to_assigned_datatypes_in_column_table(
            column_table, description
        )

        # (data , datatype)
        expected_convertion = [
            (9, int),
            ("Test Varchar", str),
            (True, bool),
            (123, int),
            (9876543210, int),
            (32000, int),
            (120, int),
            (1.23, float),
            (4.56, float),
            (decimal.Decimal("7890.12"), decimal.Decimal),
            (datetime.date(2023, 12, 31), datetime.date),
            (datetime.datetime(2023, 12, 31, 12, 30, 0), datetime.datetime),
            (datetime.datetime(2023, 12, 31, 12, 30, 0), datetime.datetime),
            (datetime.datetime(2021, 9, 30, 11, 27, 35, 123000), datetime.datetime),
            (datetime.datetime(2024, 3, 8, 14, 30, 15), datetime.datetime),
            (datetime.datetime(2024, 3, 8, 14, 30, 15), datetime.datetime),
            (datetime.datetime(2024, 3, 16, 14, 30, 25, 123000), datetime.datetime),
            (
                datetime.datetime(
                    2025,
                    3,
                    16,
                    12,
                    30,
                    45,
                    tzinfo=timezone(timedelta(hours=5, minutes=30)),
                ),
                datetime.datetime,
            ),
            (
                datetime.datetime(
                    2025,
                    3,
                    16,
                    12,
                    30,
                    45,
                    tzinfo=timezone(timedelta(hours=5, minutes=30)),
                ),
                datetime.datetime,
            ),
            (b"\xde\xad\xbe\xef", bytes),
            ('["item1","item2"]', str),
            ('{"key1":"value1","key2":"value2"}', str),
            ('{"name":"John","age":30}', str),
            ('"semi-structured data"', str),
        ]

        for index, entry in enumerate(converted_column_table):
            assert entry[0] == expected_convertion[index][0]
            assert isinstance(entry[0], expected_convertion[index][1])
