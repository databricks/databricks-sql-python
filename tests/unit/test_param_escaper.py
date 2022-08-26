from datetime import date, datetime
import unittest, pytest

from databricks.sql.utils import ParamEscaper, inject_parameters

pe = ParamEscaper()

class TestIndividualFormatters(object):

    # Test individual type escapers
    def test_escape_number_integer(self):
        """This behaviour falls back to Python's default string formatting of numbers
        """
        assert pe.escape_number(100) == 100

    def test_escape_number_float(self):
        """This behaviour falls back to Python's default string formatting of numbers
        """
        assert pe.escape_number(100.1234) == 100.1234

    def test_escape_string_normal(self):
        """
        """

        assert pe.escape_string("golly bob howdy") == "'golly bob howdy'"

    def test_escape_string_that_includes_quotes(self):
        # Databricks queries support just one special character: a single quote mark
        # These are escaped by doubling: 
        # e.g. INPUT: his name was 'robert palmer'
        # e.g. OUTPUT: 'his name was ''robert palmer'''

        assert pe.escape_string("his name was 'robert palmer'") == "'his name was ''robert palmer'''"

    def test_escape_date_time(self):
        INPUT = datetime(1991,8,3,21,55)
        OUTPUT = "1991-08-03 21:55:00"
        assert pe.escape_datetime(INPUT, OUTPUT)

    def test_escape_date(self):
        INPUT = date(1991,8,3)
        OUTPUT = "1991-08-03"
        assert pe.escape_datetime(INPUT, OUTPUT)

    def test_escape_sequence_integer(self):
        assert pe.escape_sequence([1,2,3,4]) == "(1,2,3,4)"

    def test_escape_sequence_float(self):
        assert pe.escape_sequence([1.1,2.2,3.3,4.4]) == "(1.1,2.2,3.3,4.4)"

    def test_escape_sequence_string(self):
        assert pe.escape_sequence(
            ["his", "name", "was", "robert", "palmer"]) == \
            "('his','name','was','robert','palmer')"

    def test_escape_sequence_sequence_of_strings(self):
        # This is not valid SQL.
        INPUT = [["his", "name"], ["was", "robert"], ["palmer"]]
        OUTPUT = "(('his','name'),('was','robert'),('palmer'))"

        assert pe.escape_sequence(INPUT) == OUTPUT


class TestFullQueryEscaping(object):

    def test_simple(self):

        INPUT = """
        SELECT
          field1,
          field2,
          field3
        FROM
          table
        WHERE
          field1 = %(param1)s
        """

        OUTPUT = """
        SELECT
          field1,
          field2,
          field3
        FROM
          table
        WHERE
          field1 = ';DROP ALL TABLES'
        """

        args = {"param1": ";DROP ALL TABLES"}

        assert inject_parameters(INPUT, pe.escape_args(args)) == OUTPUT

    @unittest.skipUnless(False, "Thrift server supports native parameter binding.")
    def test_only_bind_in_where_clause(self):

        INPUT = """
        SELECT
          %(field)s,
          field2,
          field3
        FROM table
        """

        args = {"field": "Some Value"}

        with pytest.raises(Exception):
            inject_parameters(INPUT, pe.escape_args(args))
