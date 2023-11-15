from datetime import date, datetime
import unittest, pytest, decimal
from typing import Any, Dict
from databricks.sql.parameters.native import dbsql_parameter_from_primitive

from databricks.sql.utils import ParamEscaper, inject_parameters, transform_paramstyle, ParameterStructure

pe = ParamEscaper()


class TestIndividualFormatters(object):
    # Test individual type escapers
    def test_escape_number_integer(self):
        """This behaviour falls back to Python's default string formatting of numbers"""
        assert pe.escape_number(100) == 100

    def test_escape_number_float(self):
        """This behaviour falls back to Python's default string formatting of numbers"""
        assert pe.escape_number(100.1234) == 100.1234

    def test_escape_number_decimal(self):
        """This behaviour uses the string representation of a decimal"""
        assert pe.escape_decimal(decimal.Decimal("124.32")) == "124.32"

    def test_escape_string_normal(self):
        """ """

        assert pe.escape_string("golly bob howdy") == "'golly bob howdy'"

    def test_escape_string_that_includes_special_characters(self):
        """Tests for how special characters are treated.

        When passed a string, the `escape_string` method wraps it in single quotes
        and escapes any special characters with a back stroke (\)

        Example:

        IN : his name was 'robert palmer'
        OUT: 'his name was \'robert palmer\''
        """

        # Testing for the presence of these characters: '"/\😂

        assert (
            pe.escape_string("his name was 'robert palmer'")
            == r"'his name was \'robert palmer\''"
        )

        # These tests represent the same user input in the several ways it can be written in Python
        # Each argument to `escape_string` evaluates to the same bytes. But Python lets us write it differently.
        assert (
            pe.escape_string('his name was "robert palmer"')
            == "'his name was \"robert palmer\"'"
        )
        assert (
            pe.escape_string('his name was "robert palmer"')
            == "'his name was \"robert palmer\"'"
        )
        assert (
            pe.escape_string("his name was {}".format('"robert palmer"'))
            == "'his name was \"robert palmer\"'"
        )

        assert (
            pe.escape_string("his name was robert / palmer")
            == r"'his name was robert / palmer'"
        )

        # If you need to include a single backslash, use an r-string to prevent Python from raising a
        # DeprecationWarning for an invalid escape sequence
        assert (
            pe.escape_string("his name was robert \\/ palmer")
            == r"'his name was robert \\/ palmer'"
        )
        assert (
            pe.escape_string("his name was robert \\ palmer")
            == r"'his name was robert \\ palmer'"
        )
        assert (
            pe.escape_string("his name was robert \\\\ palmer")
            == r"'his name was robert \\\\ palmer'"
        )

        assert (
            pe.escape_string("his name was robert palmer 😂")
            == r"'his name was robert palmer 😂'"
        )

        # Adding the test from PR #56 to prove escape behaviour

        assert pe.escape_string("you're") == r"'you\'re'"

        # Adding this test from #51 to prove escape behaviour when the target string involves repeated SQL escape chars
        assert pe.escape_string("cat\\'s meow") == r"'cat\\\'s meow'"

        # Tests from the docs: https://docs.databricks.com/sql/language-manual/data-types/string-type.html

        assert pe.escape_string("Spark") == "'Spark'"
        assert pe.escape_string("O'Connell") == r"'O\'Connell'"
        assert pe.escape_string("Some\\nText") == r"'Some\\nText'"
        assert pe.escape_string("Some\\\\nText") == r"'Some\\\\nText'"
        assert pe.escape_string("서울시") == "'서울시'"
        assert pe.escape_string("\\\\") == r"'\\\\'"

    def test_escape_date_time(self):
        INPUT = datetime(1991, 8, 3, 21, 55)
        FORMAT = "%Y-%m-%d %H:%M:%S"
        OUTPUT = "'1991-08-03 21:55:00'"
        assert pe.escape_datetime(INPUT, FORMAT) == OUTPUT

    def test_escape_date(self):
        INPUT = date(1991, 8, 3)
        FORMAT = "%Y-%m-%d"
        OUTPUT = "'1991-08-03'"
        assert pe.escape_datetime(INPUT, FORMAT) == OUTPUT

    def test_escape_sequence_integer(self):
        assert pe.escape_sequence([1, 2, 3, 4]) == "(1,2,3,4)"

    def test_escape_sequence_float(self):
        assert pe.escape_sequence([1.1, 2.2, 3.3, 4.4]) == "(1.1,2.2,3.3,4.4)"

    def test_escape_sequence_string(self):
        assert (
            pe.escape_sequence(["his", "name", "was", "robert", "palmer"])
            == "('his','name','was','robert','palmer')"
        )

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


class TestInlineToNativeTransformer(object):
    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            ("no effect", "SELECT 1", {}, "SELECT 1"),
            ("one marker", "%(param)s", {"param": ""}, ":param"),
            (
                "multiple markers",
                "%(foo)s %(bar)s %(baz)s",
                {"foo": None, "bar": None, "baz": None},
                ":foo :bar :baz",
            ),
            (
                "sql query",
                "SELECT * FROM table WHERE field = %(param)s AND other_field IN (%(list)s)",
                {"param": None, "list": None},
                "SELECT * FROM table WHERE field = :param AND other_field IN (:list)",
            ),
            (
                "query with like wildcard",
                'select * from table where field like "%"',
                {},
                'select * from table where field like "%"'
            ),
            (
                "query with named param and like wildcard",
                'select :param from table where field like "%"',
                {"param": None},
                'select :param from table where field like "%"'
            ),
            (
                "query with doubled wildcards",
                'select 1 where '' like "%%"',
                {"param": None},
                'select 1 where '' like "%%"',
            )
        ),
    )
    def test_transformer(
        self, label: str, query: str, params: Dict[str, Any], expected: str
    ):
        
        _params = [dbsql_parameter_from_primitive(value=value, name=name) for name, value in params.items()]
        output = transform_paramstyle(query, _params, param_structure=ParameterStructure.NAMED)
        assert output == expected
