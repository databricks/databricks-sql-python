from datetime import date, datetime
import unittest, pytest, decimal
from typing import Any, Dict
from unittest.mock import patch
from databricks.sql.parameters.native import dbsql_parameter_from_primitive
from databricks.sql.exc import ProgrammingError
from databricks.sql.types import Row

from databricks.sql.utils import (
    ParamEscaper,
    inject_parameters,
    transform_paramstyle,
    ParameterStructure,
)

pe = ParamEscaper()


def _fake_probe(index: int) -> str:
    """Text shaped like render_inline's probe token for marker `index`, with a fixed nonce."""
    return f"\x000123456789abcdef:{index}\x00"


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
        r"""Tests for how special characters are treated.

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

    def test_escape_item_bytes_as_string(self):
        """bytes are escaped like the equivalent str, not as an ARRAY of ints"""
        assert pe.escape_item(b"golly bob howdy") == "'golly bob howdy'"
        assert pe.escape_item("golly bob howdy".encode("utf-8")) == pe.escape_item(
            "golly bob howdy"
        )
        assert pe.escape_item([b"his", b"name"]) == "ARRAY('his','name')"

    def test_escape_sequence_integer(self):
        assert pe.escape_sequence([1, 2, 3, 4]) == "ARRAY(1,2,3,4)"

    def test_escape_sequence_float(self):
        assert pe.escape_sequence([1.1, 2.2, 3.3, 4.4]) == "ARRAY(1.1,2.2,3.3,4.4)"

    def test_escape_sequence_string(self):
        assert (
            pe.escape_sequence(["his", "name", "was", "robert", "palmer"])
            == "ARRAY('his','name','was','robert','palmer')"
        )

    def test_escape_sequence_sequence_of_strings(self):
        INPUT = [["his", "name"], ["was", "robert"], ["palmer"]]
        OUTPUT = "ARRAY(ARRAY('his','name'),ARRAY('was','robert'),ARRAY('palmer'))"

        assert pe.escape_sequence(INPUT) == OUTPUT

    def test_escape_map_string_int(self):
        INPUT = {"a": 1, "b": 2}
        OUTPUT = "MAP('a',1,'b',2)"
        assert pe.escape_mapping(INPUT) == OUTPUT

    def test_escape_map_string_sequence_of_floats(self):
        INPUT = {"a": [1.1, 2.2, 3.3], "b": [4.4, 5.5, 6.6]}
        OUTPUT = "MAP('a',ARRAY(1.1,2.2,3.3),'b',ARRAY(4.4,5.5,6.6))"
        assert pe.escape_mapping(INPUT) == OUTPUT

    def test_escape_sequence_of_map_int_string(self):
        INPUT = [{1: "a", 2: "foo"}, {3: "b", 4: "bar"}]
        OUTPUT = "ARRAY(MAP(1,'a',2,'foo'),MAP(3,'b',4,'bar'))"
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


class TestInClauseSequenceRendering(object):
    """Regression tests for databricks-sql-python#629.

    The IN predicate needs a parenthesised list; Spark rejects `x IN ARRAY(...)` as a syntax
    error. Everywhere else a sequence must keep rendering as an ARRAY literal so inline values can
    still be written to ARRAY columns.
    """

    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            (
                "named list",
                "SELECT * FROM t WHERE x IN %(ids)s",
                {"ids": [1, 2, 3]},
                "SELECT * FROM t WHERE x IN (1,2,3)",
            ),
            (
                "positional list",
                "SELECT * FROM t WHERE x IN %s",
                ([1, 2, 3],),
                "SELECT * FROM t WHERE x IN (1,2,3)",
            ),
            (
                "tuple of strings is escaped",
                "SELECT * FROM t WHERE name IN %(names)s",
                {"names": ("a", "O'Connell")},
                r"SELECT * FROM t WHERE name IN ('a','O\'Connell')",
            ),
            (
                "NOT IN",
                "SELECT * FROM t WHERE x NOT IN %(ids)s",
                {"ids": [1, 2]},
                "SELECT * FROM t WHERE x NOT IN (1,2)",
            ),
            (
                "lowercase keyword and newline before the marker",
                "SELECT * FROM t WHERE x in\n  %(ids)s",
                {"ids": [1, 2]},
                "SELECT * FROM t WHERE x in\n  (1,2)",
            ),
            (
                "nested lists stay arrays",
                "SELECT * FROM t WHERE arr IN %(arrs)s",
                {"arrs": [[1, 2], [3]]},
                "SELECT * FROM t WHERE arr IN (ARRAY(1,2),ARRAY(3))",
            ),
            (
                "nested tuples render as rows",
                "SELECT * FROM t WHERE (a, b) IN %(pairs)s",
                {"pairs": [(1, "x"), (2, "O'Connell")]},
                r"SELECT * FROM t WHERE (a, b) IN ((1,'x'),(2,'O\'Connell'))",
            ),
            (
                "tuple of tuples with NOT IN, positional",
                "SELECT * FROM t WHERE (a, b) NOT IN %s",
                (((1, 2), (3, 4)),),
                "SELECT * FROM t WHERE (a, b) NOT IN ((1,2),(3,4))",
            ),
            (
                "Row objects render as rows",
                "SELECT * FROM t WHERE (a, b) IN %(rows)s",
                {"rows": [Row(a=1, b=2), Row(a=3, b=4)]},
                "SELECT * FROM t WHERE (a, b) IN ((1,2),(3,4))",
            ),
            (
                "a row can hold an array and a nested row",
                "SELECT * FROM t WHERE (a, arr, s) IN %(rows)s",
                {"rows": [(1, [2, 3], (4, 5))]},
                "SELECT * FROM t WHERE (a, arr, s) IN ((1,ARRAY(2,3),(4,5)))",
            ),
            (
                "only the positional marker after IN",
                "SELECT %s AS tags FROM t WHERE x IN %s AND y = %s",
                (["a"], [1, 2], 5),
                "SELECT ARRAY('a') AS tags FROM t WHERE x IN (1,2) AND y = 5",
            ),
            (
                "escaped percent does not shift positional markers",
                "SELECT '100%%' AS pct, %s AS tags FROM t WHERE x IN %s",
                (["a"], [1, 2]),
                "SELECT '100%' AS pct, ARRAY('a') AS tags FROM t WHERE x IN (1,2)",
            ),
            (
                "named %d marker elsewhere in the query",
                "SELECT * FROM t WHERE x IN %(ids)s LIMIT %(n)d",
                {"ids": [1, 2], "n": 5},
                "SELECT * FROM t WHERE x IN (1,2) LIMIT 5",
            ),
            (
                "positional %d marker elsewhere in the query",
                "SELECT * FROM t WHERE x IN %s LIMIT %d",
                ([1, 2], 5),
                "SELECT * FROM t WHERE x IN (1,2) LIMIT 5",
            ),
            (
                "PIVOT value list",
                "SELECT * FROM t PIVOT (sum(v) FOR k IN %(ks)s)",
                {"ks": ["a", "b"]},
                "SELECT * FROM t PIVOT (sum(v) FOR k IN ('a','b'))",
            ),
        ),
    )
    def test_sequence_after_in_renders_as_list(self, label, query, params, expected):
        assert pe.render_inline(query, params) == expected

    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            (
                "insert into an array column",
                "INSERT INTO t (arr) VALUES (%(p)s)",
                {"p": ["a", "b"]},
                "INSERT INTO t (arr) VALUES (ARRAY('a','b'))",
            ),
            (
                "function argument",
                "SELECT array_contains(%(p)s, 2)",
                {"p": [1, 2, 3]},
                "SELECT array_contains(ARRAY(1,2,3), 2)",
            ),
            (
                "caller-written parentheses after IN compare against an array value",
                "SELECT * FROM t WHERE arr IN (%(p)s)",
                {"p": [1, 2]},
                "SELECT * FROM t WHERE arr IN (ARRAY(1,2))",
            ),
            (
                "a tuple outside an IN list is still an array",
                "INSERT INTO t (arr) VALUES (%(p)s)",
                {"p": (1, 2)},
                "INSERT INTO t (arr) VALUES (ARRAY(1,2))",
            ),
        ),
    )
    def test_sequence_elsewhere_stays_array(self, label, query, params, expected):
        assert pe.render_inline(query, params) == expected

    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            (
                "same name after IN and as a function argument",
                "SELECT * FROM t WHERE x IN %(ids)s AND array_contains(%(ids)s, x)",
                {"ids": [1, 2]},
                "SELECT * FROM t WHERE x IN (1,2) AND array_contains(ARRAY(1,2), x)",
            ),
            (
                "commented-out IN does not change the live marker",
                "SELECT * FROM t WHERE arr = %(tags)s\n-- AND tag IN %(tags)s\n",
                {"tags": ["a", "b"]},
                "SELECT * FROM t WHERE arr = ARRAY('a','b')\n-- AND tag IN ('a','b')\n",
            ),
        ),
    )
    def test_rendering_is_chosen_per_marker(self, label, query, params, expected):
        assert pe.render_inline(query, params) == expected

    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            (
                "IN ending a line comment does not change the marker on the next line",
                "SELECT * FROM t WHERE arr =  -- tags we filter IN\n  %(tags)s",
                {"tags": ["a", "b"]},
                "SELECT * FROM t WHERE arr =  -- tags we filter IN\n  ARRAY('a','b')",
            ),
            (
                "the same with CRLF line endings",
                "SELECT * FROM t WHERE arr = -- filter IN\r\n  %(tags)s",
                {"tags": ["a"]},
                "SELECT * FROM t WHERE arr = -- filter IN\r\n  ARRAY('a')",
            ),
            (
                "a value containing -- does not hide an IN that ends the line",
                "SELECT * FROM t WHERE name = %(name)s AND id IN\n  %(ids)s",
                {"name": "a--b", "ids": [1, 2]},
                "SELECT * FROM t WHERE name = 'a--b' AND id IN\n  (1,2)",
            ),
            (
                "a -- string literal on the line of IN and the marker",
                "SELECT * FROM t WHERE sep = '--' AND id IN %(ids)s",
                {"ids": [1, 2]},
                "SELECT * FROM t WHERE sep = '--' AND id IN (1,2)",
            ),
            (
                # Detection reads the SQL text without parsing string literals, so this falls
                # back to the ARRAY rendering, as on main.
                "a -- string literal before an IN that ends its line keeps ARRAY",
                "SELECT * FROM t WHERE sep = '--' AND id IN\n  %(ids)s",
                {"ids": [1, 2]},
                "SELECT * FROM t WHERE sep = '--' AND id IN\n  ARRAY(1,2)",
            ),
        ),
    )
    def test_line_comments(self, label, query, params, expected):
        assert pe.render_inline(query, params) == expected

    def test_single_digit_decimal_at_a_c_marker_still_renders_the_in_list(self):
        query = "SELECT %(d)c AS digit FROM t WHERE x IN %(ids)s"
        params = {"d": decimal.Decimal("5"), "ids": [1, 2]}
        assert (
            pe.render_inline(query, params)
            == "SELECT 5 AS digit FROM t WHERE x IN (1,2)"
        )

    @pytest.mark.parametrize(
        ("label", "query", "params"),
        (
            (
                "named markers",
                "INSERT INTO t VALUES (%(s)s, %(n)s, %(arr)s, %(m)s, %(none)s)",
                {"s": "it's", "n": 1.5, "arr": [1, [2]], "m": {"k": 1}, "none": None},
            ),
            (
                "bare %s with dict parameters formats the whole dict",
                "SELECT * FROM t WHERE x IN %s",
                {"ids": [1]},
            ),
        ),
    )
    def test_matches_escape_args_without_in_list(self, label, query, params):
        assert pe.render_inline(query, params) == inject_parameters(
            query, pe.escape_args(params)
        )

    @pytest.mark.parametrize(
        ("query", "params"),
        (
            ("SELECT * FROM t WHERE a = %(a)s AND b IN (%(b)s)", {"a": "x", "b": 2}),
            ("SELECT * FROM t WHERE a = %s LIMIT %d", ("x", 5)),
            ("SELECT * FROM t WHERE a = %s", {"a": "x"}),
        ),
    )
    def test_without_sequences_renders_as_escape_args_without_probing(
        self, query, params
    ):
        with patch.object(
            ParamEscaper, "_markers_following_in", side_effect=AssertionError
        ) as probe:
            assert pe.render_inline(query, params) == inject_parameters(
                query, pe.escape_args(params)
            )
        probe.assert_not_called()

    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            (
                # The operation's own text reaches the probe pass unchanged, so only the nonce
                # check keeps this token from marking %(tags)s (marker 0) as an IN list.
                "probe-shaped text in the operation",
                "SELECT * FROM t WHERE x IN " + _fake_probe(0) + " AND arr = %(tags)s",
                {"tags": ["a"]},
                "SELECT * FROM t WHERE x IN "
                + _fake_probe(0)
                + " AND arr = ARRAY('a')",
            ),
            (
                # String values are blanked during the probe pass, so their text cannot mark
                # %(tags)s (marker 1) as an IN list either.
                "probe-shaped text in a string value",
                "INSERT INTO t VALUES (%(note)s, %(tags)s)",
                {"note": "x IN " + _fake_probe(1), "tags": ["a"]},
                "INSERT INTO t VALUES ('x IN " + _fake_probe(1) + "', ARRAY('a'))",
            ),
        ),
    )
    def test_probe_shaped_text_is_not_a_probe(self, label, query, params, expected):
        assert pe.render_inline(query, params) == expected

    @pytest.mark.parametrize(
        ("query", "params", "error", "message"),
        (
            (
                "SELECT * FROM t WHERE x IN %s AND name LIKE '%sonite'",
                ([1, 2],),
                TypeError,
                "not enough arguments for format string",
            ),
            (
                "SELECT * FROM t WHERE x IN %d",
                ([1, 2],),
                TypeError,
                "a real number is required, not str",
            ),
            ("SELECT * FROM t WHERE x IN %(nope)s", {"ids": [1]}, KeyError, "nope"),
        ),
    )
    def test_errors_match_escape_args(self, query, params, error, message):
        with pytest.raises(error, match=message):
            inject_parameters(query, pe.escape_args(params))
        with pytest.raises(error, match=message):
            pe.render_inline(query, params)

    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            (
                "set, sorted",
                "SELECT * FROM t WHERE x IN %(ids)s",
                {"ids": {3, 1, 2}},
                "SELECT * FROM t WHERE x IN (1,2,3)",
            ),
            (
                "frozenset, positional, NOT IN",
                "SELECT * FROM t WHERE x NOT IN %s",
                (frozenset({2, 1}),),
                "SELECT * FROM t WHERE x NOT IN (1,2)",
            ),
            (
                "dict keys",
                "SELECT * FROM t WHERE name IN %(names)s",
                {"names": {"b": 1, "a": 2}.keys()},
                "SELECT * FROM t WHERE name IN ('a','b')",
            ),
            (
                "items that do not compare are sorted by repr",
                "SELECT * FROM t WHERE x IN %(ids)s",
                {"ids": {None, 1}},
                "SELECT * FROM t WHERE x IN (1,NULL)",
            ),
            (
                "set of tuples renders sorted rows",
                "SELECT * FROM t WHERE (a, b) IN %(pairs)s",
                {"pairs": {(2, "b"), (1, "a")}},
                "SELECT * FROM t WHERE (a, b) IN ((1,'a'),(2,'b'))",
            ),
            (
                "same set after IN twice",
                "SELECT * FROM t WHERE x IN %(ids)s OR y IN %(ids)s",
                {"ids": {2, 1}},
                "SELECT * FROM t WHERE x IN (1,2) OR y IN (1,2)",
            ),
            (
                "set next to an array elsewhere",
                "SELECT %(tags)s AS tags FROM t WHERE x IN %(ids)s",
                {"tags": ["a"], "ids": {1}},
                "SELECT ARRAY('a') AS tags FROM t WHERE x IN (1)",
            ),
        ),
    )
    def test_set_after_in_renders_as_sorted_list(self, label, query, params, expected):
        assert pe.render_inline(query, params) == expected

    @pytest.mark.parametrize(
        ("label", "query", "params", "expected"),
        (
            (
                "bytes",
                "SELECT * FROM t WHERE x IN %(b)s",
                {"b": b"ab"},
                "SELECT * FROM t WHERE x IN ARRAY(97,98)",
            ),
            (
                "bytearray, positional",
                "SELECT * FROM t WHERE x IN %s",
                (bytearray(b"ab"),),
                "SELECT * FROM t WHERE x IN ARRAY(97,98)",
            ),
            (
                "bytes next to a list after IN",
                "SELECT * FROM t WHERE x IN %(b)s AND y IN %(ids)s",
                {"b": b"ab", "ids": [1]},
                "SELECT * FROM t WHERE x IN ARRAY(97,98) AND y IN (1)",
            ),
        ),
    )
    def test_bytes_after_in_keep_the_array_rendering(
        self, label, query, params, expected
    ):
        """bytes render as an ARRAY of byte values. After IN they keep that rendering, which Spark
        rejects as on main, instead of becoming a valid IN list that matches the wrong rows.
        """
        assert pe.render_inline(query, params) == expected

    @pytest.mark.parametrize(
        ("label", "query", "params"),
        (
            (
                "set at a marker not after IN",
                "INSERT INTO t VALUES (%(s)s)",
                {"s": {1}},
            ),
            (
                "set used after IN and elsewhere",
                "SELECT %(ids)s FROM t WHERE x IN %(ids)s",
                {"ids": {1}},
            ),
            ("unused set", "SELECT * FROM t WHERE x = %(a)s", {"a": 1, "s": {1}}),
            ("set after IN in caller-written parentheses", "SELECT 1 IN (%s)", ({1},)),
            ("set inside a list", "SELECT * FROM t WHERE x IN %(v)s", {"v": [{1}]}),
            ("set at a %d marker after IN", "SELECT * FROM t WHERE x IN %d", ({1},)),
            ("set in an operation that does not format", "SELECT %s, %s", ({1},)),
            ("set in an IN that ends a line comment", "SELECT -- IN\n %s", ({1},)),
        ),
    )
    def test_set_elsewhere_raises_as_before(self, label, query, params):
        with pytest.raises(ProgrammingError, match="Unsupported object"):
            inject_parameters(query, pe.escape_args(params))
        with pytest.raises(ProgrammingError, match="Unsupported object"):
            pe.render_inline(query, params)


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
                'select * from table where field like "%"',
            ),
            (
                "query with named param and like wildcard",
                'select :param from table where field like "%"',
                {"param": None},
                'select :param from table where field like "%"',
            ),
            (
                "query with doubled wildcards",
                "select 1 where " ' like "%%"',
                {"param": None},
                "select 1 where " ' like "%%"',
            ),
        ),
    )
    def test_transformer(
        self, label: str, query: str, params: Dict[str, Any], expected: str
    ):

        _params = [
            dbsql_parameter_from_primitive(value=value, name=name)
            for name, value in params.items()
        ]
        output = transform_paramstyle(
            query, _params, param_structure=ParameterStructure.NAMED
        )
        assert output == expected
