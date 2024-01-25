from typing import List, Optional, Dict
import re

import sqlalchemy
from sqlalchemy.engine import CursorResult
from sqlalchemy.engine.interfaces import ReflectedColumn

from databricks.sqlalchemy import _types as type_overrides

"""
This module contains helper functions that can parse the contents
of metadata and exceptions received from DBR. These are mostly just
wrappers around regexes.
"""


class DatabricksSqlAlchemyParseException(Exception):
    pass


def _match_table_not_found_string(message: str) -> bool:
    """Return True if the message contains a substring indicating that a table was not found"""

    DBR_LTE_12_NOT_FOUND_STRING = "Table or view not found"
    DBR_GT_12_NOT_FOUND_STRING = "TABLE_OR_VIEW_NOT_FOUND"
    return any(
        [
            DBR_LTE_12_NOT_FOUND_STRING in message,
            DBR_GT_12_NOT_FOUND_STRING in message,
        ]
    )


def _describe_table_extended_result_to_dict_list(
    result: CursorResult,
) -> List[Dict[str, str]]:
    """Transform the CursorResult of DESCRIBE TABLE EXTENDED into a list of Dictionaries"""

    rows_to_return = []
    for row in result.all():
        this_row = {"col_name": row.col_name, "data_type": row.data_type}
        rows_to_return.append(this_row)

    return rows_to_return


def extract_identifiers_from_string(input_str: str) -> List[str]:
    """For a string input resembling (`a`, `b`, `c`) return a list of identifiers ['a', 'b', 'c']"""

    # This matches the valid character list contained in DatabricksIdentifierPreparer
    pattern = re.compile(r"`([A-Za-z0-9_]+)`")
    matches = pattern.findall(input_str)
    return [i for i in matches]


def extract_identifier_groups_from_string(input_str: str) -> List[str]:
    """For a string input resembling :

    FOREIGN KEY (`pname`, `pid`, `pattr`) REFERENCES `main`.`pysql_sqlalchemy`.`tb1` (`name`, `id`, `attr`)

    Return ['(`pname`, `pid`, `pattr`)', '(`name`, `id`, `attr`)']
    """
    pattern = re.compile(r"\([`A-Za-z0-9_,\s]*\)")
    matches = pattern.findall(input_str)
    return [i for i in matches]


def extract_three_level_identifier_from_constraint_string(input_str: str) -> dict:
    """For a string input resembling :
    FOREIGN KEY (`parent_user_id`) REFERENCES `main`.`pysql_dialect_compliance`.`users` (`user_id`)

    Return a dict like
        {
            "catalog": "main",
            "schema": "pysql_dialect_compliance",
            "table": "users"
        }

    Raise a DatabricksSqlAlchemyParseException if a 3L namespace isn't found
    """
    pat = re.compile(r"REFERENCES\s+(.*?)\s*\(")
    matches = pat.findall(input_str)

    if not matches:
        raise DatabricksSqlAlchemyParseException(
            "3L namespace not found in constraint string"
        )

    first_match = matches[0]
    parts = first_match.split(".")

    def strip_backticks(input: str):
        return input.replace("`", "")

    try:
        return {
            "catalog": strip_backticks(parts[0]),
            "schema": strip_backticks(parts[1]),
            "table": strip_backticks(parts[2]),
        }
    except IndexError:
        raise DatabricksSqlAlchemyParseException(
            "Incomplete 3L namespace found in constraint string: " + ".".join(parts)
        )


def _parse_fk_from_constraint_string(constraint_str: str) -> dict:
    """Build a dictionary of foreign key constraint information from a constraint string.

    For example:

    ```
    FOREIGN KEY (`pname`, `pid`, `pattr`) REFERENCES `main`.`pysql_dialect_compliance`.`tb1` (`name`, `id`, `attr`)
    ```

    Return a dictionary like:

    ```
    {
        "constrained_columns": ["pname", "pid", "pattr"],
        "referred_table": "tb1",
        "referred_schema": "pysql_dialect_compliance",
        "referred_columns": ["name", "id", "attr"]
    }
    ```

    Note that the constraint name doesn't appear in the constraint string so it will not
    be present in the output of this function.
    """

    referred_table_dict = extract_three_level_identifier_from_constraint_string(
        constraint_str
    )
    referred_table = referred_table_dict["table"]
    referred_schema = referred_table_dict["schema"]

    # _extracted is a tuple of two lists of identifiers
    # we assume the first immediately follows "FOREIGN KEY" and the second
    # immediately follows REFERENCES $tableName
    _extracted = extract_identifier_groups_from_string(constraint_str)
    constrained_columns_str, referred_columns_str = (
        _extracted[0],
        _extracted[1],
    )

    constrained_columns = extract_identifiers_from_string(constrained_columns_str)
    referred_columns = extract_identifiers_from_string(referred_columns_str)

    return {
        "constrained_columns": constrained_columns,
        "referred_table": referred_table,
        "referred_columns": referred_columns,
        "referred_schema": referred_schema,
    }


def build_fk_dict(
    fk_name: str, fk_constraint_string: str, schema_name: Optional[str]
) -> dict:
    """
    Given a foriegn key name and a foreign key constraint string, return a dictionary
    with the following keys:

    name
        the name of the foreign key constraint
    constrained_columns
        a list of column names that make up the foreign key
    referred_table
        the name of the table that the foreign key references
    referred_columns
        a list of column names that are referenced by the foreign key
    referred_schema
        the name of the schema that the foreign key references.

    referred schema will be None if the schema_name argument is None.
    This is required by SQLAlchey's ComponentReflectionTest::test_get_foreign_keys
    """

    # The foreign key name is not contained in the constraint string so we
    # need to add it manually
    base_fk_dict = _parse_fk_from_constraint_string(fk_constraint_string)

    if not schema_name:
        schema_override_dict = dict(referred_schema=None)
    else:
        schema_override_dict = {}

    # mypy doesn't like this method of conditionally adding a key to a dictionary
    # while keeping everything immutable
    complete_foreign_key_dict = {
        "name": fk_name,
        **base_fk_dict,
        **schema_override_dict,  # type: ignore
    }

    return complete_foreign_key_dict


def _parse_pk_columns_from_constraint_string(constraint_str: str) -> List[str]:
    """Build a list of constrained columns from a constraint string returned by DESCRIBE TABLE EXTENDED

    For example:

    PRIMARY KEY (`id`, `name`, `email_address`)

    Returns a list like

    ["id", "name", "email_address"]
    """

    _extracted = extract_identifiers_from_string(constraint_str)

    return _extracted


def build_pk_dict(pk_name: str, pk_constraint_string: str) -> dict:
    """Given a primary key name and a primary key constraint string, return a dictionary
    with the following keys:

    constrained_columns
      A list of string column names that make up the primary key

    name
      The name of the primary key constraint
    """

    constrained_columns = _parse_pk_columns_from_constraint_string(pk_constraint_string)

    return {"constrained_columns": constrained_columns, "name": pk_name}


def match_dte_rows_by_value(dte_output: List[Dict[str, str]], match: str) -> List[dict]:
    """Return a list of dictionaries containing only the col_name:data_type pairs where the `data_type`
    value contains the match argument.

    Today, DESCRIBE TABLE EXTENDED doesn't give a deterministic name to the fields
    a constraint will be found in its output. So we cycle through its output looking
    for a match. This is brittle. We could optionally make two roundtrips: the first
    would query information_schema for the name of the constraint on this table, and
    a second to DESCRIBE TABLE EXTENDED, at which point we would know the name of the
    constraint. But for now we instead assume that Python list comprehension is faster
    than a network roundtrip
    """

    output_rows = []

    for row_dict in dte_output:
        if match in row_dict["data_type"]:
            output_rows.append(row_dict)

    return output_rows


def match_dte_rows_by_key(dte_output: List[Dict[str, str]], match: str) -> List[dict]:
    """Return a list of dictionaries containing only the col_name:data_type pairs where the `col_name`
    value contains the match argument.
    """

    output_rows = []

    for row_dict in dte_output:
        if match in row_dict["col_name"]:
            output_rows.append(row_dict)

    return output_rows


def get_fk_strings_from_dte_output(dte_output: List[Dict[str, str]]) -> List[dict]:
    """If the DESCRIBE TABLE EXTENDED output contains foreign key constraints, return a list of dictionaries,
    one dictionary per defined constraint
    """

    output = match_dte_rows_by_value(dte_output, "FOREIGN KEY")

    return output


def get_pk_strings_from_dte_output(
    dte_output: List[Dict[str, str]]
) -> Optional[List[dict]]:
    """If the DESCRIBE TABLE EXTENDED output contains primary key constraints, return a list of dictionaries,
    one dictionary per defined constraint.

    Returns None if no primary key constraints are found.
    """

    output = match_dte_rows_by_value(dte_output, "PRIMARY KEY")

    return output


def get_comment_from_dte_output(dte_output: List[Dict[str, str]]) -> Optional[str]:
    """Returns the value of the first "Comment" col_name data in dte_output"""
    output = match_dte_rows_by_key(dte_output, "Comment")
    if not output:
        return None
    else:
        return output[0]["data_type"]


# The keys of this dictionary are the values we expect to see in a
# TGetColumnsRequest's .TYPE_NAME attribute.
# These are enumerated in ttypes.py as class TTypeId.
# TODO: confirm that all types in TTypeId are included here.
GET_COLUMNS_TYPE_MAP = {
    "boolean": sqlalchemy.types.Boolean,
    "smallint": sqlalchemy.types.SmallInteger,
    "tinyint": type_overrides.TINYINT,
    "int": sqlalchemy.types.Integer,
    "bigint": sqlalchemy.types.BigInteger,
    "float": sqlalchemy.types.Float,
    "double": sqlalchemy.types.Float,
    "string": sqlalchemy.types.String,
    "varchar": sqlalchemy.types.String,
    "char": sqlalchemy.types.String,
    "binary": sqlalchemy.types.String,
    "array": sqlalchemy.types.String,
    "map": sqlalchemy.types.String,
    "struct": sqlalchemy.types.String,
    "uniontype": sqlalchemy.types.String,
    "decimal": sqlalchemy.types.Numeric,
    "timestamp": type_overrides.TIMESTAMP,
    "timestamp_ntz": type_overrides.TIMESTAMP_NTZ,
    "date": sqlalchemy.types.Date,
}


def parse_numeric_type_precision_and_scale(type_name_str):
    """Return an intantiated sqlalchemy Numeric() type that preserves the precision and scale indicated
    in the output from TGetColumnsRequest.

    type_name_str
      The value of TGetColumnsReq.TYPE_NAME.

    If type_name_str is "DECIMAL(18,5) returns sqlalchemy.types.Numeric(18,5)
    """

    pattern = re.compile(r"DECIMAL\((\d+,\d+)\)")
    match = re.search(pattern, type_name_str)
    precision_and_scale = match.group(1)
    precision, scale = tuple(precision_and_scale.split(","))

    return sqlalchemy.types.Numeric(int(precision), int(scale))


def parse_column_info_from_tgetcolumnsresponse(thrift_resp_row) -> ReflectedColumn:
    """Returns a dictionary of the ReflectedColumn schema parsed from
    a single of the result of a TGetColumnsRequest thrift RPC
    """

    pat = re.compile(r"^\w+")

    # This method assumes a valid TYPE_NAME field in the response.
    # TODO: add error handling in case TGetColumnsResponse format changes

    _raw_col_type = re.search(pat, thrift_resp_row.TYPE_NAME).group(0).lower()  # type: ignore
    _col_type = GET_COLUMNS_TYPE_MAP[_raw_col_type]

    if _raw_col_type == "decimal":
        final_col_type = parse_numeric_type_precision_and_scale(
            thrift_resp_row.TYPE_NAME
        )
    else:
        final_col_type = _col_type

    # See comments about autoincrement in test_suite.py
    # Since Databricks SQL doesn't currently support inline AUTOINCREMENT declarations
    # the autoincrement must be manually declared with an Identity() construct in SQLAlchemy
    # Other dialects can perform this extra Identity() step automatically. But that is not
    # implemented in the Databricks dialect right now. So autoincrement is currently always False.
    # It's not clear what IS_AUTO_INCREMENT in the thrift response actually reflects or whether
    # it ever returns a `YES`.

    # Per the guidance in SQLAlchemy's docstrings, we prefer to not even include an autoincrement
    # key in this dictionary.
    this_column = {
        "name": thrift_resp_row.COLUMN_NAME,
        "type": final_col_type,
        "nullable": bool(thrift_resp_row.NULLABLE),
        "default": thrift_resp_row.COLUMN_DEF,
        "comment": thrift_resp_row.REMARKS or None,
    }

    # TODO: figure out how to return sqlalchemy.interfaces in a way that mypy respects
    return this_column  # type: ignore
