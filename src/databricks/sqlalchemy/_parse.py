from typing import List, Optional
import re

"""
This module contains helper functions that can parse the contents
of DESCRIBE TABLE EXTENDED calls. Mostly wrappers around regexes.
"""

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
    """
    pat = re.compile(r"REFERENCES\s+(.*?)\s*\(")
    matches = pat.findall(input_str)
    
    if not matches:
        return None
    
    first_match = matches[0]
    parts = first_match.split(".")

    def strip_backticks(input:str):
        return input.replace("`", "")
    
    return {
        "catalog": strip_backticks(parts[0]),  
        "schema": strip_backticks(parts[1]),
        "table": strip_backticks(parts[2])
    }

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

    complete_foreign_key_dict = {
        "name": fk_name,
        **base_fk_dict,
        **schema_override_dict,
    }

    return complete_foreign_key_dict