from typing import List
import re


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
