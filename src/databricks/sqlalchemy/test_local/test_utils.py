import pytest
from databricks.sqlalchemy.utils import (
    extract_identifiers_from_string,
    extract_identifier_groups_from_string,
    extract_three_level_identifier_from_constraint_string
)


# These are outputs from DESCRIBE TABLE EXTENDED
@pytest.mark.parametrize(
    "input, expected",
    [
        ("PRIMARY KEY (`pk1`, `pk2`)", ["pk1", "pk2"]),
        ("PRIMARY KEY (`a`, `b`, `c`)", ["a", "b", "c"]),
        ("PRIMARY KEY (`name`, `id`, `attr`)", ["name", "id", "attr"]),
    ],
)
def test_extract_identifiers(input, expected):
    assert (
        extract_identifiers_from_string(input) == expected
    ), "Failed to extract identifiers from string"


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            "FOREIGN KEY (`pname`, `pid`, `pattr`) REFERENCES `main`.`pysql_sqlalchemy`.`tb1` (`name`, `id`, `attr`)",
            [
                "(`pname`, `pid`, `pattr`)",
                "(`name`, `id`, `attr`)",
            ],
        )
    ],
)
def test_extract_identifer_batches(input, expected):
    assert (
        extract_identifier_groups_from_string(input) == expected
    ), "Failed to extract identifier groups from string"

def test_extract_3l_namespace_from_constraint_string():

    input = "FOREIGN KEY (`parent_user_id`) REFERENCES `main`.`pysql_dialect_compliance`.`users` (`user_id`)"
    expected = {
        "catalog": "main",
        "schema": "pysql_dialect_compliance",
        "table": "users"
    }

    assert extract_three_level_identifier_from_constraint_string(input) == expected, "Failed to extract 3L namespace from constraint string"