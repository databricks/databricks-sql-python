import pytest
from databricks.sqlalchemy._parse import (
    extract_identifiers_from_string,
    extract_identifier_groups_from_string,
    extract_three_level_identifier_from_constraint_string,
    build_fk_dict
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

@pytest.mark.parametrize("schema", [None, "some_schema"])
def test_build_fk_dict(schema):
    fk_constraint_string = "FOREIGN KEY (`parent_user_id`) REFERENCES `main`.`some_schema`.`users` (`user_id`)"

    result = build_fk_dict("some_fk_name", fk_constraint_string, schema_name=schema)

    assert result == {
        "name": "some_fk_name",
        "constrained_columns": ["parent_user_id"],
        "referred_schema": schema,
        "referred_table": "users",
        "referred_columns": ["user_id"],
    }

