"""Additional tests authored by Databricks that use SQLAlchemy's test fixtures
"""

from sqlalchemy.testing.suite.test_types import (
    _LiteralRoundTripFixture,
    fixtures,
    testing,
    eq_,
    select,
    Table,
    Column,
    config,
)
from databricks.sqlalchemy import TINYINT


class TinyIntegerTest(_LiteralRoundTripFixture, fixtures.TestBase):
    __backend__ = True

    def test_literal(self, literal_round_trip):
        literal_round_trip(TINYINT, [5], [5])

    @testing.fixture
    def integer_round_trip(self, metadata, connection):
        def run(datatype, data):
            int_table = Table(
                "tiny_integer_table",
                metadata,
                Column(
                    "id",
                    TINYINT,
                    primary_key=True,
                    test_needs_autoincrement=False,
                ),
                Column("tiny_integer_data", datatype),
            )

            metadata.create_all(config.db)

            connection.execute(int_table.insert(), {"id": 1, "integer_data": data})

            row = connection.execute(select(int_table.c.integer_data)).first()

            eq_(row, (data,))

            assert isinstance(row[0], int)

        return run
