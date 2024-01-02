"""Additional tests authored by Databricks that use SQLAlchemy's test fixtures
"""

import datetime

from sqlalchemy import Integer, String, schema, inspect
from sqlalchemy.testing import util
from sqlalchemy.testing.config import requirements
from sqlalchemy.testing.suite.test_types import (
    _LiteralRoundTripFixture,
    fixtures,
    testing,
    eq_,
    select,
    Table,
    Column,
    config,
    _DateFixture,
    literal,
)
from databricks.sqlalchemy import TINYINT, TIMESTAMP


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


class DateTimeTZTestCustom(_DateFixture, fixtures.TablesTest):
    """This test confirms that when a user uses the TIMESTAMP
    type to store a datetime object, it retains its timezone
    """

    __backend__ = True
    datatype = TIMESTAMP
    data = datetime.datetime(2012, 10, 15, 12, 57, 18, tzinfo=datetime.timezone.utc)

    @testing.requires.datetime_implicit_bound
    def test_select_direct(self, connection):
        # We need to pass the TIMESTAMP type to the literal function
        # so that the value is processed correctly.
        result = connection.scalar(select(literal(self.data, TIMESTAMP)))
        eq_(result, self.data)


class TableDDLTestCustom(fixtures.TestBase):
    """This test confirms that a table comment can be dropped.
    The difference with TableDDLTest is that the comment value is '' and not None after
    being dropped.
    """

    __backend__ = True

    def _simple_fixture(self, schema=None):
        return Table(
            "test_table",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
            schema=schema,
        )

    @requirements.comment_reflection
    @util.provide_metadata
    def test_drop_table_comment(self, connection):
        table = self._simple_fixture()
        table.create(connection, checkfirst=False)
        table.comment = "a comment"
        connection.execute(schema.SetTableComment(table))
        connection.execute(schema.DropTableComment(table))
        eq_(inspect(connection).get_table_comment("test_table"), {"text": ""})


class FutureTableDDLTestCustom(fixtures.FutureEngineMixin, TableDDLTestCustom):
    pass
