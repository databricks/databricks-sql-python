import datetime

from sqlalchemy import BIGINT
from sqlalchemy import BOOLEAN
from sqlalchemy import DATE
from sqlalchemy import DECIMAL
from sqlalchemy import FLOAT
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import Interval
from sqlalchemy import SMALLINT
from sqlalchemy import String
from sqlalchemy import TIMESTAMP

from sqlalchemy import Table, Column

from sqlalchemy import and_
from sqlalchemy import asc
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import desc
from sqlalchemy import exc
from sqlalchemy import except_
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import intersect
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import not_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import TypeDecorator
from sqlalchemy import union
from sqlalchemy import union_all
from sqlalchemy import VARCHAR
from sqlalchemy.engine import default
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql.selectable import LABEL_STYLE_NONE
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import ComparesTables
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import resolve_lambda



class ReflectionTest(fixtures.TablesTest, ComparesTables):
    def test_numtypes(self, metadata, connection):
        meta = metadata

        # TODO: switch over to internal golden tables once all types are implemented
        all_num_types = Table(
            "all_num_types",
            meta,
            Column("f_short", SMALLINT),
            Column("f_int", INT),
            Column("f_long", BIGINT),
            Column("f_float", FLOAT),
            Column("f_decimal", DECIMAL(9,3)),
            Column("f_boolean", BOOLEAN),
        )

        meta.create_all(connection)

        meta2 = MetaData()
        reflected_types = Table(
            "all_num_types", meta2, autoload_with=connection
        )

        self.assert_tables_equal(all_num_types, reflected_types)


    # TODO: not working yet
    def test_strtypes(self, metadata, connection):
        meta = metadata

        all_num_types = Table(
            "all_str_types",
            meta,
            Column("f_string", String),
        )

        meta.create_all(connection)

        meta2 = MetaData()
        reflected_types = Table(
            "all_str_types", meta2, autoload_with=connection
        )

        self.assert_tables_equal(all_str_types, reflected_types)



class SimpleTest(fixtures.TablesTest, ComparesTables, AssertsExecutionResults):
    # __only_on__ = "databricks"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "simpletest_num",
            metadata,
            Column("f_byte", INT),
            Column("f_short", SMALLINT),
            Column("f_int", INT),
            Column("f_long", BIGINT),
            Column("f_float", FLOAT),
            Column("f_decimal", DECIMAL),
            Column("f_boolean", BOOLEAN),
            test_needs_acid=False,
        )

    def test_select_type_byte(self, connection):
        simpletest_num = self.tables.simpletest_num
        stmt = select([simpletest_num.c.f_byte])

        connection.execute(stmt)

    def test_select_type_inttype(self, connection):
        simpletest_num = self.tables.simpletest_num
        stmt = select([simpletest_num.c.f_int])

        connection.execute(stmt)


    def test_select_star_with_limit(self, connection):
        simpletest_num = self.tables.simpletest_num
        stmt = select([simpletest_num.c.f_byte]).limit(10)

        connection.execute(stmt)
