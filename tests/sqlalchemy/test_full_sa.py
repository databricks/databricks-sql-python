from sqlalchemy.testing.suite import *

from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import StringTest as _StringTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import BooleanTest as _BooleanTest

from sqlalchemy.testing.suite import DateTest as _DateTest
# from sqlalchemy.testing.suite import _LiteralRoundTripFixture

from sqlalchemy.testing.suite import DateTimeTest as _DateTimeTest


from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest

from sqlalchemy.testing.suite import JoinTest as _JoinTest

# class _MyDateFixture(_LiteralRoundTripFixture, fixtures.TestBase):
#     compare = None

#     @classmethod
#     def define_tables(cls, metadata):
#         # class Decorated(TypeDecorator):
#         #     impl = cls.datatype
#         #     cache_ok = True

#         Table(
#             "date_table",
#             metadata,
#             Column(
#                 "id", Integer, primary_key=True, test_needs_autoincrement=True
#             ),
#             Column("date_data", cls.datatype),
#             # Column("decorated_date_data", Decorated),
#         )

#     @testing.requires.datetime_implicit_bound
#     def test_select_direct(self, connection):
#         result = connection.scalar(select(literal(self.data)))
#         eq_(result, self.data)

#     def test_round_trip(self, connection):
#         date_table = self.tables.date_table

#         connection.execute(
#             date_table.insert(), {"id": 1, "date_data": self.data}
#         )

#         row = connection.execute(select(date_table.c.date_data)).first()

#         compare = self.compare or self.data
#         eq_(row, (compare,))
#         assert isinstance(row[0], type(compare))

#     def off_test_round_trip_decorated(self, connection):
#         date_table = self.tables.date_table

#         connection.execute(
#             date_table.insert(), {"id": 1, "decorated_date_data": self.data}
#         )

#         row = connection.execute(
#             select(date_table.c.decorated_date_data)
#         ).first()

#         compare = self.compare or self.data
#         eq_(row, (compare,))
#         assert isinstance(row[0], type(compare))

#     def test_null(self, connection):
#         date_table = self.tables.date_table

#         connection.execute(date_table.insert(), {"id": 1, "date_data": None})

#         row = connection.execute(select(date_table.c.date_data)).first()
#         eq_(row, (None,))

#     @testing.requires.datetime_literals
#     def test_literal(self, literal_round_trip):
#         compare = self.compare or self.data

#         literal_round_trip(
#             self.datatype, [self.data], [compare], compare=compare
#         )

#     @testing.requires.standalone_null_binds_whereclause
#     def test_null_bound_comparison(self):
#         # this test is based on an Oracle issue observed in #4886.
#         # passing NULL for an expression that needs to be interpreted as
#         # a certain type, does the DBAPI have the info it needs to do this.
#         date_table = self.tables.date_table
#         with config.db.begin() as conn:
#             result = conn.execute(
#                 date_table.insert(), {"id": 1, "date_data": self.data}
#             )
#             id_ = result.inserted_primary_key[0]
#             stmt = select(date_table.c.id).where(
#                 case(
#                     (
#                         bindparam("foo", type_=self.datatype) != None,
#                         bindparam("foo", type_=self.datatype),
#                     ),
#                     else_=date_table.c.date_data,
#                 )
#                 == date_table.c.date_data
#             )

#             row = conn.execute(stmt, {"foo": None}).first()
#             eq_(row[0], id_)


class DateTest(_DateTest):
    pass
    # __requires__ = ("date",)
    # __backend__ = True
    # datatype = Date
    # data = datetime.date(2012, 10, 15)



class BooleanTest(_BooleanTest):
    pass


class DateTimeTest(_DateTimeTest):
    pass

class IntegerTest(_IntegerTest):
    pass

class NumericTest(_NumericTest):
    pass

class StringTest(_StringTest):
    pass

class TableDDLTest(_TableDDLTest):
    pass

class JoinTest(_JoinTest):
    pass

