from sqlalchemy.testing.suite import *

from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import StringTest as _StringTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import BooleanTest as _BooleanTest
from sqlalchemy.testing.suite import DateTest as _DateTest
from sqlalchemy.testing.suite import DateTimeTest as _DateTimeTest


from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest

from sqlalchemy.testing.suite import JoinTest as _JoinTest



class BooleanTest(_BooleanTest):
    pass

class DateTest(_DateTest):
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

