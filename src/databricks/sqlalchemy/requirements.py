"""
This module is supposedly used by the compliance tests to control which tests are run based on database capabilities.
However, based on some experimentation that does not appear to be consistently the case. Until we better understand
when these requirements are and are not implemented, we prefer to manually capture the exact nature of the failures
and errors.

Once we better understand how to use requirements.py, an example exclusion will look like this:

    import sqlalchemy.testing.requirements
    import sqlalchemy.testing.exclusions

    class Requirements(sqlalchemy.testing.requirements.SuiteRequirements):
        @property
        def __some_example_requirement(self):
            return sqlalchemy.testing.exclusions.closed


The complete list of requirements is provided by SQLAlchemy here:

https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/testing/requirements.py
"""

import sqlalchemy.testing.requirements
import sqlalchemy.testing.exclusions

import logging

logger = logging.getLogger(__name__)

logger.warning("requirements.py is not currently employed by Databricks dialect")


class Requirements(sqlalchemy.testing.requirements.SuiteRequirements):
    
    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return sqlalchemy.testing.exclusions.open()
    
    @property
    def datetime_literals(self):
        """target dialect supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """

        return sqlalchemy.testing.exclusions.open()


