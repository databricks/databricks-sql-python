# type: ignore
from sqlalchemy.testing.suite import *
from databricks.sqlalchemy.test._regression import *
from databricks.sqlalchemy.test._future import *
from databricks.sqlalchemy.test._unsupported import *

# Test definitions are found here:
# https://github.com/sqlalchemy/sqlalchemy/tree/main/lib/sqlalchemy/testing/suite


# Per the instructions for dialect authors, tests should be skippable based on
# dialect limitations defined in requirements.py. However, we found that these
# definitions are not universally honoured by the SQLAlchemy test runner so we
# opt to manually delete them from the test suite for the time-being. This makes
# it obvious what areas of dialect compliance we have not evaluated. The next
# step towards dialect compliance is to review each of these and document exactly
# which methods should or should not work. This can be done by removing the corr-
# esponding skip marker and then running the test.

# If we find tests that are skippable for a documented reason, we can call these
# out directly in the way suggested by SQLAlchemy's document for dialect authors:
#
#   > In the case that the decorators are not covering a particular test, a test
#   > can also be directly modified or bypassed.
#
# See further: https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4_48/README.dialects.rst




























