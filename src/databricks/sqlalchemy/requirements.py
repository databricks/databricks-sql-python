# Following official SQLAlchemy guide:
#
# https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst#dialect-layout
#
# The full group of requirements is available here:
#
# https://github.com/sqlalchemy/sqlalchemy/blob/a453256afc334acabee25ec275de555ef7287144/test/requirements.py


from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions

class Requirements(SuiteRequirements):
    
    @property
    def two_phase_transactions(self):
        # Databricks SQL doesn't support transactions
        return exclusions.closed()

    @property
    def table_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for tables."""

        return exclusions.open()

    @property
    def foreign_keys(self):
        # Databricks SQL doesn't support foreign keys
        return exclusions.closed()

    @property
    def self_referential_foreign_keys(self):

        return exclusions.closed()

    @property
    def foreign_key_ddl(self):
        
        return exclusions.closed()
