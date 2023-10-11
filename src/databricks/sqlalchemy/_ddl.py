import re
from sqlalchemy.sql import compiler
import logging

logger = logging.getLogger(__name__)


class DatabricksIdentifierPreparer(compiler.IdentifierPreparer):
    """https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
    """

    legal_characters = re.compile(r"^[A-Z0-9_]+$", re.I)

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="`")


class DatabricksDDLCompiler(compiler.DDLCompiler):
    def post_create_table(self, table):
        return " USING DELTA"
    
    def visit_unique_constraint(self, constraint, **kw):
        logger.warn("Databricks does not support unique constraints")
        pass

    def visit_check_constraint(self, constraint, **kw):
        logger.warn("Databricks does not support check constraints")
        pass