# type: ignore
from sqlalchemy.testing.suite import *
import pytest

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


@pytest.mark.skip(reason="pysql doesn't support binding of BINARY type parameters")
class BinaryTest(BinaryTest):
    pass


@pytest.mark.reviewed
class BooleanTest(BooleanTest):
    pass


@pytest.mark.reviewed
class NumericTest(NumericTest):

    @pytest.mark.skip(reason="Databricks doesn't support E notation for DECIMAL types")
    def test_enotation_decimal(self):
        pass

    @pytest.mark.skip(reason="Databricks doesn't support E notation for DECIMAL types")
    def test_enotation_decimal_large(self):
        pass



@pytest.mark.reviewed
class TimeMicrosecondsTest(TimeMicrosecondsTest):
    pass


@pytest.mark.reviewed
class TextTest(TextTest):
    pass


@pytest.mark.reviewed
class StringTest(StringTest):
    pass


@pytest.mark.reviewed
class DateTimeMicrosecondsTest(DateTimeMicrosecondsTest):
    pass


@pytest.mark.reviewed
class TimestampMicrosecondsTest(TimestampMicrosecondsTest):
    pass


@pytest.mark.reviewed
class DateTimeCoercedToDateTimeTest(DateTimeCoercedToDateTimeTest):
    pass


@pytest.mark.reviewed
class TimeTest(TimeTest):
    pass


@pytest.mark.reviewed
class DateTimeTest(DateTimeTest):
    pass


@pytest.mark.reviewed
class DateTimeHistoricTest(DateTimeHistoricTest):
    pass


@pytest.mark.reviewed
class DateTest(DateTest):
    pass


@pytest.mark.reviewed
class DateHistoricTest(DateHistoricTest):
    pass


class FetchLimitOffsetTest(FetchLimitOffsetTest):
    @pytest.mark.skip(
        reason="Dialect should advertise which offset rules Databricks supports. Offset handling needs work."
    )
    def test_bound_offset(self):
        """
        Exception:
            sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] The limit like expression "-1" is invalid. The limit expression must be equal to or greater than 0, but got -1.; line 3 pos 7
        """

    @pytest.mark.skip(
        reason="Dialect should advertise which offset rules Databricks supports. Offset handling needs work."
    )
    def test_limit_render_multiple_times(self):
        """
        Exception:
            AssertionError: [(5,)] != [(1,)]
        """

    @pytest.mark.skip(
        reason="Dialect should advertise which offset rules Databricks supports. Offset handling needs work."
    )
    def test_simple_offset(self):
        """
        Exception:
            sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] The limit like expression "-1" is invalid. The limit expression must be equal to or greater than 0, but got -1.; line 3 pos 7
        """

    @pytest.mark.skip(
        reason="Dialect should advertise which offset rules Databricks supports. Offset handling needs work."
    )
    def test_simple_offset_zero(self):
        """
        Exception:
            sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] The limit like expression "-1" is invalid. The limit expression must be equal to or greater than 0, but got -1.; line 3 pos 7
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_expr_offset(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] The limit like expression "-1" is invalid. The limit expression must be equal to or greater than 0, but got -1.; line 3 pos 7
        """


class FutureTableDDLTest(FutureTableDDLTest):
    @pytest.mark.skip(
        reason="Internal bug. DESCRIBE TABLE function should deliver an executable object."
    )
    def test_add_table_comment(self):
        """
        Exception:
            sqlalchemy.exc.ObjectNotExecutableError: Not an executable object: 'DESCRIBE TABLE main.pysql_sqlalchemy.test_table'
        """

    @pytest.mark.skip(
        reason="Internal bug. DESCRIBE TABLE function should deliver an executable object."
    )
    def test_create_table(self):
        """
        Exception:
            sqlalchemy.exc.ObjectNotExecutableError: Not an executable object: 'DESCRIBE TABLE main.pysql_sqlalchemy.test_table'
        """

    @pytest.mark.skip(
        reason="Internal bug. DESCRIBE TABLE function should deliver an executable object."
    )
    def test_drop_table(self):
        """
        Exception:
            sqlalchemy.exc.ObjectNotExecutableError: Not an executable object: 'DESCRIBE TABLE main.pysql_sqlalchemy.test_table'
        """

    @pytest.mark.skip(
        reason="Internal bug. DESCRIBE TABLE function should deliver an executable object."
    )
    def test_drop_table_comment(self):
        """
        Exception:
            sqlalchemy.exc.ObjectNotExecutableError: Not an executable object: 'DESCRIBE TABLE main.pysql_sqlalchemy.test_table'
        """

    @pytest.mark.skip(
        reason="Internal bug. DESCRIBE TABLE function should deliver an executable object."
    )
    def test_underscore_names(self):
        """
        Exception:
            sqlalchemy.exc.ObjectNotExecutableError: Not an executable object: 'DESCRIBE TABLE main.pysql_sqlalchemy._test_table'
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_create_table_schema(self):
        """
        Exception:
        - sqlalchemy.exc.ObjectNotExecutableError: Not an executable object: 'DESCRIBE TABLE main.test_schema.test_table'
        """


class IdentityAutoincrementTest(IdentityAutoincrementTest):
    @pytest.mark.skip(reason="Identity column handling needs work.")
    def test_autoincrement_with_identity(self):
        """
        Exception:
            sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Column id is not specified in INSERT
        """


class LongNameBlowoutTest(LongNameBlowoutTest):
    @pytest.mark.skip(
        reason="CreateIndex is not supported in Unity Catalog + parameters cannot exceed 255 characters in length"
    )
    def test_long_convention_name(self):
        """
        This test is parameterized. It receives the following failures from Databricks compute
        Exception:
            [fk-_exclusions0] sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [RequestId=9e4262cc-05bc-4086-b17d-0c8082599218 ErrorClass=INVALID_PARAMETER_VALUE.INVALID_FIELD_LENGTH] CreateTable foreign_key.name too long. Maximum length is 255 characters.
            [ix-_exclusions2] sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [UC_COMMAND_NOT_SUPPORTED.WITHOUT_RECOMMENDATION] The command(s): CreateIndex are not supported in Unity Catalog.
            [pk-_exclusions1] sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [RequestId=f3e6940b-bd69-455d-9314-87522bcf8cef ErrorClass=INVALID_PARAMETER_VALUE.INVALID_FIELD_LENGTH] CreateTable primary_key.name too long. Maximum length is 255 characters.
        """


class RowFetchTest(RowFetchTest):
    @pytest.mark.skip(
        reason="Date type implementation needs work. Timezone information not preserved."
    )
    def test_row_w_scalar_select(self):
        """
        Exception:
            AssertionError: datetime.datetime(2006, 5, 12, 12, 0, tzinfo=<StaticTzInfo 'Etc/UTC'>) != datetime.datetime(2006, 5, 12, 12, 0)
        """


class ExceptionTest(ExceptionTest):
    @pytest.mark.skip(reason="Databricks may not support this method.")
    def test_integrity_error(self):
        """
        Exception:
            databricks.sql.exc.ServerOperationError: Column id is not specified in INSERT
        """


class HasTableTest(HasTableTest):
    @pytest.mark.skip(reason="Schema is not properly configured for this test.")
    def test_has_table(self):
        """
                Exception

                    databricks.sql.exc.ServerOperationError: [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
        If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
        To tolerate the error on drop use DROP SCHEMA IF EXISTS.

        """

    @pytest.mark.skip(reason="Schema is not properly configured for this test.")
    def test_has_table_schema(self):
        """
                Exception
                    databricks.sql.exc.ServerOperationError: [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
        If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
        To tolerate the error on drop use DROP SCHEMA IF EXISTS.

        """

    @pytest.mark.skip(reason="Schema is not properly configured for this test.")
    def test_has_table_temp_table(self):
        """
                Exception
                    databricks.sql.exc.ServerOperationError: [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
        If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
        To tolerate the error on drop use DROP SCHEMA IF EXISTS.

        """

    @pytest.mark.skip(reason="Schema is not properly configured for this test.")
    def test_has_table_temp_view(self):
        """
                Exception
                    databricks.sql.exc.ServerOperationError: [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
        If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
        To tolerate the error on drop use DROP SCHEMA IF EXISTS.

        """

    @pytest.mark.skip(reason="Schema is not properly configured for this test.")
    def test_has_table_view(self):
        """
                Exception
                    databricks.sql.exc.ServerOperationError: [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
        If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
        To tolerate the error on drop use DROP SCHEMA IF EXISTS.

        """

    @pytest.mark.skip(reason="Schema is not properly configured for this test.")
    def test_has_table_view_schema(self):
        """
                Exception
                databricks.sql.exc.ServerOperationError: [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
        If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
        To tolerate the error on drop use DROP SCHEMA IF EXISTS.

        """


class LastrowidTest(LastrowidTest):
    @pytest.mark.skip(reason="DDL for INSERT requires adjustment")
    def test_autoincrement_on_insert(self):
        """
        Exception
            databricks.sql.exc.ServerOperationError: Column id is not specified in INSERT

        """

    @pytest.mark.skip(reason="DDL for INSERT requires adjustment")
    def test_last_inserted_id(self):
        """
        Exception:
            databricks.sql.exc.ServerOperationError: Column id is not specified in INSERT

        """


class CompositeKeyReflectionTest(CompositeKeyReflectionTest):
    @pytest.mark.skip(reason="Primary key handling needs work.")
    def test_pk_column_order(self):
        """
        Exception:
        AssertionError: [] != ['name', 'id', 'attr']
            assert [] == ['name', 'id', 'attr']
            Right contains 3 more items, first extra item: 'name'
            Full diff:
            - ['name', 'id', 'attr']
            + []
        """

    @pytest.mark.skip(
        reason="Composite key implementation needs. Work may not be supported by Databricks."
    )
    def test_fk_column_order(self):
        """
        Excpetion:
            AssertionError: 0 != 1
            assert 0 == 1
        """


class ComponentReflectionTestExtra(ComponentReflectionTestExtra):
    @pytest.mark.skip(reason="Test setup needs adjustment.")
    def test_varchar_reflection(self):
        """
        Exception:
            databricks.sql.exc.ServerOperationError: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `pysql_sqlalchemy`.`t` because it already exists.
            Choose a different name, drop or replace the existing object, add the IF NOT EXISTS clause to tolerate pre-existing objects, or add the OR REFRESH clause to refresh the existing streaming table.
        """

    @pytest.mark.skip(reason="Test setup appears broken")
    def test_numeric_reflection(self):
        """
        Exception:
            databricks.sql.exc.ServerOperationError: [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
            If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
            To tolerate the error on drop use DROP SCHEMA IF EXISTS.
        """


class DifficultParametersTest(DifficultParametersTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_round_trip_same_named_column(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Found invalid character(s) among ' ,;{}()\n\t=' in the column names of your schema.
        """


class InsertBehaviorTest(InsertBehaviorTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_autoclose_on_insert(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Column id is not specified in INSERT
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_empty_insert(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_insert_from_select_autoinc(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Column id is not specified in INSERT
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_insert_from_select_autoinc_no_rows(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) Column id is not specified in INSERT
        """

    @pytest.mark.skip(reason="Databricks doesn't support empty INSERT.")
    def test_empty_insert_multiple(self):
        """
        Exception:
            sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)

            E               sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
            E               [PARSE_SYNTAX_ERROR] Syntax error at or near ')'.(line 1, pos 24)
            E
            E               == SQL ==
            E               INSERT INTO autoinc_pk () VALUES ()
            E               ------------------------^^^
            E
            E               [SQL: INSERT INTO autoinc_pk () VALUES ()]
            E               [parameters: ({}, {}, {})]
            E               (Background on this error at: https://sqlalche.me/e/14/4xp6)
        """


class TableDDLTest(TableDDLTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_create_table(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `pysql_sqlalchemy`.`test_table` because it already exists.
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_create_table_schema(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [SCHEMA_NOT_FOUND] The schema `main.test_schema` cannot be found. Verify the spelling and correctness of the schema and catalog.
        """

    @pytest.mark.skip(
        reason="DDL handling needs work. Some features not implemented in dialect."
    )
    def test_add_table_comment(self):
        """
        Exception:
            NotImplementedError
        """

    @pytest.mark.skip(
        reason="DDL handling needs work. Some features not implemented in dialect."
    )
    def test_drop_table_comment(self):
        """
        Exception:
            NotImplementedError
        """

    @pytest.mark.skip(
        reason="DDL handling needs work. Some features not implemented in dialect."
    )
    def test_underscore_names(self):
        """
        This exception may require this test to simply be rewritten as it appears to be a race condition.

        Exception:
            sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `pysql_sqlalchemy`.`_test_table` because it already exists.
        """


class ComponentReflectionTest(ComponentReflectionTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_autoincrement_col(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_dialect_initialize(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_columns(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_comments(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_comments_with_schema(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_default_schema_name(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_foreign_keys(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_foreign_keys(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_indexes(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_inter_schema_foreign_keys(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_noncol_index(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_pk_constraint(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_schema_names(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_schema_names_w_translate_map(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_table_names(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_table_oid(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_table_oid(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_temp_table_columns(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_temp_table_indexes(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_temp_table_names(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_temp_table_unique_constraints(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_temp_view_columns(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_temp_view_names(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_unique_constraints(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_view_definition(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_view_definition(self):
        """
        Exception:
        - NotImplementedError: no temp table keyword args routine for cfg: databricks+databricks://token:***redacted***@e2-dogfood.staging.cloud.databricks.com?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2F5c89f447c476a5a8&schema=pysql_sqlalchemy
        """


class HasIndexTest(HasIndexTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_has_index_schema(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError) [UC_COMMAND_NOT_SUPPORTED.WITHOUT_RECOMMENDATION] The command(s): CreateIndex are not supported in Unity Catalog.
        """

    @pytest.mark.skip(reason="Dialect doesn't know how to handle indexes.")
    def test_has_index(self):
        """
        Exception:
            AssertionError: assert False
        """


class QuotedNameArgumentTest(QuotedNameArgumentTest):
    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_check_constraints(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_columns(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_foreign_keys(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_indexes(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_pk_constraint(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_table_comment(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_table_options(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_view_definition(self):
        """
        Exception:
        - sqlalchemy.exc.DatabaseError: (databricks.sql.exc.ServerOperationError)
        """

    @pytest.mark.skip(reason="Error during execution. Requires investigation.")
    def test_get_unique_constraints(self):
        pass
