from databricks.sql.backend.sea.utils.result_column import ResultColumn
from databricks.sql.backend.sea.utils.conversion import SqlType
from databricks.sql.backend.sea.utils.metadata_transforms import (
    transform_remarks,
    transform_is_autoincrement,
    transform_is_nullable,
    transform_nullable,
    transform_data_type,
    transform_ordinal_position,
)


class MetadataColumnMappings:
    """Column mappings for metadata queries following JDBC specification."""

    CATALOG_COLUMN_FOR_GET_CATALOGS = ResultColumn(
        "TABLE_CAT", "catalog", SqlType.STRING
    )

    CATALOG_FULL_COLUMN = ResultColumn("TABLE_CATALOG", None, SqlType.STRING)
    SCHEMA_COLUMN_FOR_GET_SCHEMA = ResultColumn(
        "TABLE_SCHEM", "databaseName", SqlType.STRING
    )

    CATALOG_COLUMN = ResultColumn("TABLE_CAT", "catalogName", SqlType.STRING)
    SCHEMA_COLUMN = ResultColumn("TABLE_SCHEM", "namespace", SqlType.STRING)
    TABLE_NAME_COLUMN = ResultColumn("TABLE_NAME", "tableName", SqlType.STRING)
    TABLE_TYPE_COLUMN = ResultColumn("TABLE_TYPE", "tableType", SqlType.STRING)
    REMARKS_COLUMN = ResultColumn("REMARKS", "remarks", SqlType.STRING, transform_remarks)
    TYPE_CATALOG_COLUMN = ResultColumn("TYPE_CAT", None, SqlType.STRING)
    TYPE_SCHEM_COLUMN = ResultColumn("TYPE_SCHEM", None, SqlType.STRING)
    TYPE_NAME_COLUMN = ResultColumn("TYPE_NAME", None, SqlType.STRING)
    SELF_REFERENCING_COL_NAME_COLUMN = ResultColumn(
        "SELF_REFERENCING_COL_NAME", None, SqlType.STRING
    )
    REF_GENERATION_COLUMN = ResultColumn("REF_GENERATION", None, SqlType.STRING)

    COL_NAME_COLUMN = ResultColumn("COLUMN_NAME", "col_name", SqlType.STRING)
    DATA_TYPE_COLUMN = ResultColumn("DATA_TYPE", None, SqlType.INT, transform_data_type)
    COLUMN_TYPE_COLUMN = ResultColumn("TYPE_NAME", "columnType", SqlType.STRING)
    COLUMN_SIZE_COLUMN = ResultColumn("COLUMN_SIZE", "columnSize", SqlType.INT)
    BUFFER_LENGTH_COLUMN = ResultColumn("BUFFER_LENGTH", None, SqlType.TINYINT)

    DECIMAL_DIGITS_COLUMN = ResultColumn(
        "DECIMAL_DIGITS",
        "decimalDigits",
        SqlType.INT,
    )
    NUM_PREC_RADIX_COLUMN = ResultColumn("NUM_PREC_RADIX", "radix", SqlType.INT)
    ORDINAL_POSITION_COLUMN = ResultColumn(
        "ORDINAL_POSITION",
        "ordinalPosition",
        SqlType.INT,
        transform_ordinal_position,
    )

    NULLABLE_COLUMN = ResultColumn("NULLABLE", "isNullable", SqlType.INT, transform_nullable)
    COLUMN_DEF_COLUMN = ResultColumn("COLUMN_DEF", None, SqlType.STRING)
    SQL_DATA_TYPE_COLUMN = ResultColumn("SQL_DATA_TYPE", None, SqlType.INT)
    SQL_DATETIME_SUB_COLUMN = ResultColumn("SQL_DATETIME_SUB", None, SqlType.INT)
    CHAR_OCTET_LENGTH_COLUMN = ResultColumn("CHAR_OCTET_LENGTH", None, SqlType.INT)
    IS_NULLABLE_COLUMN = ResultColumn(
        "IS_NULLABLE", "isNullable", SqlType.STRING, transform_is_nullable
    )

    SCOPE_CATALOG_COLUMN = ResultColumn("SCOPE_CATALOG", None, SqlType.STRING)
    SCOPE_SCHEMA_COLUMN = ResultColumn("SCOPE_SCHEMA", None, SqlType.STRING)
    SCOPE_TABLE_COLUMN = ResultColumn("SCOPE_TABLE", None, SqlType.STRING)
    SOURCE_DATA_TYPE_COLUMN = ResultColumn("SOURCE_DATA_TYPE", None, SqlType.SMALLINT)

    IS_AUTO_INCREMENT_COLUMN = ResultColumn(
        "IS_AUTO_INCREMENT",
        "isAutoIncrement",
        SqlType.STRING,
        transform_is_autoincrement,
    )
    IS_GENERATED_COLUMN = ResultColumn(
        "IS_GENERATEDCOLUMN", "isGenerated", SqlType.STRING
    )

    CATALOG_COLUMNS = [CATALOG_COLUMN_FOR_GET_CATALOGS]

    SCHEMA_COLUMNS = [
        SCHEMA_COLUMN_FOR_GET_SCHEMA,
        CATALOG_FULL_COLUMN,
    ]

    TABLE_COLUMNS = [
        CATALOG_COLUMN,
        SCHEMA_COLUMN,
        TABLE_NAME_COLUMN,
        TABLE_TYPE_COLUMN,
        REMARKS_COLUMN,
        TYPE_CATALOG_COLUMN,
        TYPE_SCHEM_COLUMN,
        TYPE_NAME_COLUMN,
        SELF_REFERENCING_COL_NAME_COLUMN,
        REF_GENERATION_COLUMN,
    ]

    COLUMN_COLUMNS = [
        CATALOG_COLUMN,
        SCHEMA_COLUMN,
        TABLE_NAME_COLUMN,
        COL_NAME_COLUMN,
        DATA_TYPE_COLUMN,
        COLUMN_TYPE_COLUMN,
        COLUMN_SIZE_COLUMN,
        BUFFER_LENGTH_COLUMN,
        DECIMAL_DIGITS_COLUMN,
        NUM_PREC_RADIX_COLUMN,
        NULLABLE_COLUMN,
        REMARKS_COLUMN,
        COLUMN_DEF_COLUMN,
        SQL_DATA_TYPE_COLUMN,
        SQL_DATETIME_SUB_COLUMN,
        CHAR_OCTET_LENGTH_COLUMN,
        ORDINAL_POSITION_COLUMN,
        IS_NULLABLE_COLUMN,
        SCOPE_CATALOG_COLUMN,
        SCOPE_SCHEMA_COLUMN,
        SCOPE_TABLE_COLUMN,
        SOURCE_DATA_TYPE_COLUMN,
        IS_AUTO_INCREMENT_COLUMN,
        # not including IS_GENERATED_COLUMN of SEA because Thrift does not return an equivalent
    ]
