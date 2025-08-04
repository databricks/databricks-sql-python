from databricks.sql.backend.sea.utils.result_column import ResultColumn
from databricks.sql.backend.sea.utils.conversion import SqlType


class MetadataColumnMappings:
    """Column mappings for metadata queries following JDBC specification."""

    CATALOG_COLUMN_FOR_GET_CATALOGS = ResultColumn(
        "TABLE_CAT", "catalog", SqlType.VARCHAR
    )

    CATALOG_FULL_COLUMN = ResultColumn("TABLE_CATALOG", "catalogName", SqlType.VARCHAR)
    SCHEMA_COLUMN_FOR_GET_SCHEMA = ResultColumn(
        "TABLE_SCHEM", "databaseName", SqlType.VARCHAR
    )

    CATALOG_COLUMN = ResultColumn("TABLE_CAT", "catalogName", SqlType.VARCHAR)

    SCHEMA_COLUMN = ResultColumn("TABLE_SCHEM", "namespace", SqlType.VARCHAR)

    TABLE_NAME_COLUMN = ResultColumn("TABLE_NAME", "tableName", SqlType.VARCHAR)
    TABLE_TYPE_COLUMN = ResultColumn("TABLE_TYPE", "tableType", SqlType.VARCHAR)
    REMARKS_COLUMN = ResultColumn("REMARKS", "remarks", SqlType.VARCHAR)

    # Columns specific to getColumns()
    COLUMN_NAME_COLUMN = ResultColumn("COLUMN_NAME", "col_name", SqlType.VARCHAR)
    DATA_TYPE_COLUMN = ResultColumn("DATA_TYPE", None, SqlType.INT)
    TYPE_NAME_COLUMN = ResultColumn("TYPE_NAME", "columnType", SqlType.VARCHAR)

    COLUMN_SIZE_COLUMN = ResultColumn("COLUMN_SIZE", "columnSize", SqlType.INT)
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
    )

    NULLABLE_COLUMN = ResultColumn("NULLABLE", None, SqlType.INT)
    COLUMN_DEF_COLUMN = ResultColumn("COLUMN_DEF", "columnType", SqlType.VARCHAR)
    SQL_DATA_TYPE_COLUMN = ResultColumn("SQL_DATA_TYPE", None, SqlType.INT)
    SQL_DATETIME_SUB_COLUMN = ResultColumn("SQL_DATETIME_SUB", None, SqlType.INT)
    CHAR_OCTET_LENGTH_COLUMN = ResultColumn("CHAR_OCTET_LENGTH", None, SqlType.INT)
    IS_NULLABLE_COLUMN = ResultColumn("IS_NULLABLE", "isNullable", SqlType.VARCHAR)

    # Columns for getTables() that don't exist in SEA
    TYPE_CAT_COLUMN = ResultColumn("TYPE_CAT", None, SqlType.VARCHAR)
    TYPE_SCHEM_COLUMN = ResultColumn("TYPE_SCHEM", None, SqlType.VARCHAR)
    TYPE_NAME_COLUMN = ResultColumn("TYPE_NAME", None, SqlType.VARCHAR)
    SELF_REFERENCING_COL_NAME_COLUMN = ResultColumn(
        "SELF_REFERENCING_COL_NAME", None, SqlType.VARCHAR
    )
    REF_GENERATION_COLUMN = ResultColumn("REF_GENERATION", None, SqlType.VARCHAR)

    SCOPE_CATALOG_COLUMN = ResultColumn("SCOPE_CATALOG", None, SqlType.VARCHAR)
    SCOPE_SCHEMA_COLUMN = ResultColumn("SCOPE_SCHEMA", None, SqlType.VARCHAR)
    SCOPE_TABLE_COLUMN = ResultColumn("SCOPE_TABLE", None, SqlType.VARCHAR)
    SOURCE_DATA_TYPE_COLUMN = ResultColumn("SOURCE_DATA_TYPE", None, SqlType.INT)

    IS_AUTO_INCREMENT_COLUMN = ResultColumn(
        "IS_AUTOINCREMENT", "isAutoIncrement", SqlType.VARCHAR
    )
    IS_GENERATED_COLUMN = ResultColumn(
        "IS_GENERATEDCOLUMN", "isGenerated", SqlType.VARCHAR
    )

    BUFFER_LENGTH_COLUMN = ResultColumn("BUFFER_LENGTH", None, SqlType.INT)

    # Column lists for each metadata operation
    CATALOG_COLUMNS = [CATALOG_COLUMN_FOR_GET_CATALOGS]

    SCHEMA_COLUMNS = [
        SCHEMA_COLUMN_FOR_GET_SCHEMA,
        CATALOG_FULL_COLUMN,
    ]

    TABLE_COLUMNS = [
        CATALOG_COLUMN,  # Use general catalog column (catalogName)
        SCHEMA_COLUMN,
        TABLE_NAME_COLUMN,
        TABLE_TYPE_COLUMN,
        REMARKS_COLUMN,
        TYPE_CAT_COLUMN,
        TYPE_SCHEM_COLUMN,
        TYPE_NAME_COLUMN,
        SELF_REFERENCING_COL_NAME_COLUMN,
        REF_GENERATION_COLUMN,
    ]

    COLUMN_COLUMNS = [
        CATALOG_COLUMN,  # Use general catalog column (catalogName)
        SCHEMA_COLUMN,
        TABLE_NAME_COLUMN,
        COLUMN_NAME_COLUMN,
        DATA_TYPE_COLUMN,
        TYPE_NAME_COLUMN,
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
        # DO NOT INCLUDE IS_GENERATED_COLUMN - Thrift returns 23 columns
    ]
