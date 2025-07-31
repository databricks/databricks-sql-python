from databricks.sql.backend.sea.utils.result_column import ResultColumn


class MetadataColumnMappings:
    """Column mappings for metadata queries following JDBC specification."""

    # Common columns used across multiple metadata queries
    CATALOG_COLUMN = ResultColumn("TABLE_CAT", "catalog", "string")
    CATALOG_COLUMN_FOR_TABLES = ResultColumn("TABLE_CAT", "catalogName", "string")
    SCHEMA_COLUMN = ResultColumn("TABLE_SCHEM", "namespace", "string")
    SCHEMA_COLUMN_FOR_GET_SCHEMA = ResultColumn("TABLE_SCHEM", "databaseName", "string")
    TABLE_NAME_COLUMN = ResultColumn("TABLE_NAME", "tableName", "string")
    TABLE_TYPE_COLUMN = ResultColumn("TABLE_TYPE", "tableType", "string")
    REMARKS_COLUMN = ResultColumn("REMARKS", "remarks", "string")

    # Columns specific to getColumns()
    COLUMN_NAME_COLUMN = ResultColumn("COLUMN_NAME", "col_name", "string")
    DATA_TYPE_COLUMN = ResultColumn(
        "DATA_TYPE", None, "int"
    )  # SEA doesn't provide this
    TYPE_NAME_COLUMN = ResultColumn("TYPE_NAME", "columnType", "string")
    COLUMN_SIZE_COLUMN = ResultColumn("COLUMN_SIZE", None, "int")
    DECIMAL_DIGITS_COLUMN = ResultColumn("DECIMAL_DIGITS", None, "int")
    NUM_PREC_RADIX_COLUMN = ResultColumn("NUM_PREC_RADIX", None, "int")
    NULLABLE_COLUMN = ResultColumn("NULLABLE", None, "int")
    COLUMN_DEF_COLUMN = ResultColumn(
        "COLUMN_DEF", "columnType", "string"
    )  # Note: duplicate mapping
    SQL_DATA_TYPE_COLUMN = ResultColumn("SQL_DATA_TYPE", None, "int")
    SQL_DATETIME_SUB_COLUMN = ResultColumn("SQL_DATETIME_SUB", None, "int")
    CHAR_OCTET_LENGTH_COLUMN = ResultColumn("CHAR_OCTET_LENGTH", None, "int")
    ORDINAL_POSITION_COLUMN = ResultColumn("ORDINAL_POSITION", None, "int")
    IS_NULLABLE_COLUMN = ResultColumn("IS_NULLABLE", "isNullable", "string")

    # Columns for getTables() that don't exist in SEA
    TYPE_CAT_COLUMN = ResultColumn("TYPE_CAT", None, "string")
    TYPE_SCHEM_COLUMN = ResultColumn("TYPE_SCHEM", None, "string")
    TYPE_NAME_COLUMN = ResultColumn("TYPE_NAME", None, "string")
    SELF_REFERENCING_COL_NAME_COLUMN = ResultColumn(
        "SELF_REFERENCING_COL_NAME", None, "string"
    )
    REF_GENERATION_COLUMN = ResultColumn("REF_GENERATION", None, "string")

    # Column lists for each metadata operation
    CATALOG_COLUMNS = [CATALOG_COLUMN]

    SCHEMA_COLUMNS = [
        SCHEMA_COLUMN_FOR_GET_SCHEMA,
        ResultColumn("TABLE_CATALOG", None, "string"),  # SEA doesn't return this
    ]

    TABLE_COLUMNS = [
        CATALOG_COLUMN_FOR_TABLES,
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
        CATALOG_COLUMN_FOR_TABLES,
        SCHEMA_COLUMN,
        TABLE_NAME_COLUMN,
        COLUMN_NAME_COLUMN,
        DATA_TYPE_COLUMN,
        TYPE_NAME_COLUMN,
        COLUMN_SIZE_COLUMN,
        ResultColumn("BUFFER_LENGTH", None, "int"),
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
        ResultColumn("SCOPE_CATALOG", None, "string"),
        ResultColumn("SCOPE_SCHEMA", None, "string"),
        ResultColumn("SCOPE_TABLE", None, "string"),
        ResultColumn("SOURCE_DATA_TYPE", None, "smallint"),
        ResultColumn("IS_AUTO_INCREMENT", None, "string"),
        ResultColumn("IS_GENERATEDCOLUMN", None, "string"),
    ]
