from databricks.sql.backend.sea.utils.result_column import ResultColumn


class MetadataColumnMappings:
    """Column mappings for metadata queries following JDBC specification."""

    # Common columns used across multiple metadata queries
    # FIX 1: Catalog columns - swap the mappings
    CATALOG_COLUMN = ResultColumn("TABLE_CAT", "catalogName", "string")
    CATALOG_COLUMN_FOR_GET_CATALOGS = ResultColumn("TABLE_CAT", "catalog", "string")
    # Remove CATALOG_COLUMN_FOR_TABLES - will use CATALOG_COLUMN instead

    SCHEMA_COLUMN = ResultColumn("TABLE_SCHEM", "namespace", "string")
    SCHEMA_COLUMN_FOR_GET_SCHEMA = ResultColumn("TABLE_SCHEM", "databaseName", "string")
    TABLE_NAME_COLUMN = ResultColumn("TABLE_NAME", "tableName", "string")
    TABLE_TYPE_COLUMN = ResultColumn("TABLE_TYPE", "tableType", "string")
    REMARKS_COLUMN = ResultColumn("REMARKS", "remarks", "string")

    # Columns specific to getColumns()
    COLUMN_NAME_COLUMN = ResultColumn("COLUMN_NAME", "col_name", "string")
    DATA_TYPE_COLUMN = ResultColumn(
        "DATA_TYPE", None, "int"
    )  # Calculated from columnType
    TYPE_NAME_COLUMN = ResultColumn("TYPE_NAME", "columnType", "string")

    # FIX 5: SEA actually provides these columns
    COLUMN_SIZE_COLUMN = ResultColumn("COLUMN_SIZE", "columnSize", "int")
    DECIMAL_DIGITS_COLUMN = ResultColumn(
        "DECIMAL_DIGITS",
        "decimalDigits",
        "int",
    )
    NUM_PREC_RADIX_COLUMN = ResultColumn("NUM_PREC_RADIX", "radix", "int")
    ORDINAL_POSITION_COLUMN = ResultColumn(
        "ORDINAL_POSITION",
        "ordinalPosition",
        "int",
    )

    NULLABLE_COLUMN = ResultColumn(
        "NULLABLE", None, "int"
    )  # Calculated from isNullable
    COLUMN_DEF_COLUMN = ResultColumn(
        "COLUMN_DEF", "columnType", "string"
    )  # Note: duplicate mapping
    SQL_DATA_TYPE_COLUMN = ResultColumn("SQL_DATA_TYPE", None, "int")
    SQL_DATETIME_SUB_COLUMN = ResultColumn("SQL_DATETIME_SUB", None, "int")
    CHAR_OCTET_LENGTH_COLUMN = ResultColumn("CHAR_OCTET_LENGTH", None, "int")
    IS_NULLABLE_COLUMN = ResultColumn("IS_NULLABLE", "isNullable", "string")

    # Columns for getTables() that don't exist in SEA
    TYPE_CAT_COLUMN = ResultColumn("TYPE_CAT", None, "string")
    TYPE_SCHEM_COLUMN = ResultColumn("TYPE_SCHEM", None, "string")
    TYPE_NAME_COLUMN = ResultColumn("TYPE_NAME", None, "string")
    SELF_REFERENCING_COL_NAME_COLUMN = ResultColumn(
        "SELF_REFERENCING_COL_NAME", None, "string"
    )
    REF_GENERATION_COLUMN = ResultColumn("REF_GENERATION", None, "string")

    # FIX 8: Scope columns (always null per JDBC)
    SCOPE_CATALOG_COLUMN = ResultColumn("SCOPE_CATALOG", None, "string")
    SCOPE_SCHEMA_COLUMN = ResultColumn("SCOPE_SCHEMA", None, "string")
    SCOPE_TABLE_COLUMN = ResultColumn("SCOPE_TABLE", None, "string")
    SOURCE_DATA_TYPE_COLUMN = ResultColumn("SOURCE_DATA_TYPE", None, "smallint")

    # FIX 9 & 10: Auto increment and generated columns
    IS_AUTO_INCREMENT_COLUMN = ResultColumn(
        "IS_AUTOINCREMENT", "isAutoIncrement", "string"
    )  # No underscore!
    IS_GENERATED_COLUMN = ResultColumn(
        "IS_GENERATEDCOLUMN", "isGenerated", "string"
    )  # SEA provides this

    # FIX 11: Buffer length column
    BUFFER_LENGTH_COLUMN = ResultColumn(
        "BUFFER_LENGTH", None, "int"
    )  # Always null per JDBC

    # Column lists for each metadata operation
    CATALOG_COLUMNS = [CATALOG_COLUMN_FOR_GET_CATALOGS]  # Use specific catalog column

    SCHEMA_COLUMNS = [
        SCHEMA_COLUMN_FOR_GET_SCHEMA,
        ResultColumn(
            "TABLE_CATALOG", None, "string"
        ),  # Will need special population logic
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

    # FIX 13: Remove IS_GENERATEDCOLUMN from list (should be 23 columns, not 24)
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
