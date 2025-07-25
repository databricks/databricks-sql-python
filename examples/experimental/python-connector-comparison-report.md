# Python Connector Comparison Report

**Date:** 2025-07-24 06:52:12
**Server:** benchmarking-staging-aws-us-west-2.staging.cloud.databricks.com
**HTTP Path:** /sql/1.0/warehouses/17661fca65a0e4fc

## Summary

- **Total Tests:** 27
- **Passed:** 4
- **Failed:** 23

## Performance Summary

- **Total Thrift Execution Time:** 9.5053s
- **Total SEA Execution Time:** 12.2712s
- **SEA Performance:** +29.10% slower than Thrift

## Test Results

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 5
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 5
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 5
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 5
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 5
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** Cursor Properties
**Query/Method:** No query executed
============================

**Result:** PASSED
**Execution Time:** Thrift: 0.0000s, SEA: 0.0000s

============================

**Query Type:** Cursor Properties
**Query/Method:** SHOW TABLES IN main.default
============================

**Result:** PASSED
**Execution Time:** Thrift: 0.0000s, SEA: 0.0000s

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** catalogs()
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 0 (TABLE_CAT) name mismatch: TABLE_CAT vs catalog
  - Field 'catalog' missing in all Thrift rows

**Data Differences:**
-----------------
Row Data:
  - Field 'TABLE_CAT' missing in all SEA rows

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** schemas(None, None)
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Exception: Catalog name is required for get_schemas

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** schemas(main, None)
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Description length mismatch: Thrift has 2 columns, SEA has 1: [('TABLE_SCHEM', 'string', None, None, None, None, None), ('TABLE_CATALOG', 'string', None, None, None, None, None)] vs [('databaseName', 'string', None, None, None, None, None)]
  - Field 'TABLE_CATALOG' missing in all SEA rows

**Data Differences:**
-----------------
Row Data:
  - Field 'databaseName' missing in all Thrift rows
  - Field 'TABLE_SCHEM' missing in all SEA rows

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** schemas(main, def%)
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Description length mismatch: Thrift has 2 columns, SEA has 1: [('TABLE_SCHEM', 'string', None, None, None, None, None), ('TABLE_CATALOG', 'string', None, None, None, None, None)] vs [('databaseName', 'string', None, None, None, None, None)]

**Data Differences:**
-----------------
Row Data:
  - Row count mismatch: Thrift returned 1, SEA returned 0

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** tables(None, None, None, None)
============================

**Other Differences:**
-----------------
  - Exception: Command failed: BAD_REQUEST - Inline byte limit exceeded. Statements executed with disposition=INLINE can have a result size of at most 26214400 bytes. Please execute the statement with disposition=EXTERNAL_LINKS if you want to download the full result.

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** tables(main, None, None, None)
============================

**Other Differences:**
-----------------
  - Exception: Command failed: BAD_REQUEST - Inline byte limit exceeded. Statements executed with disposition=INLINE can have a result size of at most 26214400 bytes. Please execute the statement with disposition=EXTERNAL_LINKS if you want to download the full result.

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** tables(main, default, None, None)
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Description length mismatch: Thrift has 10 columns, SEA has 7: [('TABLE_CAT', 'string', None, None, None, None, None), ('TABLE_SCHEM', 'string', None, None, None, None, None), ('TABLE_NAME', 'string', None, None, None, None, None), ('TABLE_TYPE', 'string', None, None, None, None, None), ('REMARKS', 'string', None, None, None, None, None), ('TYPE_CAT', 'string', None, None, None, None, None), ('TYPE_SCHEM', 'string', None, None, None, None, None), ('TYPE_NAME', 'string', None, None, None, None, None), ('SELF_REFERENCING_COL_NAME', 'string', None, None, None, None, None), ('REF_GENERATION', 'string', None, None, None, None, None)] vs [('namespace', 'string', None, None, None, None, None), ('tableName', 'string', None, None, None, None, None), ('isTemporary', 'boolean', None, None, None, None, None), ('information', 'string', None, None, None, None, None), ('catalogName', 'string', None, None, None, None, None), ('tableType', 'string', None, None, None, None, None), ('remarks', 'string', None, None, None, None, None)]
  - Field 'tableType' missing in all Thrift rows
  - Field 'catalogName' missing in all Thrift rows
  - Field 'TYPE_CAT' missing in all SEA rows
  - Field 'TYPE_NAME' missing in all SEA rows
  - Field 'TABLE_TYPE' missing in all SEA rows
  - Field 'TYPE_SCHEM' missing in all SEA rows

**Data Differences:**
-----------------
Row Data:
  - Field 'tableName' missing in all Thrift rows
  - Field 'remarks' missing in all Thrift rows
  - Field 'namespace' missing in all Thrift rows
  - Field 'isTemporary' missing in all Thrift rows
  - Field 'information' missing in all Thrift rows
  - Field 'SELF_REFERENCING_COL_NAME' missing in all SEA rows
  - Field 'TABLE_SCHEM' missing in all SEA rows
  - Field 'TABLE_CAT' missing in all SEA rows
  - Field 'REMARKS' missing in all SEA rows
  - Field 'TABLE_NAME' missing in all SEA rows
  - Field 'REF_GENERATION' missing in all SEA rows

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** tables(main, default, %sales, None)
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Description length mismatch: Thrift has 10 columns, SEA has 7: [('TABLE_CAT', 'string', None, None, None, None, None), ('TABLE_SCHEM', 'string', None, None, None, None, None), ('TABLE_NAME', 'string', None, None, None, None, None), ('TABLE_TYPE', 'string', None, None, None, None, None), ('REMARKS', 'string', None, None, None, None, None), ('TYPE_CAT', 'string', None, None, None, None, None), ('TYPE_SCHEM', 'string', None, None, None, None, None), ('TYPE_NAME', 'string', None, None, None, None, None), ('SELF_REFERENCING_COL_NAME', 'string', None, None, None, None, None), ('REF_GENERATION', 'string', None, None, None, None, None)] vs [('namespace', 'string', None, None, None, None, None), ('tableName', 'string', None, None, None, None, None), ('isTemporary', 'boolean', None, None, None, None, None), ('information', 'string', None, None, None, None, None), ('catalogName', 'string', None, None, None, None, None), ('tableType', 'string', None, None, None, None, None), ('remarks', 'string', None, None, None, None, None)]

**Data Differences:**
-----------------
Row Data:
  - Row count mismatch: Thrift returned 1, SEA returned 0

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** tables(main, default, None, ['TABLE', 'VIEW'])
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Description length mismatch: Thrift has 10 columns, SEA has 7: [('TABLE_CAT', 'string', None, None, None, None, None), ('TABLE_SCHEM', 'string', None, None, None, None, None), ('TABLE_NAME', 'string', None, None, None, None, None), ('TABLE_TYPE', 'string', None, None, None, None, None), ('REMARKS', 'string', None, None, None, None, None), ('TYPE_CAT', 'string', None, None, None, None, None), ('TYPE_SCHEM', 'string', None, None, None, None, None), ('TYPE_NAME', 'string', None, None, None, None, None), ('SELF_REFERENCING_COL_NAME', 'string', None, None, None, None, None), ('REF_GENERATION', 'string', None, None, None, None, None)] vs [('namespace', 'string', None, None, None, None, None), ('tableName', 'string', None, None, None, None, None), ('isTemporary', 'boolean', None, None, None, None, None), ('information', 'string', None, None, None, None, None), ('catalogName', 'string', None, None, None, None, None), ('tableType', 'string', None, None, None, None, None), ('remarks', 'string', None, None, None, None, None)]
  - Field 'tableType' missing in all Thrift rows
  - Field 'catalogName' missing in all Thrift rows
  - Field 'TYPE_CAT' missing in all SEA rows
  - Field 'TYPE_NAME' missing in all SEA rows
  - Field 'TABLE_TYPE' missing in all SEA rows
  - Field 'TYPE_SCHEM' missing in all SEA rows

**Data Differences:**
-----------------
Row Data:
  - Field 'tableName' missing in all Thrift rows
  - Field 'remarks' missing in all Thrift rows
  - Field 'namespace' missing in all Thrift rows
  - Field 'isTemporary' missing in all Thrift rows
  - Field 'information' missing in all Thrift rows
  - Field 'SELF_REFERENCING_COL_NAME' missing in all SEA rows
  - Field 'TABLE_SCHEM' missing in all SEA rows
  - Field 'TABLE_CAT' missing in all SEA rows
  - Field 'REMARKS' missing in all SEA rows
  - Field 'TABLE_NAME' missing in all SEA rows
  - Field 'REF_GENERATION' missing in all SEA rows

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** columns(main, tpcds_sf100_delta, catalog_sales, None)
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Description length mismatch: Thrift has 23 columns, SEA has 13: [('TABLE_CAT', 'string', None, None, None, None, None), ('TABLE_SCHEM', 'string', None, None, None, None, None), ('TABLE_NAME', 'string', None, None, None, None, None), ('COLUMN_NAME', 'string', None, None, None, None, None), ('DATA_TYPE', 'int', None, None, None, None, None), ('TYPE_NAME', 'string', None, None, None, None, None), ('COLUMN_SIZE', 'int', None, None, None, None, None), ('BUFFER_LENGTH', 'tinyint', None, None, None, None, None), ('DECIMAL_DIGITS', 'int', None, None, None, None, None), ('NUM_PREC_RADIX', 'int', None, None, None, None, None), ('NULLABLE', 'int', None, None, None, None, None), ('REMARKS', 'string', None, None, None, None, None), ('COLUMN_DEF', 'string', None, None, None, None, None), ('SQL_DATA_TYPE', 'int', None, None, None, None, None), ('SQL_DATETIME_SUB', 'int', None, None, None, None, None), ('CHAR_OCTET_LENGTH', 'int', None, None, None, None, None), ('ORDINAL_POSITION', 'int', None, None, None, None, None), ('IS_NULLABLE', 'string', None, None, None, None, None), ('SCOPE_CATALOG', 'string', None, None, None, None, None), ('SCOPE_SCHEMA', 'string', None, None, None, None, None), ('SCOPE_TABLE', 'string', None, None, None, None, None), ('SOURCE_DATA_TYPE', 'smallint', None, None, None, None, None), ('IS_AUTO_INCREMENT', 'string', None, None, None, None, None)] vs [('col_name', 'string', None, None, None, None, None), ('catalogName', 'string', None, None, None, None, None), ('namespace', 'string', None, None, None, None, None), ('tableName', 'string', None, None, None, None, None), ('columnType', 'string', None, None, None, None, None), ('columnSize', 'int', None, None, None, None, None), ('decimalDigits', 'int', None, None, None, None, None), ('radix', 'int', None, None, None, None, None), ('isNullable', 'string', None, None, None, None, None), ('remarks', 'string', None, None, None, None, None), ('ordinalPosition', 'int', None, None, None, None, None), ('isAutoIncrement', 'string', None, None, None, None, None), ('isGenerated', 'string', None, None, None, None, None)]
  - Field 'columnType' missing in all Thrift rows
  - Field 'columnSize' missing in all Thrift rows
  - Field 'catalogName' missing in all Thrift rows
  - Field 'COLUMN_SIZE' missing in all SEA rows
  - Field 'COLUMN_DEF' missing in all SEA rows
  - Field 'TYPE_NAME' missing in all SEA rows
  - Field 'SOURCE_DATA_TYPE' missing in all SEA rows
  - Field 'SQL_DATA_TYPE' missing in all SEA rows
  - Field 'SCOPE_CATALOG' missing in all SEA rows
  - Field 'COLUMN_NAME' missing in all SEA rows
  - Field 'SCOPE_SCHEMA' missing in all SEA rows
  - Field 'DATA_TYPE' missing in all SEA rows

**Data Differences:**
-----------------
Row Data:
  - Field 'isAutoIncrement' missing in all Thrift rows
  - Field 'tableName' missing in all Thrift rows
  - Field 'remarks' missing in all Thrift rows
  - Field 'namespace' missing in all Thrift rows
  - Field 'radix' missing in all Thrift rows
  - Field 'isGenerated' missing in all Thrift rows
  - Field 'ordinalPosition' missing in all Thrift rows
  - Field 'isNullable' missing in all Thrift rows
  - Field 'col_name' missing in all Thrift rows
  - Field 'decimalDigits' missing in all Thrift rows
  - Field 'NULLABLE' missing in all SEA rows
  - Field 'BUFFER_LENGTH' missing in all SEA rows
  - Field 'TABLE_NAME' missing in all SEA rows
  - Field 'NUM_PREC_RADIX' missing in all SEA rows
  - Field 'DECIMAL_DIGITS' missing in all SEA rows
  - Field 'SQL_DATETIME_SUB' missing in all SEA rows
  - Field 'IS_NULLABLE' missing in all SEA rows
  - Field 'TABLE_SCHEM' missing in all SEA rows
  - Field 'SCOPE_TABLE' missing in all SEA rows
  - Field 'IS_AUTO_INCREMENT' missing in all SEA rows
  - Field 'TABLE_CAT' missing in all SEA rows
  - Field 'ORDINAL_POSITION' missing in all SEA rows
  - Field 'CHAR_OCTET_LENGTH' missing in all SEA rows
  - Field 'REMARKS' missing in all SEA rows

============================

**Query Type:** Cursor Metadata Methods
**Query/Method:** columns(main, tpcds_sf100_delta, catalog_sales, cs_%)
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Description length mismatch: Thrift has 23 columns, SEA has 13: [('TABLE_CAT', 'string', None, None, None, None, None), ('TABLE_SCHEM', 'string', None, None, None, None, None), ('TABLE_NAME', 'string', None, None, None, None, None), ('COLUMN_NAME', 'string', None, None, None, None, None), ('DATA_TYPE', 'int', None, None, None, None, None), ('TYPE_NAME', 'string', None, None, None, None, None), ('COLUMN_SIZE', 'int', None, None, None, None, None), ('BUFFER_LENGTH', 'tinyint', None, None, None, None, None), ('DECIMAL_DIGITS', 'int', None, None, None, None, None), ('NUM_PREC_RADIX', 'int', None, None, None, None, None), ('NULLABLE', 'int', None, None, None, None, None), ('REMARKS', 'string', None, None, None, None, None), ('COLUMN_DEF', 'string', None, None, None, None, None), ('SQL_DATA_TYPE', 'int', None, None, None, None, None), ('SQL_DATETIME_SUB', 'int', None, None, None, None, None), ('CHAR_OCTET_LENGTH', 'int', None, None, None, None, None), ('ORDINAL_POSITION', 'int', None, None, None, None, None), ('IS_NULLABLE', 'string', None, None, None, None, None), ('SCOPE_CATALOG', 'string', None, None, None, None, None), ('SCOPE_SCHEMA', 'string', None, None, None, None, None), ('SCOPE_TABLE', 'string', None, None, None, None, None), ('SOURCE_DATA_TYPE', 'smallint', None, None, None, None, None), ('IS_AUTO_INCREMENT', 'string', None, None, None, None, None)] vs [('col_name', 'string', None, None, None, None, None), ('catalogName', 'string', None, None, None, None, None), ('namespace', 'string', None, None, None, None, None), ('tableName', 'string', None, None, None, None, None), ('columnType', 'string', None, None, None, None, None), ('columnSize', 'int', None, None, None, None, None), ('decimalDigits', 'int', None, None, None, None, None), ('radix', 'int', None, None, None, None, None), ('isNullable', 'string', None, None, None, None, None), ('remarks', 'string', None, None, None, None, None), ('ordinalPosition', 'int', None, None, None, None, None), ('isAutoIncrement', 'string', None, None, None, None, None), ('isGenerated', 'string', None, None, None, None, None)]

**Data Differences:**
-----------------
Row Data:
  - Row count mismatch: Thrift returned 34, SEA returned 0

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 10
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 10
============================

**Result:** PASSED
**Execution Time:** Thrift: 0.0000s, SEA: 0.0000s

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales WHERE 1=0
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** SQL Query
**Query/Method:** SELECT NULL as null_col, 'test' as string_col, 123 as int_col
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 0 (null_col) type_code mismatch: string vs null

============================

**Query Type:** SQL Query
**Query/Method:** 
        SELECT 
            CAST(123 AS TINYINT) as tiny_col,
            CAST(456 AS SMALLINT) as small_col,
            CAST(789 AS INT) as int_col,
            CAST(123456789 AS BIGINT) as big_col,
            CAST(123.45 AS FLOAT) as float_col,
            CAST(678.90 AS DOUBLE) as double_col,
            CAST(123.456 AS DECIMAL(10,3)) as decimal_col,
            'test_string' as string_col,
            TRUE as bool_col,
            CAST('2023-01-01' AS DATE) as date_col,
            CAST('2023-01-01 12:34:56' AS TIMESTAMP) as timestamp_col,
            ARRAY(1,2,3) as array_col,
            STRUCT(1 as a, 'b' as b) as struct_col,
            MAP('key1', 'value1', 'key2', 'value2') as map_col
        
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 0 (tiny_col) type_code mismatch: tinyint vs byte
  - Column 1 (small_col) type_code mismatch: smallint vs short
  - Column 3 (big_col) type_code mismatch: bigint vs long

**Data Differences:**
-----------------
Row Data:
  - Exception: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()

============================

**Query Type:** SQL Query
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales LIMIT 1000
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** Parameterized Queries
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales WHERE cs_sold_date_sk = :date_sk LIMIT 5
**Method Arguments:** date_sk
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** Parameterized Queries
**Query/Method:** SELECT * FROM main.tpcds_sf100_delta.catalog_sales WHERE cs_sold_date_sk = ? AND cs_sold_time_sk = ? LIMIT 5
**Method Arguments:** 2451088, 48000
============================

**Metadata Differences:**
---------------------
Column Metadata:
  - Column 17 (cs_order_number) type_code mismatch: bigint vs long

============================

**Query Type:** Batch Operations
**Query/Method:** INSERT INTO main.default.comparator_test_table VALUES (?, ?)
**Method Arguments:** (1, 'one'), (2, 'two'), (3, 'three')
============================

**Result:** PASSED
**Execution Time:** Thrift: 2.7094s, SEA: 3.1544s

============================

