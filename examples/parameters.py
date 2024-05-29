"""
This example demonstrates how to use parameters in both native (default) and inline (legacy) mode.
"""

from decimal import Decimal
from databricks import sql
from databricks.sql.parameters import *

import os
from databricks import sql
from datetime import datetime
import pytz

host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")


native_connection = sql.connect(
    server_hostname=host, http_path=http_path, access_token=access_token
)

inline_connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token,
    use_inline_params="silent",
)

# Example 1 demonstrates how in most cases, queries written for databricks-sql-connector<3.0.0 will work
# with databricks-sql-connector>=3.0.0. This is because the default mode is native mode, which is backwards
# compatible with the legacy inline mode.

LEGACY_NAMED_QUERY = "SELECT %(name)s `name`, %(age)s `age`, %(active)s `active`"
EX1_PARAMS = {"name": "Jane", "age": 30, "active": True}

with native_connection.cursor() as cursor:
    ex1_native_result = cursor.execute(LEGACY_NAMED_QUERY, EX1_PARAMS).fetchone()

with inline_connection.cursor() as cursor:
    ex1_inline_result = cursor.execute(LEGACY_NAMED_QUERY, EX1_PARAMS).fetchone()

print("\nEXAMPLE 1")
print("Example 1 result in native mode\t→\t", ex1_native_result)
print("Example 1 result in inline mode\t→\t", ex1_inline_result)


# Example 2 shows how to update example 1 to use the new `named` parameter markers.
# This query would fail in inline mode.

# This is an example of the automatic transformation from pyformat → named.
# The output looks like this:
# SELECT :name `name`, :age `age`, :active `active`
NATIVE_NAMED_QUERY = LEGACY_NAMED_QUERY % {
    "name": ":name",
    "age": ":age",
    "active": ":active",
}
EX2_PARAMS = EX1_PARAMS

with native_connection.cursor() as cursor:
    ex2_named_result = cursor.execute(NATIVE_NAMED_QUERY, EX1_PARAMS).fetchone()

with native_connection.cursor() as cursor:
    ex2_pyformat_result = cursor.execute(LEGACY_NAMED_QUERY, EX1_PARAMS).fetchone()

print("\nEXAMPLE 2")
print("Example 2 result with pyformat \t→\t", ex2_named_result)
print("Example 2 result with named \t→\t", ex2_pyformat_result)


# Example 3 shows how to use positional parameters. Notice the syntax is different between native and inline modes.
# No automatic transformation is done here. So the LEGACY_POSITIONAL_QUERY will not work in native mode.

NATIVE_POSITIONAL_QUERY = "SELECT ? `name`, ? `age`, ? `active`"
LEGACY_POSITIONAL_QUERY = "SELECT %s `name`, %s `age`, %s `active`"

EX3_PARAMS = ["Jane", 30, True]

with native_connection.cursor() as cursor:
    ex3_native_result = cursor.execute(NATIVE_POSITIONAL_QUERY, EX3_PARAMS).fetchone()

with inline_connection.cursor() as cursor:
    ex3_inline_result = cursor.execute(LEGACY_POSITIONAL_QUERY, EX3_PARAMS).fetchone()

print("\nEXAMPLE 3")
print("Example 3 result in native mode\t→\t", ex3_native_result)
print("Example 3 result in inline mode\t→\t", ex3_inline_result)

# Example 4 shows how to bypass type inference and set an exact Databricks SQL type for a parameter.
# This is only possible when use_inline_params=False


moment = datetime(2012, 10, 15, 12, 57, 18)
chicago_timezone = pytz.timezone("America/Chicago")

# For this parameter value, we don't bypass inference. So we know that the connector
# will infer the datetime object to be a TIMESTAMP, which preserves the timezone info.
ex4_p1 = chicago_timezone.localize(moment)

# For this parameter value, we bypass inference and set the type to TIMESTAMP_NTZ,
# which does not preserve the timezone info. Therefore we expect the timezone
# will be dropped in the roundtrip.
ex4_p2 = TimestampNTZParameter(value=ex4_p1)

# For this parameter, we don't bypass inference. So we know that the connector
# will infer the Decimal to be a DECIMAL and will preserve its current precision and scale.
ex4_p3 = Decimal("12.3456")

# For this parameter value, we bind a decimal with custom scale and precision
# that will result in the decimal being truncated.
ex4_p4 = DecimalParameter(value=ex4_p3, scale=4, precision=2)

EX4_QUERY = "SELECT ? `p1`, ? `p2`, ? `p3`, ? `p4`"
EX4_PARAMS = [ex4_p1, ex4_p2, ex4_p3, ex4_p4]
with native_connection.cursor() as cursor:
    result = cursor.execute(EX4_QUERY, EX4_PARAMS).fetchone()

print("\nEXAMPLE 4")
print("Example 4 inferred result\t→\t {}\t{}".format(result.p1, result.p3))
print("Example 4 explicit result\t→\t {}\t\t{}".format(result.p2, result.p4))
