from databricks import sql
import os

# Users of connector versions >= 2.9.0 and <= 3.0.0 can use the v3 retry behaviour by setting _enable_v3_retries=True
# This flag will be deprecated in databricks-sql-connector~=3.0.0 as it will become the default.
#
# The new retry behaviour is defined in src/databricks/sql/auth/retry.py
# 
# The new retry behaviour allows users to force the connector to automatically retry requests that fail with codes
# that are not retried by default (in most cases only codes 429 and 503 are retried by default). Additional HTTP
# codes to retry are specified as a list passed to `_retry_dangerous_codes`.
#
# Note that, as implied in the name, doing this is *dangerous* and should not be configured in all usages.
# With the default behaviour, ExecuteStatement Thrift commands are only retried for codes 429 and 503 because
# we can be certain at run-time that the statement never reached Databricks compute. These codes are returned by
# the SQL gateway / load balancer. So there is no risk that retrying the request would result in a doubled
# (or tripled etc) command execution. These codes are always accompanied by a Retry-After header, which we honour.
#
# However, if your use-case emits idempotent queries such as SELECT statements, it can be helpful to retry 
# for 502 (Bad Gateway) codes etc. In these cases, there is a possibility that the initial command _did_ reach
# Databricks compute and retrying it could result in additional executions. Retrying under these conditions uses
# an exponential back-off since a Retry-After header is not present.
#
# This new retry behaviour allows you to configure the maximum number of redirects that the connector will follow.
# Just set `_retry_max_redirects` to the integer number of redirects you want to allow. The default is None,
# which means all redirects will be followed. In this case, a redirect will count toward the
# _retry_stop_after_attempts_count which means that by default the connector will not enter an endless retry loop.
#
# For complete information about configuring retries, see the docstring for databricks.sql.thrift_backend.ThriftBackend

with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN"),
                 _enable_v3_retries = True,
                 _retry_dangerous_codes=[502,400],
                 _retry_max_redirects=2) as connection:

  with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM default.diamonds LIMIT 2")
    result = cursor.fetchall()

    for row in result:
      print(row)
