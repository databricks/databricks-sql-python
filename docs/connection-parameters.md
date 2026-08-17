# Connection parameters

This is a reference for every keyword argument accepted by
`databricks.sql.connect(...)` (which forwards straight into
`Connection.__init__`). For each parameter it lists the type, default,
which backend(s) actually consume it, and what it does.

```python
from databricks import sql

connection = sql.connect(
    server_hostname="********.databricks.com",
    http_path="/sql/1.0/warehouses/abc123",
    access_token="dapi...",
    # ...any of the parameters below...
)
```

## The three backends

The connector can talk to Databricks through one of three backend
implementations. You pick one at connect time:

| Backend | Selected by | Status | Notes |
| --- | --- | --- | --- |
| **Thrift** | *(default)* | GA | Thrift-over-HTTP client. Works against SQL warehouses **and** all-purpose (interactive) clusters. |
| **SEA** | `use_sea=True` | Public preview | Pure-Python client for the [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution). SQL warehouses only. |
| **Kernel** | `use_kernel=True` | Early access | Routes through the native Rust core (`databricks-sql-kernel`) via PyO3. Requires Python ≥ 3.10 and the `[kernel]` extra. See the "Rust kernel backend" section of the [README](../README.md). |

`use_sea` and `use_kernel` are **mutually exclusive** — passing both raises
`ValueError`.

## How to read the backend columns

The **Thrift**, **SEA**, and **Kernel** columns say whether that backend
actually reads the parameter:

- **✓** — consumed by that backend.
- **✗** — accepted (it's just `**kwargs`) but ignored by that backend, so it
  has no effect.
- **—** — not applicable (e.g. a backend-selection flag).

> **Kernel is early access.** The session layer forwards only a *curated,
> named subset* of parameters to the kernel (it does **not** splat
> `**kwargs`). Any parameter marked **✗** for Kernel is silently dropped on
> that path rather than raising — so it will not take effect. Result-format
> and transport tuning (cloud fetch, LZ4, download threads, arrow-native
> decimals/timestamps) is managed inside the kernel and is not user-tunable
> from here yet. Kernel capabilities are still landing; treat its column as a
> snapshot of connector `4.4.0`.

Parameters whose name begins with an underscore (e.g. `_socket_timeout`) are
**internal / advanced** knobs. They are not part of the stable public API and
may change without notice, but they are documented here because they are
commonly used in the field.

## Parameters

| Parameter | Type | Default | Thrift | SEA | Kernel | Meaning |
| --- | --- | --- | :---: | :---: | :---: | --- |
| **Connection target** | | | | | | |
| `server_hostname` | `str` | *required* | ✓ | ✓ | ✓ | Databricks workspace hostname, e.g. `dbc-12345.cloud.databricks.com`. |
| `http_path` | `str` | *required* | ✓ | ✓ | ✓ | HTTP path to a SQL warehouse (`/sql/1.0/warehouses/...`) or, for Thrift only, an all-purpose cluster (`/sql/protocolv1/o/.../...`). SEA and Kernel require a warehouse/endpoint path. |
| `_port` | `int` | `443` | ✓ | ✓ | ✗ | TCP port. Advanced/testing only. |
| `_connection_uri` | `str` | `None` | ✓ | ✗ | ✗ | Overrides `server_hostname`/`http_path` with a full URI. Internal. |
| **Authentication** | | | | | | |
| `access_token` | `str` | `None` | ✓ | ✓ | ✓ | Personal Access Token / bearer token. If omitted, auth falls back to OAuth. |
| `auth_type` | `str` | `None` | ✓ | ✓ | ✓ | Auth flow selector: `databricks-oauth` (U2M/M2M) or `azure-oauth` (Microsoft Entra ID). Defaults to Databricks OAuth when no token or cert is given. |
| `oauth_client_id` | `str` | built-in | ✓ | ✓ | ✓ | Custom OAuth client ID. Defaults to the connector's built-in `databricks-sql-python` client. |
| `oauth_redirect_port` | `int` | `None` | ✓ | ✓ | ✓ | Localhost redirect port for the U2M browser flow. Required when a custom `oauth_client_id` is set. |
| `oauth_client_secret` | `str` | `None` | ✗ | ✗ | ✓ | OAuth M2M client secret. Only honored on the Kernel path today; the Thrift/SEA connector does not consume it. |
| `oauth_scopes` | `List[str]` | `["sql", "offline_access"]` | ✗ | ✗ | ✓ | Custom OAuth scopes. Thrift/SEA always use the built-in scope set; only the Kernel path reads a custom value. |
| `experimental_oauth_persistence` | `OAuthPersistence` | `None` | ✓ | ✓ | ✗ | Storage backend for persisting OAuth tokens across process restarts (beta). Kernel manages its own token lifecycle. |
| `credentials_provider` | `CredentialsProvider` | `None` | ✓ | ✓ | ✓ | Custom credentials provider for external auth. |
| `identity_federation_client_id` | `str` | `None` | ✓ | ✓ | ✓ | Token-federation (workload identity federation) client ID. |
| `azure_client_id` | `str` | `None` | ✓ | ✓ | ✗ | Microsoft Entra ID (Azure AD) service-principal client/app ID (with `auth_type="azure-oauth"`). |
| `azure_client_secret` | `str` | `None` | ✓ | ✓ | ✗ | Azure service-principal client secret. |
| `azure_tenant_id` | `str` | `None` | ✓ | ✓ | ✗ | Azure AD tenant ID. |
| `azure_workspace_resource_id` | `str` | `None` | ✓ | ✓ | ✗ | Azure workspace resource ID. |
| `_use_cert_as_auth` | `bool` | `False` | ✓ | ✓ | ✗ | Authenticate with a TLS client certificate instead of a token/OAuth. Internal. |
| `username` / `password` | `str` | `None` | ✗ | ✗ | ✗ | **Removed.** Basic auth is no longer supported; passing either raises `ValueError`. |
| **TLS / SSL** | | | | | | |
| `_tls_no_verify` | `bool` | `False` | ✓ | ✓ | ✓ | Disable all TLS verification (cert **and** hostname). Dangerous — testing only. |
| `_tls_verify_hostname` | `bool` | `True` | ✓ | ✓ | ✓ | Verify the server hostname matches the certificate (cert still verified). |
| `_tls_trusted_ca_file` | `str` | `None` | ✓ | ✓ | ✓ | Path to a CA bundle for server-cert verification. Defaults to the system trust store. |
| `_tls_client_cert_file` | `str` | `None` | ✓ | ✓ | ✓ | Path to a client certificate (mutual TLS). |
| `_tls_client_cert_key_file` | `str` | `None` | ✓ | ✓ | ✓ | Path to the client certificate's private key. |
| `_tls_client_cert_key_password` | `str` | `None` | ✓ | ✓ | ✓ | Password for an encrypted client-key file. |
| **Session setup** | | | | | | |
| `http_headers` | `List[Tuple[str, str]]` | `None` | ✓ | ✓ | ✓ | Extra `(key, value)` HTTP headers sent on every request. |
| `session_configuration` | `Dict[str, Any]` | `None` | ✓ | ✓ | ✓ | Spark/SQL session parameters (e.g. `{"ansi_mode": "true"}`). Run `SET -v` for the full list. |
| `catalog` | `str` | `None` | ✓ | ✓ | ✓ | Initial catalog for the session (DBR 9.0+). |
| `schema` | `str` | `None` | ✓ | ✓ | ✓ | Initial schema for the session (DBR 9.0+). |
| `query_tags` | `Dict[str, Optional[str]]` | `None` | ✓ | ✓ | ✓ | Key/value tags serialized into the `QUERY_TAGS` session config. (Per-*statement* query tags are not yet supported on Kernel.) |
| `enable_metric_view_metadata` | `bool` | `False` | ✓ | ✓ | ✓ | Sets `spark.sql.thriftserver.metadata.metricview.enabled` so `cursor.tables()`/`cursor.columns()` surface metric-view metadata. |
| `user_agent_entry` | `str` | `None` | ✓ | ✓ | ✓ | Custom tag appended to the `User-Agent` header (used by partners to identify their app). |
| `_user_agent_entry` | `str` | `None` | ✓ | ✓ | ✓ | **Deprecated** alias for `user_agent_entry`; emits a warning. |
| **Backend selection** | | | | | | |
| `use_sea` | `bool` | `False` | — | — | — | Route through the pure-Python SEA backend. |
| `use_kernel` | `bool` | `False` | — | — | — | Route through the Rust kernel backend. Mutually exclusive with `use_sea`. |
| `use_hybrid_disposition` | `bool` | `False` | ✗ | ✓ | ✗ | SEA only: use the hybrid result disposition instead of inline. |
| **Result format & data types** | | | | | | |
| `use_cloud_fetch` | `bool` | `True` | ✓ | ✓ | ✗ | Download large result sets in parallel from cloud storage. Kernel manages result transport internally. |
| `max_download_threads` | `int` | `10` | ✓ | ✓ | ✗ | Worker threads for cloud-fetch downloads. |
| `enable_query_result_lz4_compression` | `bool` | `True` | ✓ | ✓ | ✗ | LZ4-compress result payloads. |
| `_disable_pandas` | `bool` | `False` | ✓ | ✓ | ✗ | Skip the pandas-based Arrow deserialization path (fallback for pandas edge cases). |
| `_use_arrow_native_complex_types` | `bool` | `True` | ✓ | ✓ | ✓ | Return `ARRAY`/`MAP`/`STRUCT` as native Arrow types instead of JSON strings. |
| `_use_arrow_native_decimals` | `bool` | `True` | ✓ | ✗ | ✗ | Return `DECIMAL` as a native Arrow type instead of a string. |
| `_use_arrow_native_timestamps` | `bool` | `True` | ✓ | ✗ | ✗ | Return `TIMESTAMP` as a native Arrow type instead of a string. |
| **Query parameters & transactions** | | | | | | |
| `use_inline_params` | `bool` \| `"silent"` | `False` | ✓ | ✓ | ✗ | Render parameters inline into the SQL text (legacy) instead of native bound parameters (DBR 14.1+). `"silent"` suppresses the deprecation warning. |
| `ignore_transactions` | `bool` | `True` | ✓ | ✓ | ✓ | When `True`: `commit()` is a no-op, `rollback()` raises `NotSupportedError`, and setting `autocommit` is a no-op. |
| `fetch_autocommit_from_server` | `bool` | `False` | ✓ | ✓ | ✓ | Query the server (`SET AUTOCOMMIT`) for autocommit state instead of returning the cached value. |
| **Volume staging (`PUT`/`GET`)** | | | | | | |
| `staging_allowed_local_path` | `str` \| `List[str]` | `None` | ✓ | ✓ | ✗ | Local path(s) permitted for Unity Catalog Volume `PUT`/`GET` staging operations. Kernel has no Volume API yet. |
| **Networking / connection pool** | | | | | | |
| `_socket_timeout` | `float` | `900` (Thrift) | ✓ | ✗ | ✗ | Socket send/recv/connect timeout, in seconds. |
| `_pool_connections` | `int` | `10` | ✓ | ✓ | ✓ | Number of urllib3 connection pools on the shared HTTP client. |
| `_pool_maxsize` | `int` | `20` | ✓ | ✓ | ✓ | Max connections per pool on the shared HTTP client. |
| `_proxy_auth_method` | `str` | `None` | ✓ | ✓ | ✓ | Proxy authentication scheme: `basic` or `negotiate` (Kerberos). See [`docs/proxy.md`](proxy.md). |
| **Retry policy** | | | | | | |
| `_retry_stop_after_attempts_count` | `int` | `30` | ✓ | ✓ | ✓ | Max attempts in a retry sequence. Bounded to `[1, 60]` on Thrift. |
| `_retry_stop_after_attempts_duration` | `float` | `900` | ✓ | ✓ | ✓ | Max total wall-clock seconds spent retrying. |
| `_retry_delay_min` | `float` | `1` | ✓ | ✓ | ✓ | Minimum backoff delay between retries (seconds). |
| `_retry_delay_max` | `float` | `60` | ✓ | ✓ | ✓ | Maximum backoff delay between retries (seconds). |
| `_retry_delay_default` | `float` | `5` | ✓ | ✓ | ✗ | Delay used when a poll fails due to a TCP/OS error. |
| `_retry_dangerous_codes` | `List[int]` | `[]` | ✓ | ✓ | ✗ | HTTP status codes for which even non-idempotent commands (e.g. `ExecuteStatement`) are retried. |
| `_respect_server_retry_after_header` | `bool` | `False` | ✓ | ✓ | ✗ | Honor the server's `Retry-After` header. |
| `_retry_max_redirects` | `int` | `None` | ✓ | ✓ | ✗ | Max HTTP redirects to follow (must be ≤ `_retry_stop_after_attempts_count`). |
| `_enable_v3_retries` | `bool` | `True` | ✓ | ✓ | ✗ | Use the urllib3-based v3 retry policy. Setting `False` selects the deprecated legacy policy. |
| **Telemetry** | | | | | | |
| `enable_telemetry` | `bool` | `True` | ✓ | ✓ | ✓ | Enable client telemetry collection. |
| `force_enable_telemetry` | `bool` | `False` | ✓ | ✓ | ✓ | Force telemetry on regardless of the server-side feature flag. |
| `telemetry_batch_size` | `int` | `100` | ✓ | ✓ | ✓ | Number of telemetry events buffered before a flush. |
| `_telemetry_circuit_breaker_enabled` | `bool` | `True` | ✓ | ✓ | ✓ | Enable the telemetry circuit breaker. |

## Notes

- **Underscore-prefixed parameters are advanced/internal** and not part of the
  stable public API. They can change between releases.
- **`_socket_timeout`** governs the Thrift transport socket. The SEA and
  Kernel HTTP layers manage their own timeouts and do not read it. (It is also
  passed to the auth HTTP client, which applies its own retry defaults.)
- **Retry defaults for auth requests differ.** The values above are the
  defaults for *query* traffic (from the backend retry policy). The internal
  auth HTTP client reuses the same `_retry_*` kwargs but applies its own
  built-in defaults when they are unset.
- A couple of flags are declared in the code but currently unused (`_enable_ssl`,
  `_skip_routing_headers`); they are omitted from the table above.

*Generated against connector version `4.4.0`. When in doubt, the source of
truth is `Connection.__init__` in [`src/databricks/sql/client.py`](../src/databricks/sql/client.py)
and the per-backend clients under `src/databricks/sql/backend/`.*
