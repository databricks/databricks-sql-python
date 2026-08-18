# Connection parameters reference

This document lists **every public connection / session parameter that is
consumed by at least one currently-supported backend** the Python connector
(`databricks.sql.connect(...)`) accepts, and — because the driver
ships more than one backend — whether each parameter is honored on the
**Thrift** backend (the default) or the **Kernel** backend (opt-in via
`use_kernel=True`).

The goal is to make protocol gaps explicit: a parameter honored on one backend
but ignored (or rejected) on the other is called out in the **Note** column.

> **Backend selection.** The connector defaults to Thrift. The **Kernel**
> backend — a native Rust core exposed via PyO3 — is selected with
> `use_kernel=True`; it requires **Python ≥ 3.10** and the
> `databricks-sql-connector[kernel]` extra, and is **early access** (its
> parameter surface is still landing and may change without notice). Crucially,
> the session layer forwards only a *curated, named subset* of parameters to
> the kernel (`Session._create_backend`, `src/databricks/sql/session.py`) — it
> does **not** splat `**kwargs` — so anything outside that subset is silently
> ignored on the kernel path. That is why many rows below are ❌ for Kernel.
>
> A separate pure-Python **SEA** backend (`use_sea=True`) also exists but is
> being deprecated, so it is intentionally omitted from this reference.

## Legend

| Symbol | Meaning                                                             |
| ------ | ------------------------------------------------------------------- |
| ✅     | Honored — the option is read and forwarded to the backend.          |
| ❌     | Ignored or rejected — see the Note column.                          |
| ⚠️     | Partially supported or behaves differently from the other backend.  |
| —      | Not applicable / no public equivalent / no default on this backend. |

Parameters whose name begins with an underscore (e.g. `_socket_timeout`) are
**internal / advanced** knobs — not part of the stable public API, and subject
to change without notice.

## Sources of truth

- Public signature ← `Connection.__init__` (`src/databricks/sql/client.py`) and
  `connect()` (`src/databricks/sql/__init__.py`).
- Backend routing / kernel-forwarded subset ← `Session._create_backend`
  (`src/databricks/sql/session.py`).
- Default values ← the backend clients' `__init__` and the shared
  `ClientContext` (`src/databricks/sql/auth/common.py`).
- Thrift wiring ← `src/databricks/sql/backend/thrift_backend.py`; auth ←
  `src/databricks/sql/auth/auth.py`.
- Kernel wiring ← `src/databricks/sql/backend/kernel/client.py` and
  `auth_bridge.py`, plus the `kernel_auth_options` / `kernel_retry_options`
  forwarding in `session.py`.
- Shared HTTP / telemetry ← `src/databricks/sql/common/unified_http_client.py`
  and `build_client_context` (`src/databricks/sql/utils.py`).

---

## Connection identity

| Option            | Type  | Thrift | Kernel | Default Value | Note                                                                                                                    |
| ----------------- | ----- | :----: | :----: | ------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `server_hostname` | `str` |   ✅   |   ✅   | — (required)  | Workspace hostname, e.g. `dbc-12345.cloud.databricks.com`.                                                              |
| `http_path`       | `str` |   ✅   |   ✅   | — (required)  | Thrift accepts a SQL-warehouse path **or** an all-purpose-cluster path; Kernel requires a warehouse/endpoint path.      |
| `_port`           | `int` |   ✅   |   —    | `443`         | TCP port (advanced). Not threaded to the kernel; it derives host/port from `server_hostname` + `http_path`.             |
| `_connection_uri` | `str` |   ✅   |   —    | `None`        | Thrift-only internal override of `server_hostname`/`http_path`. No kernel equivalent.                                   |
| `user_agent_entry`| `str` |   ✅   |   ✅   | `None`        | Custom tag folded into the composed `User-Agent` on both. (`_user_agent_entry` is a deprecated alias that warns.)       |

## Authentication

| Option                                              | Type                 | Thrift | Kernel | Default Value             | Note                                                                                                                                                            |
| --------------------------------------------------- | -------------------- | :----: | :----: | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `access_token` (PAT)                                | `str`                |   ✅   |   ✅   | `None`                    | Personal Access Token / bearer token. The default auth mode when set; otherwise auth falls back to OAuth.                                                       |
| `auth_type`                                         | `str`                |   ✅   |   ✅   | `None` ⇒ Databricks OAuth | `databricks-oauth` (U2M), `azure-oauth` (Azure AD U2M), or `azure-sp-m2m` (Azure service-principal M2M). All three work on the kernel path; `azure-oauth` / `azure-sp-m2m` kernel support added in #919 (they route onto the kernel's generic OAuth flows with Azure values). |
| `oauth_client_id` (U2M)                             | `str`                |   ✅   |   ✅   | built-in client id        | Custom U2M client id. Forwarded on both; when absent, each path applies its own built-in default.                                                              |
| `oauth_redirect_port` (U2M)                         | `int`                |   ✅   |   ✅   | `None`                    | Localhost redirect port for the browser flow. On **both** backends it is only honored when a custom `oauth_client_id` is also supplied — then that single port becomes the redirect URI. With the built-in client id (or when omitted) the connector uses the full registered range 8020–8024 and binds the first free port, so a bare `oauth_redirect_port` has no effect. (Thrift: `auth.py` `oauth_redirect_port_range`; Kernel: same logic, forwarded as `redirect_ports`.) |
| `oauth_client_secret` (OAuth M2M)                   | `str`                |   ❌   |   ✅   | `None`                    | **Kernel-only in practice.** The Thrift auth path never reads `oauth_client_secret`; use `credentials_provider` or an Azure service principal for M2M on Thrift. |
| `oauth_scopes`                                      | `List[str]`          |   ❌   |   ✅   | `["sql","offline_access"]`| **Thrift ignores custom scopes** — it always uses the built-in scope set. Only the kernel honors a custom `oauth_scopes`.                                       |
| `credentials_provider`                              | `CredentialsProvider`|   ✅   |   ❌   | `None`                    | Custom external credentials provider. **Rejected on the kernel path** (`NotSupportedError`) — it is an opaque token source, so the kernel cannot own the token lifecycle; use `oauth_client_id` + `oauth_client_secret` for M2M, or the Thrift backend. |
| `identity_federation_client_id`                     | `str`                |   ✅   |   ✅   | `None`                    | Workload identity / token-federation client id (kernel support added in #910).                                                                                 |
| `experimental_oauth_persistence`                    | `OAuthPersistence`   |   ✅   |   ❌   | `None`                    | **Thrift-only.** The kernel owns its own token lifecycle and does not accept a persistence store.                                                              |
| `azure_client_id` / `azure_client_secret` / `azure_tenant_id` | `str` | ✅ | ✅ | `None` | Azure service-principal (Entra ID M2M), selected by `auth_type="azure-sp-m2m"`. On the kernel path these route onto OAuth M2M with an Entra v2.0 token endpoint + the Databricks-resource `.default` scope (#919). **`azure_tenant_id` is required on the kernel path** — unlike Thrift, it is not auto-discovered from the workspace. |
| `azure_workspace_resource_id`                       | `str`                |   ✅   |   ⚠️   | `None`                    | Thrift sends this with the Azure SP **management token** (`X-Databricks-Azure-SP-Management-Token`) to authorize an SP that has an Azure RBAC role but is not a workspace member. **Not applied on the kernel path** — the management-token flow is unsupported there (matching the Go and Node SQL drivers, which don't use it); add the SP as a workspace principal instead. Setting it on the kernel path logs a warning and is otherwise ignored. |
| `_use_cert_as_auth` (+ `_tls_client_cert_file`)     | `bool`               |   ✅   |   ❌   | `False`                   | Authenticate with a TLS client certificate instead of a token. Thrift-only.                                                                                    |
| `username` / `password`                             | `str`                |   ❌   |   ❌   | `None`                    | **Removed.** Basic auth is no longer supported; passing either raises `ValueError`.                                                                            |

## HTTP client, proxy, retries

> **Retry defaults are Thrift defaults.** The **Default Value** column lists the
> values the *Thrift* backend applies. For the ✅-Kernel retry rows
> (`_retry_stop_after_attempts_count` / `_duration`, `_retry_delay_min` /
> `_max`), `session.py` forwards each as `kwargs.get(...)` **with no fallback**,
> so when a caller omits one, `None` is passed and the kernel's Rust retry
> policy supplies **its own** default — which is not guaranteed to match the
> Thrift value shown here.

| Option                               | Type        | Thrift | Kernel | Default Value | Note                                                                                                                                            |
| ------------------------------------ | ----------- | :----: | :----: | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `_socket_timeout`                    | `float` (s) |   ✅   |   ❌   | `900`         | Socket send/recv/connect timeout. Not forwarded to the kernel, which manages its own request timeout.                                          |
| `_pool_connections`                  | `int`       |   ✅   |   ⚠️   | `10`          | Number of urllib3 connection pools. Configures the connector's shared Python HTTP client; the kernel's query transport is its own Rust stack.  |
| `_pool_maxsize`                      | `int`       |   ✅   |   ⚠️   | `20`          | Max connections per pool on the shared Python HTTP client. Same kernel caveat as `_pool_connections`.                                          |
| `_proxy_auth_method`                 | `str`       |   ✅   |   ⚠️   | `None`        | `basic` or `negotiate` (Kerberos). Applies to the shared Python HTTP client; not threaded to the kernel query transport. See [`docs/proxy.md`](docs/proxy.md). |
| `_retry_stop_after_attempts_count`   | `int`       |   ✅   |   ✅   | `30`          | Max attempts in a retry sequence. Bounded to `[1, 60]` on Thrift; forwarded to the kernel's retry policy.                                       |
| `_retry_stop_after_attempts_duration`| `float` (s) |   ✅   |   ✅   | `900`         | Max total wall-clock seconds spent retrying. Forwarded to the kernel.                                                                           |
| `_retry_delay_min`                   | `float` (s) |   ✅   |   ✅   | `1`           | Minimum backoff delay. Forwarded to the kernel.                                                                                                 |
| `_retry_delay_max`                   | `float` (s) |   ✅   |   ✅   | `60`          | Maximum backoff delay. Forwarded to the kernel.                                                                                                 |
| `_retry_delay_default`               | `float` (s) |   ✅   |   ❌   | `5`           | Delay used when a poll fails due to a TCP/OS error. Not forwarded — the kernel's backoff has no flat-default equivalent.                        |
| `_retry_dangerous_codes`             | `List[int]` |   ✅   |   ❌   | `[]`          | HTTP status codes for which even non-idempotent commands are retried. Thrift-only.                                                             |
| `_respect_server_retry_after_header` | `bool`      |   ✅   |   ❌   | `False`       | Honor the server's `Retry-After` header. Thrift-only.                                                                                           |
| `_retry_max_redirects`               | `int`       |   ✅   |   ❌   | `None`        | Max HTTP redirects to follow (must be ≤ `_retry_stop_after_attempts_count`). Thrift-only.                                                       |
| `_enable_v3_retries`                 | `bool`      |   ✅   |   ❌   | `True`        | Use the urllib3-based v3 retry policy; `False` selects the deprecated legacy policy. Thrift-only.                                               |

## TLS / SSL

> TLS options are assembled into a single `SSLOptions` object in `session.py`
> and passed to **every** backend, so they are honored on both Thrift and
> Kernel. Verification is **on by default**; you must pass `_tls_no_verify=True`
> to disable it.

| Option                          | Type  | Thrift | Kernel | Default Value | Note                                                                       |
| ------------------------------- | ----- | :----: | :----: | ------------- | -------------------------------------------------------------------------- |
| `_tls_no_verify`                | `bool`|   ✅   |   ✅   | `False`       | Disable all TLS verification (cert **and** hostname). Dangerous — testing. |
| `_tls_verify_hostname`          | `bool`|   ✅   |   ✅   | `True`        | Verify the server hostname matches the certificate (cert still verified).  |
| `_tls_trusted_ca_file`          | `str` |   ✅   |   ✅   | `None`        | Path to a CA bundle. Defaults to the system trust store.                   |
| `_tls_client_cert_file`         | `str` |   ✅   |   ✅   | `None`        | Client certificate for mutual TLS.                                         |
| `_tls_client_cert_key_file`     | `str` |   ✅   |   ✅   | `None`        | Private key for the client certificate.                                    |
| `_tls_client_cert_key_password` | `str` |   ✅   |   ✅   | `None`        | Password for an encrypted client-key file.                                 |

## Results & type rendering

| Option                                | Type   | Thrift | Kernel | Default Value | Note                                                                                                          |
| ------------------------------------- | ------ | :----: | :----: | ------------- | ------------------------------------------------------------------------------------------------------------- |
| `use_cloud_fetch`                     | `bool` |   ✅   |   ❌   | `True`        | Download large result sets in parallel from cloud storage. The kernel manages result transport internally.    |
| `max_download_threads`                | `int`  |   ✅   |   ❌   | `10`          | Worker threads for cloud-fetch downloads. Not forwarded to the kernel.                                        |
| `enable_query_result_lz4_compression` | `bool` |   ✅   |   ❌   | `True`        | LZ4-compress result payloads. Not forwarded; the kernel handles compression internally.                       |
| `_disable_pandas`                     | `bool` |   ✅   |   ❌   | `False`       | Skip the pandas-based Arrow deserialization path. Not forwarded to the kernel.                                |
| `_use_arrow_native_complex_types`     | `bool` |   ✅   |   ✅   | `True`        | Return `ARRAY`/`MAP`/`STRUCT` as native Arrow types instead of JSON strings. Forwarded to the kernel.         |
| `_use_arrow_native_decimals`          | `bool` |   ✅   |   ❌   | `True`        | Return `DECIMAL` as a native Arrow type instead of a string. Thrift-only.                                     |
| `_use_arrow_native_timestamps`        | `bool` |   ✅   |   ❌   | `True`        | Return `TIMESTAMP` as a native Arrow type instead of a string. Thrift-only.                                   |

## Session defaults & transactions

| Option                        | Type                              | Thrift | Kernel | Default Value | Note                                                                                                                       |
| ----------------------------- | --------------------------------- | :----: | :----: | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `session_configuration`       | `Dict[str, Any]`                  |   ✅   |   ✅   | `None`        | Spark/SQL session parameters (e.g. `{"ansi_mode": "true"}`). Delivered via `open_session` on both backends.                |
| `catalog`                     | `str`                             |   ✅   |   ✅   | `None`        | Initial catalog for the session (DBR 9.0+).                                                                                |
| `schema`                      | `str`                             |   ✅   |   ✅   | `None`        | Initial schema for the session (DBR 9.0+).                                                                                 |
| `query_tags`                  | `Dict[str, Optional[str]]`        |   ✅   |   ✅   | `None`        | Key/value tags serialized into the reserved `QUERY_TAGS` conf. On the kernel path they are applied per statement via `set_query_tags`.  |
| `enable_metric_view_metadata` | `bool`                            |   ✅   |   ✅   | `False`       | Sets `spark.sql.thriftserver.metadata.metricview.enabled` via session config so metric-view metadata surfaces.            |
| `use_inline_params`           | `bool` \| `"silent"`              |   ✅   |   ⚠️   | `False`       | Render parameters inline (legacy) vs. native bound params (DBR 14.1+). The kernel uses native binding; inline may differ.  |
| `ignore_transactions`         | `bool`                            |   ✅   |   ✅   | `True`        | When `True`: `commit()` is a no-op, `rollback()` raises `NotSupportedError`, and setting `autocommit` is a no-op.          |
| `fetch_autocommit_from_server`| `bool`                            |   ✅   |   ✅   | `False`       | Query the server (`SET AUTOCOMMIT`) for autocommit state instead of returning the cached value.                           |
| `staging_allowed_local_path`  | `str` \| `List[str]`              |   ✅   |   ❌   | `None`        | Local path(s) permitted for Unity Catalog Volume `PUT`/`GET`. **Thrift-only** — the kernel has no Volume API yet.          |

## Telemetry

All `*telemetry*` options live in the driver layer (the shared HTTP client and
`TelemetryClientFactory`), not inside either backend, so they are read
regardless of `use_kernel`.

| Option                             | Type   | Thrift | Kernel | Default Value | Note                                                     |
| ---------------------------------- | ------ | :----: | :----: | ------------- | -------------------------------------------------------- |
| `enable_telemetry`                 | `bool` |   ✅   |   ✅   | `True`        | Enable client telemetry (also gated by a server flag).   |
| `force_enable_telemetry`           | `bool` |   ✅   |   ✅   | `False`       | Force telemetry on regardless of the server-side flag.   |
| `telemetry_batch_size`             | `int`  |   ✅   |   ✅   | `100`         | Events buffered before a flush.                          |
| `_telemetry_circuit_breaker_enabled`| `bool`|   ✅   |   ✅   | `True`        | Enable the telemetry circuit breaker.                    |

> **Telemetry _events_ differ by backend.** The knobs above are backend-agnostic,
> but because the kernel owns result fetching internally it emits fewer
> per-statement / CloudFetch telemetry events than the Thrift path.

---

## Summary of gaps

### Supported on Thrift, missing / ignored on Kernel

1. Custom `credentials_provider` (rejected on the kernel path).
2. `experimental_oauth_persistence` (custom OAuth token store).
3. Azure service-principal / Entra ID OAuth (`azure_client_id`,
   `azure_client_secret`, `azure_tenant_id`, `azure_workspace_resource_id`).
4. TLS-client-cert *authentication* (`_use_cert_as_auth`) — note the TLS
   *transport* options (`_tls_*`) themselves **are** honored on both backends.
5. Result-transport tuning: `use_cloud_fetch`, `max_download_threads`,
   `enable_query_result_lz4_compression`, `_disable_pandas`.
6. Arrow-native rendering for `_use_arrow_native_decimals` /
   `_use_arrow_native_timestamps` (complex types **are** forwarded).
7. `staging_allowed_local_path` (Volume `PUT`/`GET`).
8. Retry fine-tuning beyond the four forwarded knobs: `_retry_delay_default`,
   `_retry_dangerous_codes`, `_respect_server_retry_after_header`,
   `_retry_max_redirects`, `_enable_v3_retries`.
9. `_socket_timeout`, `_port`, `_connection_uri`.

### Supported on Kernel, no Thrift public equivalent

None — the kernel's parameter surface is currently a subset of Thrift's.

### Behavioral divergences to watch

- **Connection pooling / proxy** (`_pool_connections`, `_pool_maxsize`,
  `_proxy_auth_method`) configure the connector's shared Python HTTP client
  (auth/telemetry); the kernel's query traffic uses its own Rust transport.
- **`use_inline_params`** renders parameters inline on Thrift; the kernel uses
  native parameter binding.

> All kernel-path behavior reflects the current **early-access** surface and is
> subject to change. Generated against connector version `4.4.0`; the source of
> truth is `Connection.__init__` (`src/databricks/sql/client.py`) and the
> per-backend clients under `src/databricks/sql/backend/`.
