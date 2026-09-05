# Connection parameters reference

This document lists **every recognized connection / session parameter** the Python
connector (`databricks.sql.connect(...)`) accepts, and — because the driver
ships more than one backend — whether each parameter is honored on the
**Thrift** backend (the default) or the **Kernel** backend (opt-in via
`use_kernel=True`).

The goal is to make protocol gaps explicit: a parameter honored on one backend
but ignored (or rejected) on the other is called out in the **Note** column.

> **Backend selection.** The connector defaults to Thrift. The **Kernel**
> backend — a native Rust core exposed via PyO3 — is selected with
> `use_kernel=True`; it requires **Python ≥ 3.10** and the
> `databricks-sql-connector[kernel]` extra, and is **early access** (its
> parameter surface is still landing and may change without notice). The
> session layer forwards only a *curated, named subset* of backend options to
> the kernel (`Session._create_backend`, `src/databricks/sql/session.py`) — it
> does **not** splat `**kwargs`. Options implemented in the shared Python driver
> layer can still apply to both backends; unforwarded backend-specific options
> are ignored or rejected on the kernel path.
>
> A separate pure-Python **SEA** backend (`use_sea=True`) also exists but is
> being deprecated. Its selector and SEA-only hybrid-disposition option are
> listed for completeness, but this reference does not provide a third SEA
> support column.

## Legend

| Symbol | Meaning                                                             |
| ------ | ------------------------------------------------------------------- |
| ✅     | Honored by the backend or by the shared driver layer.               |
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

## Backend selection

| Option                   | Type   | Thrift | Kernel | Default Value | Note                                                                                               |
| ------------------------ | ------ | :----: | :----: | ------------- | -------------------------------------------------------------------------------------------------- |
| `use_kernel`             | `bool` |   —    |   —    | `False`       | Routing selector: `False` selects Thrift; `True` selects Kernel. Mutually exclusive with `use_sea`. |
| `use_sea`                | `bool` |   —    |   —    | `False`       | Select the deprecated pure-Python SEA backend. Mutually exclusive with `use_kernel`.               |
| `use_hybrid_disposition` | `bool` |   ❌   |   ❌   | `False`       | SEA-only result-disposition option; ignored by Thrift and Kernel.                                  |
| `max_connections`        | `int`  |   ❌   |   ❌   | `10`          | Internal SEA-only HTTP-pool limit; ignored by Thrift and Kernel.                                   |

---

## Connection identity

| Option            | Type  | Thrift | Kernel | Default Value | Note                                                                                                                    |
| ----------------- | ----- | :----: | :----: | ------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `server_hostname` | `str` |   ✅   |   ✅   | — (required)  | Workspace hostname, e.g. `dbc-12345.cloud.databricks.com`.                                                              |
| `http_path`       | `str` |   ✅   |   ✅   | — (required)  | Thrift accepts a SQL-warehouse path **or** an all-purpose-cluster path; Kernel requires a warehouse/endpoint path.      |
| `_port`           | `int` |   ✅   |   —    | `443`         | TCP port (advanced). Not threaded to the kernel; it derives host/port from `server_hostname` + `http_path`.             |
| `_connection_uri` | `str` |   ✅   |   —    | `None`        | Thrift-only internal override of `server_hostname`/`http_path`. No kernel equivalent.                                   |
| `http_headers`    | `List[Tuple[str, str]]` | ✅ | ⚠️ | `None` | Extra headers sent on requests. Kernel filters `Authorization` and `x-databricks-org-id` because it manages them itself; other headers are forwarded. |
| `user_agent_entry`| `str` |   ✅   |   ✅   | `None`        | Custom tag folded into the composed `User-Agent` on both. (`_user_agent_entry` is a deprecated alias that warns.)       |

## Authentication

| Option                                              | Type                 | Thrift | Kernel | Default Value             | Note                                                                                                                                                            |
| --------------------------------------------------- | -------------------- | :----: | :----: | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `access_token` (PAT)                                | `str`                |   ✅   |   ✅   | `None`                    | Personal Access Token / bearer token. When no auth signal is supplied, Thrift falls back to Databricks OAuth U2M; Kernel requires an explicit U2M `auth_type` or another supported credential flow. |
| `auth_type`                                         | `str`                |   ✅   |   ✅   | `None`                    | `databricks-oauth` (U2M), `azure-oauth` (Azure AD U2M), or `azure-sp-m2m` (Azure service-principal M2M). All three work on both backends. Thrift treats an otherwise credential-less `None` as Databricks OAuth; Kernel does not implicitly select U2M. On Kernel, `azure-oauth` uses the same workspace-federated browser flow as `databricks-oauth`. |
| `oauth_client_id` (OAuth)                           | `str`                |   ✅   |   ✅   | built-in id for U2M       | Custom U2M client id on both backends. Kernel also uses it with `oauth_client_secret` or the JWT options for M2M; those M2M flows have no built-in client-id default. |
| `oauth_redirect_port` (U2M)                         | `int`                |   ✅   |   ✅   | `None`                    | Localhost redirect port for the browser flow. On **both** backends it is only honored when a custom `oauth_client_id` is also supplied — then that single port becomes the redirect URI. With the built-in client id (or when omitted) the connector uses the full registered range 8020–8024 and binds the first free port, so a bare `oauth_redirect_port` has no effect. (Thrift: `auth.py` `oauth_redirect_port_range`; Kernel: same logic, forwarded as `redirect_ports`.) |
| `oauth_client_secret` (OAuth M2M)                   | `str`                |   ❌   |   ✅   | `None`                    | **Kernel-only in practice.** The Thrift auth path never reads `oauth_client_secret`; use `credentials_provider` or an Azure service principal for M2M on Thrift. |
| `oauth_jwt_key_file` (OAuth M2M, JWT private key)   | `str`                |   ❌   |   ✅   | `None`                    | **Kernel-only.** Path to the PEM private key for JWT private-key M2M (RFC 7523 client assertion). Supplying it selects the JWT flow: the kernel signs a short-lived assertion with the key instead of sending a client secret. Requires `oauth_client_id` + `oauth_jwt_kid`; mutually exclusive with `oauth_client_secret` / `credentials_provider`. |
| `oauth_jwt_kid` (OAuth M2M, JWT private key)        | `str`                |   ❌   |   ✅   | `None`                    | **Kernel-only.** Key id written into the JWT header so the IdP can select the registered public key. Required with `oauth_jwt_key_file`. (For Entra ID this is the certificate's `x5t` thumbprint.) |
| `oauth_jwt_passphrase` (OAuth M2M, JWT private key) | `str`                |   ❌   |   ✅   | `None`                    | **Kernel-only.** Passphrase for an encrypted PKCS#8 private key; omit for an unencrypted key.                                                                    |
| `oauth_jwt_algorithm` (OAuth M2M, JWT private key)  | `str`                |   ❌   |   ✅   | `RS256`                   | **Kernel-only.** JWT signing algorithm (`RS256`/`384`/`512`, `PS256`/`384`/`512`, `ES256`, `ES384`).                                                            |
| `token_url` (OAuth M2M)                             | `str`                |   ❌   |   ✅   | `None` ⇒ OIDC discovery   | **Kernel-only.** OAuth IdP token endpoint override. Required for JWT M2M against an external-IdP-backed workspace (e.g. Entra ID for Azure Databricks), since Databricks-native OIDC does not advertise the `private_key_jwt` method. Applies to shared-secret M2M too. |
| `oauth_scopes`                                      | `List[str]`          |   ❌   |   ✅   | `["sql","offline_access"]` for U2M | **Thrift ignores custom scopes** — it always uses the built-in scope set. Kernel honors custom scopes for U2M and M2M; an omitted M2M value uses the kernel's own default. |
| `credentials_provider`                              | `CredentialsProvider`|   ✅   |   ❌   | `None`                    | Custom external credentials provider. **Rejected on the kernel path** (`NotSupportedError`) — it is an opaque token source, so the kernel cannot own the token lifecycle; use `oauth_client_id` + `oauth_client_secret` for M2M, or the Thrift backend. |
| `identity_federation_client_id`                     | `str`                |   ✅   |   ✅   | `None`                    | Workload identity / token-federation client id (kernel support added in #910).                                                                                 |
| `experimental_oauth_persistence`                    | `OAuthPersistence`   |   ✅   |   ❌   | `None`                    | **Thrift-only.** The kernel owns its own token lifecycle and does not accept a persistence store.                                                              |
| `oauth_token_cache_enabled`                         | `bool \| None`       |   ❌   |   ✅   | `None`                    | **Kernel-only, U2M-only.** Controls whether the kernel persists OAuth U2M refresh tokens to disk (AES-256 encrypted, in the OS config dir — `~/Library/Application Support/databricks-sql-kernel/oauth/` on macOS, `~/.config/databricks-sql-kernel/oauth/` on Linux; requires databricks-sql-kernel PR #283). **Disabled by default:** when unset (None) or False, the connector disables on-disk persistence (tokens in-memory only, matching Thrift); True enables the cache. Omitting it does **not** inherit the kernel's enabled-by-default. Distinct from `experimental_oauth_persistence` — this toggles the kernel's built-in encrypted storage, not a pluggable callback.  |
| `azure_client_id` / `azure_client_secret` / `azure_tenant_id` | `str` | ✅ | ✅ | `None` | Azure service-principal (Entra ID M2M), selected by `auth_type="azure-sp-m2m"`. On the kernel path the connector forwards these to the kernel, which owns Azure resolution (Entra v2.0 token endpoint + the Databricks-resource `.default` scope) (#919). **`azure_tenant_id` is optional on the kernel path too** — like Thrift, the kernel auto-discovers it from the workspace's `/aad/auth` redirect when omitted. |
| `azure_workspace_resource_id`                       | `str`                |   ✅   |   ✅   | `None`                    | For `azure-sp-m2m`. When set, the SP **management token** (`X-Databricks-Azure-SP-Management-Token`) + `X-Databricks-Azure-Workspace-Resource-Id` header are sent, to authorize an SP that has an Azure RBAC role but is not a workspace member. Omit it for a workspace-member SP (the data token authenticates alone; no management token is fetched). Works on both the kernel and Thrift paths. |
| `_use_cert_as_auth` (+ `_tls_client_cert_file`)     | `bool`               |   ✅   |   ❌   | `False`                   | Authenticate with a TLS client certificate instead of a token. Thrift-only.                                                                                    |
| `username` / `password`                             | `str`                |   ❌   |   ❌   | `None`                    | **Removed.** Basic auth is no longer supported. On **Thrift**, passing either raises `ValueError`; on the **kernel** path it is silently ignored (the Thrift auth provider that raises is never built).                                                                            |

## HTTP client, proxy, retries

> **Retry defaults are Thrift defaults.** The **Default Value** column lists the
> values the *Thrift* backend applies. For the Kernel-supported retry rows
> (`_retry_stop_after_attempts_count` / `_duration`, `_retry_delay_min` /
> `_max`), `session.py` forwards each as `kwargs.get(...)` **with no fallback**,
> so when a caller omits one, `None` is passed and the kernel's Rust retry
> policy supplies **its own** default — which is not guaranteed to match the
> Thrift value shown here.

| Option                               | Type        | Thrift | Kernel | Default Value | Note                                                                                                                                            |
| ------------------------------------ | ----------- | :----: | :----: | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `_socket_timeout`                    | `float` (s) |   ✅   |   ✅   | `900` (Thrift); `120` (kernel) | Thrift: socket send/recv/connect timeout. Kernel: total HTTP request deadline from connect through response-body completion. A positive value is forwarded; unset or `0` selects the kernel's 120s default. On the kernel path, `0` is neither unlimited nor an immediate timeout. |
| `_pool_connections`                  | `int`       |   ⚠️   |   ❌   | `10`          | Number of pools in the shared Python HTTP client. On Thrift this affects auxiliary auth/feature-flag/telemetry traffic, not the separate Thrift query transport. Kernel-owned network traffic does not use this client. |
| `_pool_maxsize`                      | `int`       |   ⚠️   |   ⚠️   | `20` (Python); `100` (kernel when unset) | Max idle connections retained per host. It configures the shared Python client but not the Thrift query transport. With kernel ≥ 1.1.0, a positive value also configures the kernel's Rust HTTP pool. Unset or `0` keeps each client's default. |
| `_proxy_auth_method`                 | `str`       |   ✅   |   ❌   | `None`        | `basic` or `negotiate` (Kerberos). Applied to both Thrift query traffic and the shared Python client. It is not forwarded to the kernel's Rust transport. See [`docs/proxy.md`](docs/proxy.md). |
| `_retry_stop_after_attempts_count`   | `int`       |   ✅   |   ✅   | `30`          | Max attempts in a retry sequence. Bounded to `[1, 60]` on Thrift; forwarded to the kernel's retry policy.                                       |
| `_retry_stop_after_attempts_duration`| `float` (s) |   ✅   |   ⚠️   | `900`         | Max total wall-clock seconds spent retrying. Kernel rounds to whole seconds.                                                                    |
| `_retry_delay_min`                   | `float` (s) |   ✅   |   ⚠️   | `1`           | Minimum backoff delay. Kernel rounds to whole seconds and floors a positive sub-second value at one second.                                     |
| `_retry_delay_max`                   | `float` (s) |   ✅   |   ⚠️   | `60`          | Maximum backoff delay. Kernel rounds to whole seconds.                                                                                           |
| `_retry_delay_default`               | `float` (s) |   ✅   |   ❌   | `5`           | Delay used when a poll fails due to a TCP/OS error. Not forwarded — the kernel's backoff has no flat-default equivalent.                        |
| `_retry_dangerous_codes`             | `List[int]` |   ✅   |   ❌   | `[]`          | HTTP status codes for which even non-idempotent commands are retried. Thrift-only.                                                             |
| `_respect_server_retry_after_header` | `bool`      |   ✅   |   ❌   | `False`       | Honor the server's `Retry-After` header. Thrift-only.                                                                                           |
| `_retry_max_redirects`               | `int`       |   ✅   |   ❌   | `None`        | Max HTTP redirects to follow (must be ≤ `_retry_stop_after_attempts_count`). Thrift-only.                                                       |
| `_enable_v3_retries`                 | `bool`      |   ✅   |   ❌   | `True`        | Use the urllib3-based v3 retry policy; `False` selects the deprecated legacy policy. Thrift-only.                                               |

## TLS / SSL

> TLS options are assembled into a single `SSLOptions` object in `session.py`
> and passed to **every** backend, so they are honored on both Thrift and
> Kernel — with one exception: `_tls_client_cert_key_password` is **not**
> supported on the kernel path (see below). Verification is **on by default**;
> you must pass `_tls_no_verify=True` to disable it.

| Option                          | Type  | Thrift | Kernel | Default Value | Note                                                                       |
| ------------------------------- | ----- | :----: | :----: | ------------- | -------------------------------------------------------------------------- |
| `_tls_no_verify`                | `bool`|   ✅   |   ✅   | `False`       | Disable all TLS verification (cert **and** hostname). Dangerous — testing. |
| `_tls_verify_hostname`          | `bool`|   ✅   |   ✅   | `True`        | Verify the server hostname matches the certificate (cert still verified).  |
| `_tls_trusted_ca_file`          | `str` |   ✅   |   ✅   | `None`        | Path to a CA bundle. Defaults to the system trust store.                   |
| `_tls_client_cert_file`         | `str` |   ✅   |   ✅   | `None`        | Client certificate for mutual TLS.                                         |
| `_tls_client_cert_key_file`     | `str` |   ✅   |   ✅   | `None`        | Private key for the client certificate.                                    |
| `_tls_client_cert_key_password` | `str` |   ✅   |   ❌   | `None`        | Password for an encrypted client-key file. On the kernel path this is rejected with `NotSupportedError` **only when mTLS is configured** (i.e. `_tls_client_cert_file` is also set); without mTLS it is ignored. The kernel has no surface for an encrypted client key today — pass an unencrypted PEM key, or use the Thrift backend. |

## Results & type rendering

| Option                                | Type   | Thrift | Kernel | Default Value | Note                                                                                                          |
| ------------------------------------- | ------ | :----: | :----: | ------------- | ------------------------------------------------------------------------------------------------------------- |
| `use_cloud_fetch`                     | `bool` |   ✅   |   ❌   | `True`        | Download large result sets in parallel from cloud storage. The kernel manages result transport internally.    |
| `max_download_threads`                | `int`  |   ✅   |   ❌   | `10`          | Worker threads for cloud-fetch downloads. Not forwarded to the kernel.                                        |
| `enable_query_result_lz4_compression` | `bool` |   ✅   |   ❌   | `True`        | LZ4-compress result payloads. Not forwarded; the kernel handles compression internally.                       |
| `_disable_pandas`                     | `bool` |   ✅   |   ✅   | `False`       | Skip the pandas-based Arrow→row deserialization and materialize rows directly with PyArrow. This is a **Python-side** result-conversion toggle, not a wire option: the kernel returns results as Arrow (`RecordBatch`es) and the connector runs the *same* `_convert_arrow_table` for both backends, so the flag is honored on the kernel path too. Affects only row fetches (`fetchone`/`fetchmany`/`fetchall`); the `fetch*_arrow` methods return the Arrow table unchanged regardless of this flag. |
| `_use_arrow_native_complex_types`     | `bool` |   ✅   |   ✅   | `True`        | Return `ARRAY`/`MAP`/`STRUCT` as native Arrow types instead of JSON strings. Forwarded to the kernel.         |
| `_use_arrow_native_decimals`          | `bool` |   ✅   |   ❌   | `True`        | Thrift wire encoding for `DECIMAL`: `True` → native Arrow `decimal128`, `False` → Arrow string. **No value-level effect**, though: the connector unconditionally re-casts the column back to `decimal128` (`convert_decimals_in_arrow_table`, `thrift_backend.py`), so both `fetchall()` and `fetchall_arrow()` yield `Decimal` / `decimal128(p,s)` either way (verified live). Not forwarded to the kernel, which always returns native Arrow decimals. |
| `_use_arrow_native_timestamps`        | `bool` |   ✅   |   ❌   | `True`        | Thrift wire encoding for `TIMESTAMP`: `True` → native Arrow timestamp (→ Python `datetime`), `False` → Arrow string (→ Python **`str`**). **Unlike decimals there is no re-cast**, so `False` genuinely surfaces strings — and `cursor.description` still reports the type code as `'timestamp'`, a mismatch to watch for (verified live). Note the connector always also sends the `spark.thriftserver.arrowBasedRowSet.timestampAsString=false` conf, but the `timestampAsArrow=False` flag wins. Not forwarded to the kernel, which always returns native Arrow timestamps. |

## Session defaults & transactions

| Option                        | Type                              | Thrift | Kernel | Default Value | Note                                                                                                                       |
| ----------------------------- | --------------------------------- | :----: | :----: | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `session_configuration`       | `Dict[str, Any]`                  |   ✅   |   ✅   | `None`        | Spark/SQL session parameters (e.g. `{"ansi_mode": "true"}`). Delivered via `open_session` on both backends.                |
| `catalog`                     | `str`                             |   ✅   |   ✅   | `None`        | Initial catalog for the session (DBR 9.0+).                                                                                |
| `schema`                      | `str`                             |   ✅   |   ✅   | `None`        | Initial schema for the session (DBR 9.0+).                                                                                 |
| `query_tags`                  | `Dict[str, Optional[str]]`        |   ✅   |   ✅   | `None`        | Connection-level key/value tags serialized into the reserved `QUERY_TAGS` session configuration on both backends. Cursor-level `query_tags` are applied per statement separately. |
| `enable_metric_view_metadata` | `bool`                            |   ✅   |   ✅   | `False`       | Sets `spark.sql.thriftserver.metadata.metricview.enabled` via session config so metric-view metadata surfaces.            |
| `use_inline_params`           | `bool` \| `"silent"`              |   ✅   |   ✅   | `False`       | Render parameters inline (legacy) vs. native bound params (DBR 14.1+). Shared cursor code performs the rendering before dispatch, so both backends honor it identically. |
| `ignore_transactions`         | `bool`                            |   ✅   |   ✅   | `True`        | When `True`: `commit()` is a no-op, `rollback()` raises `NotSupportedError`, and setting `autocommit` is a no-op.          |
| `fetch_autocommit_from_server`| `bool`                            |   ✅   |   ✅   | `False`       | Query the server (`SET AUTOCOMMIT`) for autocommit state instead of returning the cached value.                           |
| `staging_allowed_local_path`  | `str` \| `List[str]`              |   ✅   |   ❌   | `None`        | Local path(s) permitted for Unity Catalog Volume `PUT`/`GET`. **Thrift-only** — the kernel has no Volume API yet.          |

## Telemetry

Thrift telemetry is owned by the Python driver and normally gated by the
`enableTelemetryForPythonDriver` server feature flag. Kernel telemetry is owned
by the Rust kernel: explicit `enable_telemetry`, `telemetry_batch_size`, and
`_telemetry_circuit_breaker_enabled` values are forwarded when supported by the
installed kernel binding. `force_enable_telemetry` remains Python-driver-only.

| Option                             | Type   | Thrift | Kernel | Default Value | Note                                                     |
| ---------------------------------- | ------ | :----: | :----: | ------------- | -------------------------------------------------------- |
| `enable_telemetry`                 | `bool` |   ✅   |   ✅   | `True` (Thrift); kernel-defined when unset | Thrift is also gated by the server flag. An explicit Kernel value is forwarded without that Python-side gate. |
| `force_enable_telemetry`           | `bool` |   ✅   |   ❌   | `False`       | Bypass the server-side feature flag on Thrift. Not forwarded to Kernel. |
| `telemetry_batch_size`             | `int`  |   ✅   |   ✅   | `100`         | Events buffered before a flush.                          |
| `_telemetry_circuit_breaker_enabled`| `bool`|   ✅   |   ✅   | effective `False` (Thrift); kernel-defined when unset | The `ClientContext` signature defaults to `True`, but `connect()` passes `None` when this option is omitted and `bool(None)` makes the effective Thrift default `False`. An explicit value is forwarded to Kernel. |

> **Telemetry _events_ differ by backend.** Because the kernel owns result
> fetching and telemetry internally, it emits fewer
> per-statement / CloudFetch telemetry events than the Thrift path.

---

## Summary of gaps

### Supported on Thrift, missing / ignored on Kernel

1. Custom `credentials_provider` (rejected on the kernel path).
2. `experimental_oauth_persistence` (custom OAuth token store).
3. TLS-client-cert *authentication* (`_use_cert_as_auth`) — note the TLS
   *transport* options (`_tls_*`) themselves **are** honored on both backends.
4. Result-transport tuning: `use_cloud_fetch`, `max_download_threads`,
   `enable_query_result_lz4_compression`.
5. Arrow-native rendering for `_use_arrow_native_decimals` /
   `_use_arrow_native_timestamps` (complex types **are** forwarded).
6. `staging_allowed_local_path` (Volume `PUT`/`GET`).
7. Retry fine-tuning beyond the four forwarded knobs: `_retry_delay_default`,
   `_retry_dangerous_codes`, `_respect_server_retry_after_header`,
   `_retry_max_redirects`, `_enable_v3_retries`.
8. `force_enable_telemetry`.
9. `_port` and `_connection_uri`.
10. `_pool_connections` and `_proxy_auth_method` do not configure the
    kernel-owned network stack.

### Supported on Kernel, missing / ignored on Thrift

1. Kernel-managed OAuth M2M: `oauth_client_secret`, `oauth_jwt_key_file`,
   `oauth_jwt_kid`, `oauth_jwt_passphrase`, `oauth_jwt_algorithm`, and
   `token_url`.
2. Custom `oauth_scopes`.
3. Kernel U2M encrypted token storage (`oauth_token_cache_enabled`).

### Behavioral divergences to watch

- **Authentication default**: a credential-less Thrift connection defaults to
  Databricks OAuth U2M. Kernel requires an explicit U2M `auth_type` or another
  supported credential flow.
- **Connection pooling / proxy**: `_pool_connections` configures only the
  shared Python client and `_proxy_auth_method` is not forwarded to Kernel.
  `_pool_maxsize` also configures the kernel's Rust HTTP pool when positive and
  using kernel ≥ 1.1.0. A value of `0` is treated as unset: the Python client
  keeps 20 and the kernel keeps 100.
- **Custom headers**: Kernel drops `Authorization` and
  `x-databricks-org-id`; it forwards other `http_headers`.
- **Telemetry**: Kernel owns its telemetry lifecycle. It does not honor
  `force_enable_telemetry`, and an omitted `enable_telemetry` or circuit-breaker
  setting uses the kernel's default.

> All kernel-path behavior reflects the current **early-access** surface and is
> subject to change. Generated against connector version `4.5.0`; the source of
> truth is `Connection.__init__` (`src/databricks/sql/client.py`) and the
> per-backend clients under `src/databricks/sql/backend/`.
