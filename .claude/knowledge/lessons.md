# Lessons

<!-- distilled-through: none -->

## Backend contracts and configuration
- L-0001 (1×, log:2026-08-13): When changing cursor.description type_code strings, update every downstream conversion branch that matches them.
- L-0002 (1×, #560): Recover Spark SQL types sharing a Thrift TTypeId from Arrow field Spark:DataType:SqlName metadata.
- L-0003 (4×, #910 #921 #919): New resolver branches must preserve mutual-exclusion checks and forward shared options on every applicable path; directly test forwarding even when omission has a safe default.
- L-0004 (2×, #916 #922): Use Thrift as the reference when checking kernel value semantics; deprecated SEA behavior can differ.
- L-0005 (1×, #914): OAuth redirect ports and client IDs are coupled. Override the default port range only when the custom client and port are both supplied.
- L-0006 (3×, #914 #931 #933): Document backend-specific defaults, sentinels, filter normalization and deliberate parity differences in code, API docs and changelog.
- L-0007 (2×, #913 #932): Kernel options take effect only when _create_backend forwards them. Explicitly pass connector defaults when they differ from native defaults.
- L-0008 (2×, #920): Public connect-parameter deprecations use logger.warning. Recommend kernel where supported; SEA remains necessary for RT/Lakehouse warehouse types that reject Thrift.
- L-0009 (2×, #922 #919): When delegating normalization to the kernel/server, retain boundary tests for zero, negative and None values and remove obsolete arguments, imports and documentation.
- L-0010 (1×, #932): Enable typed Optional[bool] options only on explicit True; truthy strings such as False or 0 must not accidentally enable a feature.
- L-0011 (6×, #932 #925 #940): Gate optional Session kwargs through _kernel_session_accepts_kwarg and fail closed on introspection failure. Default-off options must preserve older wheels; explicit requirements need a released dependency floor and accurate support docs.
- L-0012 (2×, #930): Verify flags against materialized values and cursor.description on each backend. Shared client-side conversion toggles differ from options handled only by a backend.
- L-0013 (2×, #925 #933 #940): Test PyO3 kwarg filtering with fixed-signature fakes or real wheels; permissive MagicMock kwargs hide incompatible argument forwarding.
- L-0014 (1×, #925): Paths mirroring a routing decision must reuse its predicate exactly; truthiness and identity checks can disagree for non-bool inputs.
- L-0015 (1×, #948): Preserve Thrift's implicit OAuth default on kernel routing only when no explicit credential shape already determines the auth flow.
- L-0016 (1×, #948): After retries or fallback mutate kwargs, attribute failures and cleanup to the backend object that actually failed.
- L-0017 (1×, #948): Scope recovery-marker error translation to the RPC the recovery layer handles; unrelated calls must retain meaningful errors.
- L-0018 (1×, #948): Anchor path extractors to valid path shapes; leading greedy wildcards can accept malformed IDs and contaminate cache keys.
- L-0019 (1×, #951): TLS certfile may contain both certificate and key; reject keyfile without certfile. Validate each identity file separately for readable, non-empty contents before load_cert_chain.
- L-0020 (1×, #974): Before replacing permissive wire parsing with typed parsing, verify real server representations and test case, quoting and numeric/bool variants.
- L-0021 (1×, #985): SQL LIKE escape handling depends on preceding backslash-run parity; a one-character negative lookbehind cannot establish JDBC equivalence.
- L-0022 (1×, #1026): Reject bool explicitly when validating integer options before native conversion, so True/False cannot silently become 1/0.

## Results and lifecycle
- L-0023 (2×, #1019): Verify server metadata and forwarded configuration behavior with live tests and an opposite-setting positive control; fabricated metadata or forwarding-only unit tests cannot establish server behavior.
- L-0024 (2×, #923): Keep the original async owning handle for telemetry-producing polls/results. Reserve retained handles against concurrent use and use attach-by-id for resume/refetch or concurrent fallback.
- L-0025 (2×, #907): Lazy imports must preserve every formerly exported name and patch seam. Import-isolation subprocess tests need distinct success/error sentinels so import crashes cannot satisfy assertions.
- L-0026 (1×, #975): Dependency upgrades may tighten size/depth defaults; explicitly set relied-on limits and test them.
- L-0027 (1×, #990): Pair result columns with Arrow fields by position to preserve duplicate aliases; use names only as a documented malformed/short-schema fallback.
- L-0028 (2×, #995): Use plain concat_tables when chunk schemas match, preserving valid duplicate column names. Enable promote/promote_options only for genuinely differing schemas.
- L-0029 (1×, log:2026-10-30): Preserve NaN, NULL and positive/negative infinity distinctly through pandas conversion, comparing nullable-dtype behavior with the as_py path.
- L-0030 (1×, #1013): When broadening rendering input types, explicitly test formerly rejected strings, bytes, sets and empty containers so loud failures cannot become silently wrong values.
- L-0031 (1×, #1017): Enforce runtime Optional-state invariants with descriptive driver errors; Python -O removes assert statements.
- L-0032 (1×, #1022): KERNEL_REV changes CI source only. Shipping a kernel fix requires a released wheel and matching packaging floor/lock update, plus an in-repo E2E regression.
- L-0033 (1×, #1020): Prove optional C acceleration actually executes and skip when unavailable. Choose input/output protocols separately and verify encoded bytes and nesting limits against the fallback.
- L-0034 (1×, #1016): Carry sync result metadata into async get_execution_result. Read numModifiedRows from the existing terminal FINISHED_STATE status response via the shared helper, avoiding an extra RPC.

## Logging and concurrency
- L-0035 (2×, #924): pyo3-log caches effective logger levels. Reset its cache after relevant level changes/session setup and isolate cached state in tests.
- L-0036 (3×, #947): Logging filters that rewrite record.args must separately handle None, positional tuples and mapping dictionaries, with coverage for all three.
- L-0037 (1×, #974): Keep shared locks out of blocking network I/O; coordinate in-flight work without serializing unrelated callers on a slow fetch.
- L-0038 (1×, #974): Do not retain singleton executors/clients that other paths can destroy and replace; resolve them at use time or handle shutdown races.
- L-0039 (1×, #1020): Log large payloads with lazy percent-format arguments; eager f-strings/format calls consume CPU and memory even when the logger is disabled.
- L-0040 (1×, #1055): urllib3 pool mocks must route both _get_conn and _put_conn by test thread. Mock close returning None and prove isolation with concurrent users of one shared pool.
- L-0041 (1×, #1024): Reset process-global caches with autouse fixtures in tests that exercise them; each test must start cold.

## Verification and bot workflow
- L-0042 (1×, #905 #906): Bot MODEL_ENDPOINT must name the concrete model serving-endpoints/model/invocations URL; the anthropic/invocations form produces an invalid translated CLI path.
- L-0043 (3×, #930): Keep planning read-only, structured output concise, and PR summaries aligned with the complete final diff.
- L-0044 (1×, log:2026-10-30): Performance tests should count calls to the real expensive operation through a spy and assert deterministic bounds instead of live wall-clock timings.
- L-0045 (1×, #1004): Recheck capability claims after tool upgrades; replacing version numbers inside old support statements does not validate them.
- L-0046 (1×, #1013): Confirm targeted regression tests fail when their guarded check is removed; inject inputs where earlier sanitization cannot neutralize the case.
- L-0047 (1×, #1045): Secret-bearing CI steps must run Python isolated or from a neutral directory, preventing checkout modules from shadowing trusted imports.
