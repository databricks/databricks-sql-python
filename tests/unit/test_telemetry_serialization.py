"""Telemetry JSON must be byte-for-byte what the asdict-based serializer produced."""

import itertools
import json
import random
from dataclasses import asdict
from enum import Enum, IntEnum
from unittest.mock import MagicMock, patch

import pytest

from databricks.sql.auth.authenticators import AccessTokenAuthProvider
from databricks.sql.telemetry.models.endpoint_models import (
    TelemetryRequest,
    TelemetryResponse,
)
from databricks.sql.telemetry.models.enums import (
    AuthFlow,
    AuthMech,
    DatabricksClientType,
    DriverVolumeOperationType,
    ExecutionResultFormat,
    StatementType,
)
from databricks.sql.telemetry.models.event import (
    ChunkDetails,
    DriverConnectionParameters,
    DriverErrorInfo,
    DriverSystemConfiguration,
    DriverVolumeOperation,
    HostDetails,
    OperationDetail,
    ResultLatency,
    SqlExecutionEvent,
    TelemetryEvent,
)
from databricks.sql.telemetry.models.frontend_logs import (
    FrontendLogContext,
    FrontendLogEntry,
    TelemetryClientContext,
    TelemetryFrontendLog,
)
from databricks.sql.telemetry.telemetry_client import (
    TelemetryClient,
    TelemetryClientFactory,
    TelemetryHelper,
)
from databricks.sql.telemetry.utils import EnumEncoder


def main_to_json(obj):
    """JsonSerializableMixin.to_json as it was before this change, kept as the oracle."""
    return json.dumps(
        asdict(obj, dict_factory=lambda data: {k: v for k, v in data if v is not None}),
        cls=EnumEncoder,
    )


class _StrEnum(str, Enum):
    A = "str-mixin"


class _IntEnum(IntEnum):
    ONE = 1


TEXTS = [
    "",
    "plain",
    "é ü ß ñ",
    "日本語のテキスト",
    "emoji \U0001f600 \U0001f9ea",
    'quote " backslash \\ slash /',
    "control \n\t\r\x00\x1f\x7f",
    "lone surrogate \ud800",
]
FLOATS = [0.0, -0.0, 0.1, 1.5, 2.5e-5, 5e-324, 1e300, float("inf"), float("-inf")]
FLOATS += [float("nan")]
INTS = [0, -1, 7, 2**31, 2**63 + 1]
SCALARS = TEXTS + FLOATS + INTS + [True, False]


def _params(**overrides):
    values = dict(
        http_path="/sql/1.0/warehouses/abc",
        mode=DatabricksClientType.THRIFT,
        host_info=HostDetails(host_url="adb-1.azuredatabricks.net", port=443),
    )
    values.update(overrides)
    return DriverConnectionParameters(**values)


FULL_PARAMS = _params(
    mode=DatabricksClientType.SEA,
    auth_mech=AuthMech.OAUTH,
    auth_flow=AuthFlow.CLIENT_CREDENTIALS,
    socket_timeout=900.5,
    azure_workspace_resource_id="/subscriptions/é/resourceGroups/日本",
    azure_tenant_id="",
    use_proxy=True,
    use_system_proxy=False,
    proxy_host_info=HostDetails(host_url="proxy.example.com", port=8080),
    use_cf_proxy=False,
    cf_proxy_host_info=HostDetails(host_url="cf.example.com", port=0),
    non_proxy_hosts=["localhost", None, "127.0.0.1"],
    allow_self_signed_support=False,
    use_system_trust_store=True,
    enable_arrow=True,
    enable_direct_results=True,
    enable_sea_hybrid_results=False,
    http_connection_pool_size=10,
    rows_fetched_per_block=100000,
    async_poll_interval_millis=2000,
    support_many_parameters=True,
    enable_complex_datatype_support=False,
    allowed_volume_ingestion_paths=["/tmp/a", "/Volumes/é"],
    query_tags='{"team": "data-ēng"}',
)

FULL_SQL_EVENT = SqlExecutionEvent(
    statement_type=StatementType.METADATA,
    is_compressed=False,
    execution_result=ExecutionResultFormat.EXTERNAL_LINKS,
    retry_count=0,
    chunk_id=3,
    chunk_details=ChunkDetails(12, 340, 5, 4, 0),
    result_latency=ResultLatency(10, None),
    operation_detail=OperationDetail(0, 1, "EXECUTE_STATEMENT", False),
)


def _model_cases():
    sys_config = TelemetryHelper.get_driver_system_configuration()
    event = TelemetryEvent(
        system_configuration=sys_config,
        driver_connection_params=FULL_PARAMS,
        session_id="01f0-session",
        sql_statement_id="01f0-statement",
        auth_type="pat ☃",
        vol_operation=DriverVolumeOperation(DriverVolumeOperationType.PUT, "/V/ü"),
        sql_operation=FULL_SQL_EVENT,
        error_info=DriverErrorInfo("Error", 'Traceback\n  "quoted" é ☃'),
        operation_latency_ms=0,
    )
    log = TelemetryFrontendLog(
        frontend_log_event_id="5f0c-event",
        context=FrontendLogContext(
            client_context=TelemetryClientContext(
                timestamp_millis=1794470000000, user_agent="PyDatabricksSqlConnector/é"
            )
        ),
        entry=FrontendLogEntry(sql_driver_log=event),
        workspace_id=6436897454825492,
    )
    cases = {
        "host": HostDetails(host_url="h", port=1),
        "params_minimal": _params(),
        "params_full": FULL_PARAMS,
        "system_config": sys_config,
        "system_config_exotic": DriverSystemConfiguration(
            *(TEXTS * 2)[1:10], client_app_name=None, locale_name="ja_JP"
        ),
        "error_info": DriverErrorInfo("", ""),
        "chunk_details_empty": ChunkDetails(),
        "result_latency_empty": ResultLatency(),
        "operation_detail_empty": OperationDetail(),
        "sql_event_full": FULL_SQL_EVENT,
        "telemetry_event_full": event,
        "telemetry_event_minimal": TelemetryEvent(
            system_configuration=sys_config, driver_connection_params=_params()
        ),
        "frontend_log": log,
        "request": TelemetryRequest(
            uploadTime=1, items=["x", "ü"], protoLogs=[main_to_json(log)] * 3
        ),
        "request_no_logs": TelemetryRequest(uploadTime=0, items=[], protoLogs=None),
        "response": TelemetryResponse(
            errors=["é"], numSuccess=1, numProtoSuccess=2, numRealtimeSuccess=0
        ),
        "containers": _params(
            non_proxy_hosts=[
                HostDetails(host_url="n", port=2),
                AuthMech.PAT,
                {"k": AuthFlow.TOKEN_PASSTHROUGH, "none": None, 1: "int key"},
                (1, 2.5, None),
                [[], {}],
                _StrEnum.A,
                _IntEnum.ONE,
            ]
        ),
    }
    for vol_type in DriverVolumeOperationType:
        cases[f"volume_{vol_type.value}"] = DriverVolumeOperation(vol_type, "/V")
    for i, value in enumerate(SCALARS):
        cases[f"scalar_{i}"] = _params(
            socket_timeout=value, query_tags=value, http_path=value
        )
    return cases


MODEL_CASES = _model_cases()


@pytest.mark.parametrize("name", list(MODEL_CASES))
def test_models_match_main_serializer(name):
    obj = MODEL_CASES[name]
    assert obj.to_json() == main_to_json(obj)


def test_unserializable_values_fail_like_main():
    for bad in ({1, 2}, object(), {AuthMech.PAT: "enum key"}):
        obj = _params(non_proxy_hosts=[bad])
        with pytest.raises(TypeError):
            main_to_json(obj)
        with pytest.raises(TypeError):
            obj.to_json()


@pytest.fixture
def client():
    executor = MagicMock()
    with patch(
        "databricks.sql.common.unified_http_client.UnifiedHttpClient._setup_pool_managers"
    ):
        return TelemetryClient(
            telemetry_enabled=True,
            session_id_hex="01f0-session",
            auth_provider=AccessTokenAuthProvider("token"),
            host_url="adb-1.azuredatabricks.net",
            executor=executor,
            batch_size=TelemetryClientFactory.DEFAULT_BATCH_SIZE,
            client_context=MagicMock(),
        )


def test_client_events_and_flush_payload_match_main_serializer(client):
    events = []
    with patch.object(client, "_export_event", side_effect=events.append):
        client.export_initial_telemetry_log(_params(), "PyDatabricksSqlConnector/4.9")
        client.export_initial_telemetry_log(FULL_PARAMS, "agent é ☃", "01f0-other")
        client.export_failure_log("OperationalError", "Traceback\n  é \U0001f600")
        for statement_type, result_format in itertools.product(
            StatementType, ExecutionResultFormat
        ):
            client.export_latency_log(
                latency_ms=52,
                sql_execution_event=SqlExecutionEvent(
                    statement_type=statement_type,
                    is_compressed=True,
                    execution_result=result_format,
                    retry_count=1,
                    chunk_id=None,
                ),
                sql_statement_id="01f0-statement",
            )
        client.export_latency_log(0, FULL_SQL_EVENT, None, session_id="01f0-x")
    assert len(events) == 4 + len(StatementType) * len(ExecutionResultFormat)
    for event in events:
        assert event.to_json() == main_to_json(event)

    with patch(
        "databricks.sql.telemetry.telemetry_client.time.time", return_value=1794.47
    ):
        client._send_telemetry(events)
    sent = client._executor.submit.call_args.kwargs["data"]
    expected = TelemetryRequest(
        uploadTime=1794470,
        items=[],
        protoLogs=[main_to_json(event) for event in events],
    )
    assert sent == main_to_json(expected)


def test_randomized_events_match_main_serializer():
    rnd = random.Random(1037)

    def maybe(value):
        return None if rnd.random() < 0.3 else value

    def scalar():
        return rnd.choice(SCALARS)

    for _ in range(300):
        params = _params(
            mode=rnd.choice(list(DatabricksClientType)),
            host_info=HostDetails(host_url=rnd.choice(TEXTS), port=rnd.choice(INTS)),
            auth_mech=maybe(rnd.choice(list(AuthMech))),
            auth_flow=maybe(rnd.choice(list(AuthFlow))),
            socket_timeout=maybe(scalar()),
            proxy_host_info=maybe(HostDetails(host_url=scalar(), port=scalar())),
            non_proxy_hosts=maybe([maybe(scalar()) for _ in range(rnd.randint(0, 3))]),
            use_proxy=maybe(rnd.random() < 0.5),
            query_tags=maybe(scalar()),
        )
        sql_event = SqlExecutionEvent(
            statement_type=rnd.choice(list(StatementType)),
            is_compressed=rnd.random() < 0.5,
            execution_result=rnd.choice(list(ExecutionResultFormat)),
            retry_count=maybe(scalar()),
            chunk_id=maybe(scalar()),
            chunk_details=maybe(ChunkDetails(*(maybe(scalar()) for _ in range(5)))),
            result_latency=maybe(ResultLatency(maybe(scalar()), maybe(scalar()))),
            operation_detail=maybe(
                OperationDetail(maybe(scalar()), None, maybe(scalar()), maybe(True))
            ),
        )
        log = TelemetryFrontendLog(
            frontend_log_event_id=scalar(),
            context=FrontendLogContext(
                client_context=TelemetryClientContext(scalar(), scalar())
            ),
            entry=FrontendLogEntry(
                sql_driver_log=TelemetryEvent(
                    system_configuration=TelemetryHelper.get_driver_system_configuration(),
                    driver_connection_params=params,
                    session_id=maybe(scalar()),
                    sql_statement_id=maybe(scalar()),
                    sql_operation=maybe(sql_event),
                    error_info=maybe(DriverErrorInfo(scalar(), scalar())),
                    operation_latency_ms=maybe(scalar()),
                )
            ),
            workspace_id=maybe(scalar()),
        )
        assert log.to_json() == main_to_json(log)
