from typing import List, Optional, Dict
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TExecuteStatementReq,
    TSparkArrowTypes,
    TSparkGetDirectResults,
    TSessionHandle,
)

from dataclasses import dataclass, field


class ExecuteStatementInjector:
    def apply(self, req: TExecuteStatementReq, **kwargs) -> TExecuteStatementReq:
        for key, val in kwargs.items():
            setattr(req, key, val)
        return req


def conf_overlay() -> Dict[str, str]:

    # We want to receive proper Timestamp arrow types.
    return {
            "spark.thriftserver.arrowBasedRowSet.timestampAsString": "false"
        }

@dataclass
class ThriftConfig:
    max_rows: int
    max_bytes: int
    use_arrow_native_timestamps: bool = True
    use_arrow_native_decimals: bool = True
    use_arrow_native_complex_types: bool = True
    lz4_compression: bool = True
    use_cloud_fetch: bool = True
    interval_types_as_arrow: bool = False
    runAsync: bool = True
    canReadArrowResult: bool = True
    use_cloud_fetch: bool = True
    config_overlay: dict = field(default_factory=conf_overlay)


class ArrowResultFetcher(ExecuteStatementInjector):
    def __init__(self, config: ThriftConfig) -> None:
        self.config = config

    def prepare_spark_get_direct_results(self) -> TSparkGetDirectResults:
        """"""

        spark_get_direct_results = TSparkGetDirectResults()
        return self.apply(
            spark_get_direct_results,
            maxRows=self.config.max_rows,
            maxBytes=self.config.max_bytes,
        )

    def prepare_spark_arrow_types(self) -> TSparkArrowTypes:
        """"""

        # TODO: For intervalTypesAsArrow:
        # The current Arrow type used for intervals can not be deserialised in PyArrow
        # DBR should be changed to use month_day_nano_interval
        spark_arrow_types = TSparkArrowTypes()
        return self.apply(
            spark_arrow_types,
            timestampAsArrow=self.config.use_arrow_native_timestamps,
            decimalAsArrow=self.config.use_arrow_native_decimals,
            complexTypesAsArrow=self.config.use_arrow_native_complex_types,
            intervalTypesAsArrow=self.config.interval_types_as_arrow,
        )

    def prepare_execute_statement(
        self,
        session_handle: TSessionHandle,
        operation: str,
        parameters: Optional[List[TSparkParameter]],
    ) -> TExecuteStatementReq:
        execute_statement = TExecuteStatementReq()
        spark_arrow_types = self.prepare_spark_arrow_types()
        spark_get_direct_results = self.prepare_spark_get_direct_results()
        return self.apply(
            execute_statement,
            sessionHandle=session_handle,
            statement=operation,
            getDirectResults=spark_get_direct_results,
            useArrowNativeTypes=spark_arrow_types,
            parameters=parameters,
            runAsync=self.config.runAsync,
            canReadArrowResult=self.config.canReadArrowResult,
            canDecompressLZ4Result=self.config.lz4_compression,
            canDownloadResult=self.config.use_cloud_fetch,
            confOverlay=self.config.config_overlay,
        )


class SparkResultFetcher(ArrowResultFetcher):

    def prepare_execute_statement(self, session_handle: TSessionHandle, operation: str, parameters: List[TSparkParameter] | None) -> TExecuteStatementReq:
        
        
        execute_statement = TExecuteStatementReq()
        return self.apply(execute_statement,
                          sessionHandle=session_handle,
            statement=operation,
            parameters=parameters,
            runAsync=self.config.runAsync,
            canReadArrowResult=False,
            canDecompressLZ4Result=self.config.lz4_compression,
            canDownloadResult=self.config.use_cloud_fetch,
            confOverlay=self.config.config_overlay,
        )
        