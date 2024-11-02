from databricks.sql.thrift_api.TCLIService import ttypes

class QueryExecutionStatus:
	INITIALIZED_STATE=ttypes.TOperationState.INITIALIZED_STATE
	RUNNING_STATE = ttypes.TOperationState.RUNNING_STATE
	FINISHED_STATE = ttypes.TOperationState.FINISHED_STATE
	CANCELED_STATE = ttypes.TOperationState.CANCELED_STATE
	CLOSED_STATE = ttypes.TOperationState.CLOSED_STATE
	ERROR_STATE = ttypes.TOperationState.ERROR_STATE
	UKNOWN_STATE = ttypes.TOperationState.UKNOWN_STATE
	PENDING_STATE = ttypes.TOperationState.PENDING_STATE
	TIMEDOUT_STATE = ttypes.TOperationState.TIMEDOUT_STATE