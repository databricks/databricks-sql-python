from databricks.sql.parameters.native import ParameterApproach
from typing import Optional, List, Tuple, TYPE_CHECKING


if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from databricks.sql.parameters.native import TParameterCollection, TSparkParameter


from databricks.sql.parameters.native import _normalize_tparametercollection, _determine_parameter_structure, prepare_native_parameters
from databricks.sql.parameters.inline import transform_paramstyle, prepare_inline_parameters


NO_NATIVE_PARAMS: List = []



def prepare_parameters_and_statement(query: str, parameters: Optional["TParameterCollection"], connection: "Connection") -> Tuple[str, List["TSparkParameter"]]:
    """Encapsulates logic for setting the parameter approach, style, and structure.

    Args:
        query: The query to prepare
        parameters: The parameters to prepare
        connection: The parameter approach is derived from whether this connection can use inline parameters

    Returns:
        The prepared query and a list of TSparkParameters
    """


    param_approach = connection._determine_parameter_approach(parameters)
    if param_approach == ParameterApproach.NONE:
        prepared_params = NO_NATIVE_PARAMS
        prepared_query = query

    elif param_approach == ParameterApproach.INLINE:
        prepared_query, prepared_params = prepare_inline_parameters(
            query, parameters
        )
    elif param_approach == ParameterApproach.NATIVE:
        normalized_parameters = _normalize_tparametercollection(parameters)
        param_structure = _determine_parameter_structure(normalized_parameters)
        transformed_operation = transform_paramstyle(
            query, normalized_parameters, param_structure  # type: ignore
        )
        prepared_query, prepared_params = prepare_native_parameters(
            transformed_operation, normalized_parameters, param_structure
        )

    return prepared_query, prepared_params