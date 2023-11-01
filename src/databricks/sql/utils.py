from __future__ import annotations

import datetime
import decimal
from abc import ABC, abstractmethod
from collections import OrderedDict, namedtuple
from collections.abc import Iterable
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Union

import lz4.frame
import pyarrow

from databricks.sql import OperationalError, exc
from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TRowSet,
    TSparkArrowResultLink,
    TSparkParameter,
    TSparkParameterValue,
    TSparkRowSetType,
)

BIT_MASKS = [1, 2, 4, 8, 16, 32, 64, 128]


class ParameterApproach(Enum):
    INLINE = 1
    NATIVE = 2
    NONE = 3


class ResultSetQueue(ABC):
    @abstractmethod
    def next_n_rows(self, num_rows: int) -> pyarrow.Table:
        pass

    @abstractmethod
    def remaining_rows(self) -> pyarrow.Table:
        pass


class ResultSetQueueFactory(ABC):
    @staticmethod
    def build_queue(
        row_set_type: TSparkRowSetType,
        t_row_set: TRowSet,
        arrow_schema_bytes: bytes,
        max_download_threads: int,
        lz4_compressed: bool = True,
        description: List[List[Any]] = None,
    ) -> ResultSetQueue:
        """
        Factory method to build a result set queue.

        Args:
            row_set_type (enum): Row set type (Arrow, Column, or URL).
            t_row_set (TRowSet): Result containing arrow batches, columns, or cloud fetch links.
            arrow_schema_bytes (bytes): Bytes representing the arrow schema.
            lz4_compressed (bool): Whether result data has been lz4 compressed.
            description (List[List[Any]]): Hive table schema description.
            max_download_threads (int): Maximum number of downloader thread pool threads.

        Returns:
            ResultSetQueue
        """
        if row_set_type == TSparkRowSetType.ARROW_BASED_SET:
            arrow_table, n_valid_rows = convert_arrow_based_set_to_arrow_table(
                t_row_set.arrowBatches, lz4_compressed, arrow_schema_bytes
            )
            converted_arrow_table = convert_decimals_in_arrow_table(
                arrow_table, description
            )
            return ArrowQueue(converted_arrow_table, n_valid_rows)
        elif row_set_type == TSparkRowSetType.COLUMN_BASED_SET:
            arrow_table, n_valid_rows = convert_column_based_set_to_arrow_table(
                t_row_set.columns, description
            )
            converted_arrow_table = convert_decimals_in_arrow_table(
                arrow_table, description
            )
            return ArrowQueue(converted_arrow_table, n_valid_rows)
        elif row_set_type == TSparkRowSetType.URL_BASED_SET:
            return CloudFetchQueue(
                arrow_schema_bytes,
                start_row_offset=t_row_set.startRowOffset,
                result_links=t_row_set.resultLinks,
                lz4_compressed=lz4_compressed,
                description=description,
                max_download_threads=max_download_threads,
            )
        else:
            raise AssertionError("Row set type is not valid")


class ArrowQueue(ResultSetQueue):
    def __init__(
        self,
        arrow_table: pyarrow.Table,
        n_valid_rows: int,
        start_row_index: int = 0,
    ):
        """
        A queue-like wrapper over an Arrow table

        :param arrow_table: The Arrow table from which we want to take rows
        :param n_valid_rows: The index of the last valid row in the table
        :param start_row_index: The first row in the table we should start fetching from
        """
        self.cur_row_index = start_row_index
        self.arrow_table = arrow_table
        self.n_valid_rows = n_valid_rows

    def next_n_rows(self, num_rows: int) -> pyarrow.Table:
        """Get upto the next n rows of the Arrow dataframe"""
        length = min(num_rows, self.n_valid_rows - self.cur_row_index)
        # Note that the table.slice API is not the same as Python's slice
        # The second argument should be length, not end index
        slice = self.arrow_table.slice(self.cur_row_index, length)
        self.cur_row_index += slice.num_rows
        return slice

    def remaining_rows(self) -> pyarrow.Table:
        slice = self.arrow_table.slice(
            self.cur_row_index, self.n_valid_rows - self.cur_row_index
        )
        self.cur_row_index += slice.num_rows
        return slice


class CloudFetchQueue(ResultSetQueue):
    def __init__(
        self,
        schema_bytes,
        max_download_threads: int,
        start_row_offset: int = 0,
        result_links: List[TSparkArrowResultLink] = None,
        lz4_compressed: bool = True,
        description: List[List[Any]] = None,
    ):
        """
        A queue-like wrapper over CloudFetch arrow batches.

        Attributes:
            schema_bytes (bytes): Table schema in bytes.
            max_download_threads (int): Maximum number of downloader thread pool threads.
            start_row_offset (int): The offset of the first row of the cloud fetch links.
            result_links (List[TSparkArrowResultLink]): Links containing the downloadable URL and metadata.
            lz4_compressed (bool): Whether the files are lz4 compressed.
            description (List[List[Any]]): Hive table schema description.
        """
        self.schema_bytes = schema_bytes
        self.max_download_threads = max_download_threads
        self.start_row_index = start_row_offset
        self.result_links = result_links
        self.lz4_compressed = lz4_compressed
        self.description = description

        self.download_manager = ResultFileDownloadManager(
            self.max_download_threads, self.lz4_compressed
        )
        self.download_manager.add_file_links(result_links)

        self.table = self._create_next_table()
        self.table_row_index = 0

    def next_n_rows(self, num_rows: int) -> pyarrow.Table:
        """
        Get up to the next n rows of the cloud fetch Arrow dataframes.

        Args:
            num_rows (int): Number of rows to retrieve.

        Returns:
            pyarrow.Table
        """
        if not self.table:
            # Return empty pyarrow table to cause retry of fetch
            return self._create_empty_table()
        results = self.table.slice(0, 0)
        while num_rows > 0 and self.table:
            # Get remaining of num_rows or the rest of the current table, whichever is smaller
            length = min(num_rows, self.table.num_rows - self.table_row_index)
            table_slice = self.table.slice(self.table_row_index, length)
            results = pyarrow.concat_tables([results, table_slice])
            self.table_row_index += table_slice.num_rows

            # Replace current table with the next table if we are at the end of the current table
            if self.table_row_index == self.table.num_rows:
                self.table = self._create_next_table()
                self.table_row_index = 0
            num_rows -= table_slice.num_rows
        return results

    def remaining_rows(self) -> pyarrow.Table:
        """
        Get all remaining rows of the cloud fetch Arrow dataframes.

        Returns:
            pyarrow.Table
        """
        if not self.table:
            # Return empty pyarrow table to cause retry of fetch
            return self._create_empty_table()
        results = self.table.slice(0, 0)
        while self.table:
            table_slice = self.table.slice(
                self.table_row_index, self.table.num_rows - self.table_row_index
            )
            results = pyarrow.concat_tables([results, table_slice])
            self.table_row_index += table_slice.num_rows
            self.table = self._create_next_table()
            self.table_row_index = 0
        return results

    def _create_next_table(self) -> Union[pyarrow.Table, None]:
        # Create next table by retrieving the logical next downloaded file, or return None to signal end of queue
        downloaded_file = self.download_manager.get_next_downloaded_file(
            self.start_row_index
        )
        if not downloaded_file:
            # None signals no more Arrow tables can be built from the remaining handlers if any remain
            return None
        arrow_table = create_arrow_table_from_arrow_file(
            downloaded_file.file_bytes, self.description
        )

        # The server rarely prepares the exact number of rows requested by the client in cloud fetch.
        # Subsequently, we drop the extraneous rows in the last file if more rows are retrieved than requested
        if arrow_table.num_rows > downloaded_file.row_count:
            self.start_row_index += downloaded_file.row_count
            return arrow_table.slice(0, downloaded_file.row_count)

        # At this point, whether the file has extraneous rows or not, the arrow table should have the correct num rows
        assert downloaded_file.row_count == arrow_table.num_rows
        self.start_row_index += arrow_table.num_rows
        return arrow_table

    def _create_empty_table(self) -> pyarrow.Table:
        # Create a 0-row table with just the schema bytes
        return create_arrow_table_from_arrow_file(self.schema_bytes, self.description)


ExecuteResponse = namedtuple(
    "ExecuteResponse",
    "status has_been_closed_server_side has_more_rows description lz4_compressed is_staging_operation "
    "command_handle arrow_queue arrow_schema_bytes",
)


def _bound(min_x, max_x, x):
    """Bound x by [min_x, max_x]

    min_x or max_x being None means unbounded in that respective side.
    """
    if min_x is None and max_x is None:
        return x
    if min_x is None:
        return min(max_x, x)
    if max_x is None:
        return max(min_x, x)
    return min(max_x, max(min_x, x))


class NoRetryReason(Enum):
    OUT_OF_TIME = "out of time"
    OUT_OF_ATTEMPTS = "out of attempts"
    NOT_RETRYABLE = "non-retryable error"


class RequestErrorInfo(
    namedtuple(
        "RequestErrorInfo_", "error error_message retry_delay http_code method request"
    )
):
    @property
    def request_session_id(self):
        if hasattr(self.request, "sessionHandle"):
            return self.request.sessionHandle.sessionId.guid
        else:
            return None

    @property
    def request_query_id(self):
        if hasattr(self.request, "operationHandle"):
            return self.request.operationHandle.operationId.guid
        else:
            return None

    def full_info_logging_context(
        self, no_retry_reason, attempt, max_attempts, elapsed, max_duration
    ):
        log_base_data_dict = OrderedDict(
            [
                ("method", self.method),
                ("session-id", self.request_session_id),
                ("query-id", self.request_query_id),
                ("http-code", self.http_code),
                ("error-message", self.error_message),
                ("original-exception", str(self.error)),
            ]
        )

        log_base_data_dict["no-retry-reason"] = (
            no_retry_reason and no_retry_reason.value
        )
        log_base_data_dict["bounded-retry-delay"] = self.retry_delay
        log_base_data_dict["attempt"] = "{}/{}".format(attempt, max_attempts)
        log_base_data_dict["elapsed-seconds"] = "{}/{}".format(elapsed, max_duration)

        return log_base_data_dict

    def user_friendly_error_message(self, no_retry_reason, attempt, elapsed):
        # This should be kept at the level that is appropriate to return to a Redash user
        user_friendly_error_message = "Error during request to server"
        if self.error_message:
            user_friendly_error_message = "{}: {}".format(
                user_friendly_error_message, self.error_message
            )
        return user_friendly_error_message


# Taken from PyHive
class ParamEscaper:
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f"
    _DATETIME_FORMAT = "{} {}".format(_DATE_FORMAT, _TIME_FORMAT)

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise exc.ProgrammingError(
                "Unsupported param format: {}".format(parameters)
            )

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("\\", "\\\\").replace("'", "\\'"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return "(" + ",".join(l) + ")"

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return "'{}'".format(formatted)

    def escape_decimal(self, item):
        return str(item)

    def escape_item(self, item):
        if item is None:
            return "NULL"
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, str):
            return self.escape_string(item)
        elif isinstance(item, Iterable):
            return self.escape_sequence(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)
        elif isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)
        elif isinstance(item, decimal.Decimal):
            return self.escape_decimal(item)
        else:
            raise exc.ProgrammingError("Unsupported object {}".format(item))


def inject_parameters(operation: str, parameters: Dict[str, str]):
    return operation % parameters


def create_arrow_table_from_arrow_file(file_bytes: bytes, description) -> pyarrow.Table:
    arrow_table = convert_arrow_based_file_to_arrow_table(file_bytes)
    return convert_decimals_in_arrow_table(arrow_table, description)


def convert_arrow_based_file_to_arrow_table(file_bytes: bytes):
    try:
        return pyarrow.ipc.open_stream(file_bytes).read_all()
    except Exception as e:
        raise RuntimeError("Failure to convert arrow based file to arrow table", e)


def convert_arrow_based_set_to_arrow_table(arrow_batches, lz4_compressed, schema_bytes):
    ba = bytearray()
    ba += schema_bytes
    n_rows = 0
    for arrow_batch in arrow_batches:
        n_rows += arrow_batch.rowCount
        ba += (
            lz4.frame.decompress(arrow_batch.batch)
            if lz4_compressed
            else arrow_batch.batch
        )
    arrow_table = pyarrow.ipc.open_stream(ba).read_all()
    return arrow_table, n_rows


def convert_decimals_in_arrow_table(table, description) -> pyarrow.Table:
    for i, col in enumerate(table.itercolumns()):
        if description[i][1] == "decimal":
            decimal_col = col.to_pandas().apply(
                lambda v: v if v is None else Decimal(v)
            )
            precision, scale = description[i][4], description[i][5]
            assert scale is not None
            assert precision is not None
            # Spark limits decimal to a maximum scale of 38,
            # so 128 is guaranteed to be big enough
            dtype = pyarrow.decimal128(precision, scale)
            col_data = pyarrow.array(decimal_col, type=dtype)
            field = table.field(i).with_type(dtype)
            table = table.set_column(i, field, col_data)
    return table


def convert_column_based_set_to_arrow_table(columns, description):
    arrow_table = pyarrow.Table.from_arrays(
        [_convert_column_to_arrow_array(c) for c in columns],
        # Only use the column names from the schema, the types are determined by the
        # physical types used in column based set, as they can differ from the
        # mapping used in _hive_schema_to_arrow_schema.
        names=[c[0] for c in description],
    )
    return arrow_table, arrow_table.num_rows


def _convert_column_to_arrow_array(t_col):
    """
    Return a pyarrow array from the values in a TColumn instance.
    Note that ColumnBasedSet has no native support for complex types, so they will be converted
    to strings server-side.
    """
    field_name_to_arrow_type = {
        "boolVal": pyarrow.bool_(),
        "byteVal": pyarrow.int8(),
        "i16Val": pyarrow.int16(),
        "i32Val": pyarrow.int32(),
        "i64Val": pyarrow.int64(),
        "doubleVal": pyarrow.float64(),
        "stringVal": pyarrow.string(),
        "binaryVal": pyarrow.binary(),
    }
    for field in field_name_to_arrow_type.keys():
        wrapper = getattr(t_col, field)
        if wrapper:
            return _create_arrow_array(wrapper, field_name_to_arrow_type[field])

    raise OperationalError("Empty TColumn instance {}".format(t_col))


def _create_arrow_array(t_col_value_wrapper, arrow_type):
    result = t_col_value_wrapper.values
    nulls = t_col_value_wrapper.nulls  # bitfield describing which values are null
    assert isinstance(nulls, bytes)

    # The number of bits in nulls can be both larger or smaller than the number of
    # elements in result, so take the minimum of both to iterate over.
    length = min(len(result), len(nulls) * 8)

    for i in range(length):
        if nulls[i >> 3] & BIT_MASKS[i & 0x7]:
            result[i] = None

    return pyarrow.array(result, type=arrow_type)


class DbSqlType(Enum):
    """The values of this enumeration are passed as literals to be used in a CAST
    evaluation by the thrift server.
    """

    STRING = "STRING"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    BOOLEAN = "BOOLEAN"
    INTERVAL_MONTH = "INTERVAL MONTH"
    INTERVAL_DAY = "INTERVAL DAY"
    VOID = "VOID"


class DbSqlParameter:
    name: str
    value: Any
    type: Union[DbSqlType, DbsqlDynamicDecimalType, Enum]

    def __init__(self, name="", value=None, type=None):
        self.name = name
        self.value = value
        self.type = type

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class DbsqlDynamicDecimalType:
    def __init__(self, value):
        self.value = value


def named_parameters_to_dbsqlparams_v1(parameters: Dict[str, str]):
    dbsqlparams = []
    for name, parameter in parameters.items():
        dbsqlparams.append(DbSqlParameter(name=name, value=parameter))
    return dbsqlparams


def named_parameters_to_dbsqlparams_v2(parameters: List[Any]):
    dbsqlparams = []
    for parameter in parameters:
        if isinstance(parameter, DbSqlParameter):
            dbsqlparams.append(parameter)
        else:
            dbsqlparams.append(DbSqlParameter(value=parameter))
    return dbsqlparams


def resolve_databricks_sql_integer_type(integer):
    """Returns DbsqlType.INTEGER unless the passed int() requires a BIGINT.

    Note: TINYINT is never inferred here because it is a rarely used type and clauses like LIMIT and OFFSET
    cannot accept TINYINT bound parameter values. If you need to bind a TINYINT value, you can explicitly
    declare its type in a DbsqlParameter object, which will bypass this inference logic."""
    if -128 <= integer <= 127:
        # If DBR is ever updated to permit TINYINT values passed to LIMIT and OFFSET
        # then we can change this line to return DbSqlType.TINYINT
        return DbSqlType.INTEGER
    elif -2147483648 <= integer <= 2147483647:
        return DbSqlType.INTEGER
    else:
        return DbSqlType.BIGINT


def infer_types(params: list[DbSqlParameter]):
    type_lookup_table = {
        str: DbSqlType.STRING,
        int: DbSqlType.INTEGER,
        float: DbSqlType.FLOAT,
        datetime.datetime: DbSqlType.TIMESTAMP,
        datetime.date: DbSqlType.DATE,
        bool: DbSqlType.BOOLEAN,
        Decimal: DbSqlType.DECIMAL,
        type(None): DbSqlType.VOID,
    }

    new_params = []

    # cycle through each parameter we've been passed
    for param in params:
        _name: str = param.name
        _value: Any = param.value
        _type: Union[DbSqlType, DbsqlDynamicDecimalType, Enum, None]

        if param.type:
            _type = param.type
        else:
            # figure out what type to use
            _type = type_lookup_table.get(type(_value), None)
            if not _type:
                raise ValueError(
                    f"Could not infer parameter type from {type(param.value)} - {param.value}"
                )

        # Decimal require special handling because one column type in Databricks can have multiple precisions
        if _type == DbSqlType.DECIMAL:
            cast_exp = calculate_decimal_cast_string(param.value)
            _type = DbsqlDynamicDecimalType(cast_exp)

        # int() requires special handling because one Python type can be cast to multiple SQL types (INT, BIGINT, TINYINT)
        if _type == DbSqlType.INTEGER:
            _type = resolve_databricks_sql_integer_type(param.value)

        # VOID / NULL types must be passed in a unique way as TSparkParameters with no value
        if _type == DbSqlType.VOID:
            new_params.append(DbSqlParameter(name=_name, type=DbSqlType.VOID))
            continue
        else:
            _value = str(param.value)

        new_params.append(DbSqlParameter(name=_name, value=_value, type=_type))

    return new_params


def calculate_decimal_cast_string(input: Decimal) -> str:
    """Returns the smallest SQL cast argument that can contain the passed decimal

    Example:
        Input:   Decimal("1234.5678")
        Output:  DECIMAL(8,4)
    """

    string_decimal = str(input)

    if string_decimal.startswith("0."):
        # This decimal is less than 1
        overall = after = len(string_decimal) - 2
    elif "." not in string_decimal:
        # This decimal has no fractional component
        overall = len(string_decimal)
        after = 0
    else:
        # This decimal has both whole and fractional parts
        parts = string_decimal.split(".")
        parts_lengths = [len(i) for i in parts]
        before, after = parts_lengths[:2]
        overall = before + after

    return f"DECIMAL({overall},{after})"


def named_parameters_to_tsparkparams(
    parameters: Union[List[Any], Dict[str, str]]
) -> List[TSparkParameter]:
    tspark_params = []
    if isinstance(parameters, dict):
        dbsql_params = named_parameters_to_dbsqlparams_v1(parameters)
    else:
        dbsql_params = named_parameters_to_dbsqlparams_v2(parameters)
    inferred_type_parameters = infer_types(dbsql_params)
    for param in inferred_type_parameters:
        # The only way to pass a VOID/NULL to DBR is to declare TSparkParameter without declaring
        # its value or type arguments. If we set these to NoneType, the request will fail with a
        # thrift transport error
        if param.type == DbSqlType.VOID:
            this_tspark_param = TSparkParameter(name=param.name)
        else:
            this_tspark_param_value = TSparkParameterValue(stringValue=param.value)
            this_tspark_param = TSparkParameter(
                type=param.type.value, name=param.name, value=this_tspark_param_value
            )
        tspark_params.append(this_tspark_param)
    return tspark_params
