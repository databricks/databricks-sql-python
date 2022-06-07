from collections import namedtuple, OrderedDict
from collections.abc import Iterable
import datetime
from enum import Enum

import pyarrow


class ArrowQueue:
    def __init__(
        self, arrow_table: pyarrow.Table, n_valid_rows: int, start_row_index: int = 0
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


ExecuteResponse = namedtuple(
    "ExecuteResponse",
    "status has_been_closed_server_side has_more_rows description "
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
        return "'{}'".format(item.replace("'", "''"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return "(" + ",".join(l) + ")"

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return "'{}'".format(formatted)

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
        else:
            raise exc.ProgrammingError("Unsupported object {}".format(item))
