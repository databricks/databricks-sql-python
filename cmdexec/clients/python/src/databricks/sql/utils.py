from collections import namedtuple

import pyarrow


class ArrowQueue:
    def __init__(self, arrow_table: pyarrow.Table, n_valid_rows: int, start_row_index: int):
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
        slice = self.arrow_table.slice(self.cur_row_index, length)
        self.cur_row_index += slice.num_rows
        return slice

    def remaining_rows(self) -> pyarrow.Table:
        slice = self.arrow_table.slice(self.cur_row_index, self.n_valid_rows - self.cur_row_index)
        self.cur_row_index += slice.num_rows
        return slice


ExecuteResponse = namedtuple(
    'ExecuteResponse', 'status has_been_closed_server_side has_more_rows description '
    'command_handle arrow_queue arrow_schema')
