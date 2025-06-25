"""
Tests for the JsonQueue class.

This module contains tests for the JsonQueue class, which implements
a queue for JSON array data returned by the SEA backend.
"""

import pytest
from databricks.sql.utils import JsonQueue


class TestJsonQueue:
    """Test suite for the JsonQueue class."""

    @pytest.fixture
    def sample_data_array(self):
        """Create a sample data array for testing."""
        return [
            [1, "value1"],
            [2, "value2"],
            [3, "value3"],
            [4, "value4"],
            [5, "value5"],
        ]

    def test_init(self, sample_data_array):
        """Test initializing JsonQueue with a data array."""
        queue = JsonQueue(sample_data_array)
        assert queue.data_array == sample_data_array
        assert queue.cur_row_index == 0
        assert queue.n_valid_rows == 5

    def test_next_n_rows_partial(self, sample_data_array):
        """Test getting a subset of rows."""
        queue = JsonQueue(sample_data_array)
        rows = queue.next_n_rows(3)

        # Check that we got the first 3 rows
        assert rows == sample_data_array[:3]

        # Check that the current row index was updated
        assert queue.cur_row_index == 3

    def test_next_n_rows_all(self, sample_data_array):
        """Test getting all rows at once."""
        queue = JsonQueue(sample_data_array)
        rows = queue.next_n_rows(10)  # More than available

        # Check that we got all rows
        assert rows == sample_data_array

        # Check that the current row index was updated
        assert queue.cur_row_index == 5

    def test_next_n_rows_empty(self):
        """Test getting rows from an empty queue."""
        queue = JsonQueue([])
        rows = queue.next_n_rows(5)

        # Check that we got an empty list
        assert rows == []

        # Check that the current row index was not updated
        assert queue.cur_row_index == 0

    def test_next_n_rows_zero(self, sample_data_array):
        """Test getting zero rows."""
        queue = JsonQueue(sample_data_array)
        rows = queue.next_n_rows(0)

        # Check that we got an empty list
        assert rows == []

        # Check that the current row index was not updated
        assert queue.cur_row_index == 0

    def test_next_n_rows_sequential(self, sample_data_array):
        """Test getting rows in multiple sequential calls."""
        queue = JsonQueue(sample_data_array)

        # Get first 2 rows
        rows1 = queue.next_n_rows(2)
        assert rows1 == sample_data_array[:2]
        assert queue.cur_row_index == 2

        # Get next 2 rows
        rows2 = queue.next_n_rows(2)
        assert rows2 == sample_data_array[2:4]
        assert queue.cur_row_index == 4

        # Get remaining rows
        rows3 = queue.next_n_rows(2)
        assert rows3 == sample_data_array[4:]
        assert queue.cur_row_index == 5

    def test_remaining_rows(self, sample_data_array):
        """Test getting all remaining rows."""
        queue = JsonQueue(sample_data_array)

        # Get first 2 rows
        queue.next_n_rows(2)

        # Get remaining rows
        rows = queue.remaining_rows()

        # Check that we got the remaining rows
        assert rows == sample_data_array[2:]

        # Check that the current row index was updated to the end
        assert queue.cur_row_index == 5

    def test_remaining_rows_empty(self):
        """Test getting remaining rows from an empty queue."""
        queue = JsonQueue([])
        rows = queue.remaining_rows()

        # Check that we got an empty list
        assert rows == []

        # Check that the current row index was not updated
        assert queue.cur_row_index == 0

    def test_remaining_rows_after_all_consumed(self, sample_data_array):
        """Test getting remaining rows after all rows have been consumed."""
        queue = JsonQueue(sample_data_array)

        # Consume all rows
        queue.next_n_rows(10)

        # Try to get remaining rows
        rows = queue.remaining_rows()

        # Check that we got an empty list
        assert rows == []

        # Check that the current row index was not updated
        assert queue.cur_row_index == 5
