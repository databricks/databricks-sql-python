"""
Tests for the SeaResultSetQueueFactory class.

This module contains tests for the SeaResultSetQueueFactory class, which builds
appropriate result set queues for the SEA backend.
"""

import pytest
from unittest.mock import Mock, patch

from databricks.sql.utils import SeaResultSetQueueFactory, JsonQueue
from databricks.sql.backend.sea.models.base import ResultData, ResultManifest


class TestSeaResultSetQueueFactory:
    """Test suite for the SeaResultSetQueueFactory class."""

    @pytest.fixture
    def mock_result_data_with_json(self):
        """Create a mock ResultData with JSON data."""
        result_data = Mock(spec=ResultData)
        result_data.data = [[1, "value1"], [2, "value2"]]
        result_data.external_links = None
        return result_data

    @pytest.fixture
    def mock_result_data_with_external_links(self):
        """Create a mock ResultData with external links."""
        result_data = Mock(spec=ResultData)
        result_data.data = None
        result_data.external_links = ["link1", "link2"]
        return result_data

    @pytest.fixture
    def mock_result_data_empty(self):
        """Create a mock ResultData with no data."""
        result_data = Mock(spec=ResultData)
        result_data.data = None
        result_data.external_links = None
        return result_data

    @pytest.fixture
    def mock_manifest(self):
        """Create a mock manifest."""
        return Mock(spec=ResultManifest)

    def test_build_queue_with_json_data(
        self, mock_result_data_with_json, mock_manifest
    ):
        """Test building a queue with JSON data."""
        queue = SeaResultSetQueueFactory.build_queue(
            sea_result_data=mock_result_data_with_json,
            manifest=mock_manifest,
            statement_id="test-statement-id",
        )

        # Check that we got a JsonQueue
        assert isinstance(queue, JsonQueue)

        # Check that the queue has the correct data
        assert queue.data_array == mock_result_data_with_json.data

    def test_build_queue_with_external_links(
        self, mock_result_data_with_external_links, mock_manifest
    ):
        """Test building a queue with external links."""
        with pytest.raises(
            NotImplementedError,
            match="EXTERNAL_LINKS disposition is not implemented for SEA backend",
        ):
            SeaResultSetQueueFactory.build_queue(
                sea_result_data=mock_result_data_with_external_links,
                manifest=mock_manifest,
                statement_id="test-statement-id",
            )

    def test_build_queue_with_empty_data(self, mock_result_data_empty, mock_manifest):
        """Test building a queue with empty data."""
        queue = SeaResultSetQueueFactory.build_queue(
            sea_result_data=mock_result_data_empty,
            manifest=mock_manifest,
            statement_id="test-statement-id",
        )

        # Check that we got a JsonQueue with empty data
        assert isinstance(queue, JsonQueue)
        assert queue.data_array == []
