"""
Tests for SEA-related queue classes in utils.py.

This module contains tests for the JsonQueue and SeaResultSetQueueFactory classes.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch

                "test-statement-123",
                description=mock_description,
                sea_client=mock_sea_client,
            )
