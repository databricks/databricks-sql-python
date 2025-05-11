#!/usr/bin/env python3

"""
Unit tests for token federation functionality in the Databricks SQL connector.
"""

import pytest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime, timezone, timedelta

from databricks.sql.auth.token_federation import (
    Token,
    DatabricksTokenFederationProvider,
    SimpleCredentialsProvider,
    TOKEN_REFRESH_BUFFER_SECONDS,
)


# Tests for Token class
def test_token_initialization():
    """Test Token initialization."""
    token = Token("access_token_value", "Bearer", "refresh_token_value")
    assert token.access_token == "access_token_value"
    assert token.token_type == "Bearer"
    assert token.refresh_token == "refresh_token_value"


def test_token_is_expired():
    """Test Token is_expired method."""
    # Token with expiry in the past
    past = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    token = Token("access_token", "Bearer", expiry=past)
    assert token.is_expired()

    # Token with expiry in the future
    future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    token = Token("access_token", "Bearer", expiry=future)
    assert not token.is_expired()


def test_token_needs_refresh():
    """Test Token needs_refresh method using actual TOKEN_REFRESH_BUFFER_SECONDS."""
    # Token with expiry in the past
    past = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    token = Token("access_token", "Bearer", expiry=past)
    assert token.needs_refresh()

    # Token with expiry in the near future (within refresh buffer)
    near_future = datetime.now(tz=timezone.utc) + timedelta(
        seconds=TOKEN_REFRESH_BUFFER_SECONDS - 1
    )
    token = Token("access_token", "Bearer", expiry=near_future)
    assert token.needs_refresh()

    # Token with expiry far in the future
    far_future = datetime.now(tz=timezone.utc) + timedelta(
        seconds=TOKEN_REFRESH_BUFFER_SECONDS + 10
    )
    token = Token("access_token", "Bearer", expiry=far_future)
    assert not token.needs_refresh()


# Tests for SimpleCredentialsProvider
def test_simple_credentials_provider():
    """Test SimpleCredentialsProvider."""
    provider = SimpleCredentialsProvider(
        "token_value", "Bearer", "custom_auth_type"
    )
    assert provider.auth_type() == "custom_auth_type"

    header_factory = provider()
    headers = header_factory()
    assert headers == {"Authorization": "Bearer token_value"}


# Tests for DatabricksTokenFederationProvider
def test_host_property():
    """Test the host property of DatabricksTokenFederationProvider."""
    creds_provider = SimpleCredentialsProvider("token")
    federation_provider = DatabricksTokenFederationProvider(
        creds_provider, "example.com", "client_id"
    )
    assert federation_provider.host == "example.com"
    assert federation_provider.hostname == "example.com"


@pytest.fixture
def mock_request_get():
    with patch("databricks.sql.auth.token_federation.requests.get") as mock:
        yield mock


@pytest.fixture
def mock_get_oauth_endpoints():
    with patch("databricks.sql.auth.token_federation.get_oauth_endpoints") as mock:
        yield mock


def test_init_oidc_discovery(mock_request_get, mock_get_oauth_endpoints):
    """Test _init_oidc_discovery method."""
    # Mock the get_oauth_endpoints function
    mock_endpoints = MagicMock()
    mock_endpoints.get_openid_config_url.return_value = (
        "https://example.com/openid-config"
    )
    mock_get_oauth_endpoints.return_value = mock_endpoints

    # Mock the requests.get response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "token_endpoint": "https://example.com/token"
    }
    mock_request_get.return_value = mock_response

    # Create the provider
    creds_provider = SimpleCredentialsProvider("token")
    federation_provider = DatabricksTokenFederationProvider(
        creds_provider, "example.com", "client_id"
    )

    # Call the method
    federation_provider._init_oidc_discovery()

    # Check if the token endpoint was set correctly
    assert federation_provider.token_endpoint == "https://example.com/token"

    # Test fallback when discovery fails
    mock_request_get.side_effect = Exception("Connection error")
    federation_provider.token_endpoint = None
    federation_provider._init_oidc_discovery()
    assert federation_provider.token_endpoint == "https://example.com/oidc/v1/token"


@pytest.fixture
def mock_parse_jwt_claims():
    with patch(
        "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._parse_jwt_claims"
    ) as mock:
        yield mock


@pytest.fixture
def mock_exchange_token():
    with patch(
        "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._exchange_token"
    ) as mock:
        yield mock


@pytest.fixture
def mock_is_same_host():
    with patch(
        "databricks.sql.auth.token_federation.DatabricksTokenFederationProvider._is_same_host"
    ) as mock:
        yield mock


def test_token_refresh(mock_parse_jwt_claims, mock_exchange_token, mock_is_same_host):
    """Test token refresh functionality for approaching expiry."""
    # Set up mocks
    mock_parse_jwt_claims.return_value = {
        "iss": "https://login.microsoftonline.com/tenant"
    }
    mock_is_same_host.return_value = False

    # Create the initial header factory
    initial_headers = {"Authorization": "Bearer initial_token"}
    initial_header_factory = MagicMock()
    initial_header_factory.return_value = initial_headers

    # Create the fresh header factory for later use
    fresh_headers = {"Authorization": "Bearer fresh_token"}
    fresh_header_factory = MagicMock()
    fresh_header_factory.return_value = fresh_headers

    # Create the credentials provider that will return the header factory
    mock_creds_provider = MagicMock()
    mock_creds_provider.return_value = initial_header_factory

    # Set up the token federation provider
    federation_provider = DatabricksTokenFederationProvider(
        mock_creds_provider, "example.com", "client_id"
    )

    # Mock the token exchange to return a known token
    future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    mock_exchange_token.return_value = Token(
        "exchanged_token_1", "Bearer", expiry=future_time
    )

    # First call to get initial headers and token - this should trigger an exchange
    headers_factory = federation_provider()
    headers = headers_factory()

    # Verify the exchange happened with the initial token
    mock_exchange_token.assert_called_with("initial_token")
    assert headers["Authorization"] == "Bearer exchanged_token_1"

    # Reset the mocks to track the next call
    mock_exchange_token.reset_mock()

    # Now simulate an approaching expiry
    near_expiry = datetime.now(tz=timezone.utc) + timedelta(
        seconds=TOKEN_REFRESH_BUFFER_SECONDS - 1
    )
    federation_provider.last_exchanged_token = Token(
        "exchanged_token_1", "Bearer", expiry=near_expiry
    )
    federation_provider.last_external_token = "initial_token"

    # For the refresh call, we need the credentials provider to return a fresh token
    # Update the mock to return fresh_header_factory for the second call
    mock_creds_provider.return_value = fresh_header_factory

    # Set up the mock to return a different token for the refresh
    mock_exchange_token.return_value = Token(
        "exchanged_token_2", "Bearer", expiry=future_time
    )

    # Make a second call which should trigger refresh
    headers = headers_factory()

    # Verify the exchange was performed with the fresh token
    mock_exchange_token.assert_called_once_with("fresh_token")

    # Verify the headers contain the new token
    assert headers["Authorization"] == "Bearer exchanged_token_2"


def test_create_token_federation_provider():
    """Test creation of a federation provider with a simple token provider."""
    # Create a simple provider
    simple_provider = SimpleCredentialsProvider("token_value", "Bearer")
    
    # Create a federation provider with the simple provider
    federation_provider = DatabricksTokenFederationProvider(
        simple_provider, "example.com", "client_id"
    )

    assert isinstance(federation_provider, DatabricksTokenFederationProvider)
    assert federation_provider.hostname == "example.com"
    assert federation_provider.identity_federation_client_id == "client_id"

    # Test that the underlying credentials provider was set up correctly
    assert federation_provider.credentials_provider.token == "token_value"
    assert federation_provider.credentials_provider.token_type == "Bearer"
