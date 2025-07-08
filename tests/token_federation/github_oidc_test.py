"""
Test script for Databricks SQL token federation with GitHub Actions OIDC tokens.

This script tests the Databricks SQL connector with token federation
using a GitHub Actions OIDC token. It connects to a Databricks SQL warehouse,
runs a simple query, and shows the connected user.
"""

import os
import sys
import logging
import jwt
from databricks import sql


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def decode_jwt(token):
    """
    Decode and return the claims from a JWT token.

    Args:
        token: The JWT token string

    Returns:
        dict: The decoded token claims or empty dict if decoding fails
    """
    try:
        # Using PyJWT library to decode token without verification
        return jwt.decode(token, options={"verify_signature": False})
    except Exception as e:
        logger.error(f"Failed to decode token: {str(e)}")
        return {}


def get_environment_variables():
    """
    Get required environment variables for the test.

    Returns:
        tuple: (github_token, host, http_path, identity_federation_client_id)
    """
    github_token = os.environ.get("OIDC_TOKEN")
    host = os.environ.get("DATABRICKS_HOST_FOR_TF")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH_FOR_TF")
    identity_federation_client_id = os.environ.get("IDENTITY_FEDERATION_CLIENT_ID")

    # Validate required environment variables
    if not github_token:
        raise ValueError("OIDC_TOKEN environment variable is required")
    if not host:
        raise ValueError("DATABRICKS_HOST_FOR_TF environment variable is required")
    if not http_path:
        raise ValueError("DATABRICKS_HTTP_PATH_FOR_TF environment variable is required")

    return github_token, host, http_path, identity_federation_client_id


def display_token_info(claims):
    """
    Display token claims for debugging.

    Args:
        claims: Dictionary containing JWT token claims
    """
    if not claims:
        logger.warning("No token claims available to display")
        return

    logger.info("=== GitHub OIDC Token Claims ===")
    logger.info(f"Token issuer: {claims.get('iss')}")
    logger.info(f"Token subject: {claims.get('sub')}")
    logger.info(f"Token audience: {claims.get('aud')}")
    logger.info(f"Token expiration: {claims.get('exp', 'unknown')}")
    logger.info(f"Repository: {claims.get('repository', 'unknown')}")
    logger.info(f"Workflow ref: {claims.get('workflow_ref', 'unknown')}")
    logger.info(f"Event name: {claims.get('event_name', 'unknown')}")
    logger.info("===============================")


def test_databricks_connection(
    host, http_path, github_token, identity_federation_client_id
):
    """
    Test connection to Databricks using token federation.

    Args:
        host: Databricks host
        http_path: Databricks HTTP path
        github_token: GitHub OIDC token
        identity_federation_client_id: Identity federation client ID

    Returns:
        bool: True if the test is successful, False otherwise
    """
    logger.info("=== Testing Connection via Connector ===")
    logger.info(f"Connecting to Databricks at {host}{http_path}")
    logger.info(f"Using client ID: {identity_federation_client_id}")

    connection_params = {
        "server_hostname": host,
        "http_path": http_path,
        "access_token": github_token,
        "auth_type": "token-federation",
    }

    # Add identity federation client ID if provided
    if identity_federation_client_id:
        connection_params[
            "identity_federation_client_id"
        ] = identity_federation_client_id

    try:
        with sql.connect(**connection_params) as connection:
            logger.info("Connection established successfully")

            # Execute a simple query
            cursor = connection.cursor()
            cursor.execute("SELECT 1 + 1 as result")
            result = cursor.fetchall()
            logger.info(f"Query result: {result[0][0]}")

            # Show current user
            cursor.execute("SELECT current_user() as user")
            result = cursor.fetchall()
            logger.info(f"Connected as user: {result[0][0]}")

            logger.info("Token federation test successful!")
            return True
    except Exception as e:
        logger.error(f"Error connecting to Databricks: {str(e)}")
        return False


def main():
    """Main entry point for the test script."""
    try:
        # Get environment variables
        (
            github_token,
            host,
            http_path,
            identity_federation_client_id,
        ) = get_environment_variables()

        # Display token claims
        claims = decode_jwt(github_token)
        display_token_info(claims)

        # Test Databricks connection
        success = test_databricks_connection(
            host, http_path, github_token, identity_federation_client_id
        )

        if not success:
            logger.error("Token federation test failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
