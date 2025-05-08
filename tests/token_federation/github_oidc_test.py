#!/usr/bin/env python3

"""
Test script for Databricks SQL token federation with GitHub Actions OIDC tokens.

This script tests the Databricks SQL connector with token federation
using a GitHub Actions OIDC token. It connects to a Databricks SQL warehouse,
runs a simple query, and shows the connected user.
"""

import os
import sys
import json
import base64
from databricks import sql


def decode_jwt(token):
    """Decode and return the claims from a JWT token."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("Invalid JWT format")
        
        payload = parts[1]
        # Add padding if needed
        padding = '=' * (4 - len(payload) % 4)
        payload += padding
        
        decoded = base64.b64decode(payload)
        return json.loads(decoded)
    except Exception as e:
        print(f"Failed to decode token: {str(e)}")
        return None


def main():
    # Get GitHub OIDC token
    github_token = os.environ.get("OIDC_TOKEN")
    if not github_token:
        print("GitHub OIDC token not available")
        sys.exit(1)
    
    # Get Databricks connection parameters
    host = os.environ.get("DATABRICKS_HOST_FOR_TF")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH_FOR_TF")
    identity_federation_client_id = os.environ.get("IDENTITY_FEDERATION_CLIENT_ID")
    
    if not host or not http_path:
        print("Missing Databricks connection parameters")
        sys.exit(1)
    
    # Display token claims for debugging
    claims = decode_jwt(github_token)
    if claims:
        print("\n=== GitHub OIDC Token Claims ===")
        print(f"Token issuer: {claims.get('iss')}")
        print(f"Token subject: {claims.get('sub')}")
        print(f"Token audience: {claims.get('aud')}")
        print(f"Token expiration: {claims.get('exp', 'unknown')}")
        print(f"Repository: {claims.get('repository', 'unknown')}")
        print(f"Workflow ref: {claims.get('workflow_ref', 'unknown')}")
        print(f"Event name: {claims.get('event_name', 'unknown')}")
        print("===============================\n")
    
    try:
        # Connect to Databricks using token federation
        print(f"=== Testing Connection via Connector ===")
        print(f"Connecting to Databricks at {host}{http_path}")
        print(f"Using client ID: {identity_federation_client_id}")
        
        connection_params = {
            "server_hostname": host,
            "http_path": http_path,
            "access_token": github_token,
            "auth_type": "token-federation",
            "identity_federation_client_id": identity_federation_client_id,
        }
        
        with sql.connect(**connection_params) as connection:
            print("Connection established successfully")
            
            # Execute a simple query
            cursor = connection.cursor()
            cursor.execute("SELECT 1 + 1 as result")
            result = cursor.fetchall()
            print(f"Query result: {result[0][0]}")
            
            # Show current user
            cursor.execute("SELECT current_user() as user")
            result = cursor.fetchall()
            print(f"Connected as user: {result[0][0]}")
            
            print("Token federation test successful!")
            return True
    except Exception as e:
        print(f"Error connecting to Databricks: {str(e)}")
        print("===================================\n")
        sys.exit(1)


if __name__ == "__main__":
    main() 