"""
Databricks SQL Token Federation Examples

This script token federation flows:
1. U2M + Account-wide federation
2. U2M + Workflow-level federation  
3. M2M + Account-wide federation
4. M2M + Workflow-level federation
5. Access Token + Workflow-level federation
6. Access Token + Account-wide federation

Token Federation Documentation:
------------------------------
For detailed setup instructions, refer to the official Databricks documentation:

- General Token Federation Overview:
  https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation.html

- Token Exchange Process:
  https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-howto.html

- Azure OAuth Token Federation:
  https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-federation

Environment variables required:
- DATABRICKS_HOST: Databricks workspace hostname
- DATABRICKS_HTTP_PATH: HTTP path for the SQL warehouse
- AZURE_TENANT_ID: Azure tenant ID
- AZURE_CLIENT_ID: Azure client ID for service principal
- AZURE_CLIENT_SECRET: Azure client secret
- DATABRICKS_SERVICE_PRINCIPAL_ID: Databricks service principal ID for workflow federation
"""

import os
from databricks import sql

def run_query(connection, description):
    cursor = connection.cursor()
    cursor.execute("SELECT 1+1 AS result")
    result = cursor.fetchall()
    print(f"Query result: {result[0][0]}")
    
    cursor.close()

def demonstrate_m2m_federation(env_vars, use_workflow_federation=False):
    """Demonstrate M2M (service principal) token federation"""
    
    connection_params = {
        "server_hostname": env_vars["DATABRICKS_HOST"],
        "http_path": env_vars["DATABRICKS_HTTP_PATH"],
        "auth_type": "client-credentials",
        "oauth_client_id": env_vars["AZURE_CLIENT_ID"],
        "client_secret": env_vars["AZURE_CLIENT_SECRET"],
        "tenant_id": env_vars["AZURE_TENANT_ID"],
        "use_token_federation": True
    }
    
    if use_workflow_federation and env_vars["DATABRICKS_SERVICE_PRINCIPAL_ID"]:
        connection_params["identity_federation_client_id"] = env_vars["DATABRICKS_SERVICE_PRINCIPAL_ID"]
        description = "M2M + Workflow-level Federation"
    else:
        description = "M2M + Account-wide Federation"
    
    with sql.connect(**connection_params) as connection:
        run_query(connection, description)

        
def demonstrate_u2m_federation(env_vars, use_workflow_federation=False):
    """Demonstrate U2M (interactive) token federation"""
    
    connection_params = {
        "server_hostname": env_vars["DATABRICKS_HOST"],
        "http_path": env_vars["DATABRICKS_HTTP_PATH"],
        "auth_type": "databricks-oauth",  # Will open browser for interactive auth
        "use_token_federation": True
    }
    
    if use_workflow_federation and env_vars["DATABRICKS_SERVICE_PRINCIPAL_ID"]:
        connection_params["identity_federation_client_id"] = env_vars["DATABRICKS_SERVICE_PRINCIPAL_ID"]
        description = "U2M + Workflow-level Federation (Interactive)"
    else:
        description = "U2M + Account-wide Federation (Interactive)"
    
    # This will open a browser for interactive auth
    with sql.connect(**connection_params) as connection:
        run_query(connection, description)

def demonstrate_access_token_federation(env_vars):
    """Demonstrate access token token federation"""
    
    access_token = os.environ.get("ACCESS_TOKEN") # This is to demonstrate a token obtained from an identity provider
    
    connection_params = {
        "server_hostname": env_vars["DATABRICKS_HOST"],
        "http_path": env_vars["DATABRICKS_HTTP_PATH"],
        "access_token": access_token,
        "use_token_federation": True
    }
    
    # Add workflow federation if available
    if env_vars["DATABRICKS_SERVICE_PRINCIPAL_ID"]:
        connection_params["identity_federation_client_id"] = env_vars["DATABRICKS_SERVICE_PRINCIPAL_ID"]
        description = "Access Token + Workflow-level Federation"
    else:
        description = "Access Token + Account-wide Federation"
    
    with sql.connect(**connection_params) as connection:
        run_query(connection, description)

