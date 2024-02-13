import unittest
import os
import pytest

from unittest.mock import patch

from databricks.sql.auth.auth import AuthType
from databricks.sql.auth.endpoint import (
    infer_cloud_from_host,
    CloudType,
    get_oauth_endpoints,
    AzureOAuthEndpointCollection,
)

aws_host = "foo-bar.cloud.databricks.com"
azure_host = "foo-bar.1.azuredatabricks.net"
azure_cn_host = "foo-bar2.databricks.azure.cn"
gcp_host = "foo.1.gcp.databricks.com"


class EndpointTest(unittest.TestCase):
    def test_infer_cloud_from_host(self):
        param_list = [
            (CloudType.AWS, aws_host),
            (CloudType.AZURE, azure_host),
            (None, "foo.example.com"),
        ]

        for expected_type, host in param_list:
            with self.subTest(expected_type or "None", expected_type=expected_type):
                self.assertEqual(infer_cloud_from_host(host), expected_type)
                self.assertEqual(
                    infer_cloud_from_host(f"https://{host}/to/path"), expected_type
                )

    def test_oauth_endpoint(self):
        scopes = ["offline_access", "sql", "admin"]
        scopes2 = ["sql", "admin"]
        azure_scope = (
            f"{AzureOAuthEndpointCollection.DATATRICKS_AZURE_APP}/user_impersonation"
        )

        param_list = [
            (
                CloudType.AWS,
                aws_host,
                False,
                f"https://{aws_host}/oidc/oauth2/v2.0/authorize",
                f"https://{aws_host}/oidc/.well-known/oauth-authorization-server",
                scopes,
                scopes2,
            ),
            (
                CloudType.AZURE,
                azure_cn_host,
                False,
                f"https://{azure_cn_host}/oidc/oauth2/v2.0/authorize",
                "https://login.microsoftonline.com/organizations/v2.0/.well-known/openid-configuration",
                [azure_scope, "offline_access"],
                [azure_scope],
            ),
            (
                CloudType.AZURE,
                azure_host,
                True,
                f"https://{azure_host}/oidc/oauth2/v2.0/authorize",
                "https://login.microsoftonline.com/organizations/v2.0/.well-known/openid-configuration",
                [azure_scope, "offline_access"],
                [azure_scope],
            ),
            (
                CloudType.AZURE,
                azure_host,
                False,
                f"https://{azure_host}/oidc/oauth2/v2.0/authorize",
                f"https://{azure_host}/oidc/.well-known/oauth-authorization-server",
                scopes,
                scopes2,
            ),
            (
                CloudType.GCP,
                gcp_host,
                False,
                f"https://{gcp_host}/oidc/oauth2/v2.0/authorize",
                f"https://{gcp_host}/oidc/.well-known/oauth-authorization-server",
                scopes,
                scopes2,
            ),
        ]

        for (
            cloud_type,
            host,
            use_azure_auth,
            expected_auth_url,
            expected_config_url,
            expected_scopes,
            expected_scope2,
        ) in param_list:
            with self.subTest(cloud_type):
                endpoint = get_oauth_endpoints(host, use_azure_auth)
                self.assertEqual(
                    endpoint.get_authorization_url(host), expected_auth_url
                )
                self.assertEqual(
                    endpoint.get_openid_config_url(host), expected_config_url
                )
                self.assertEqual(endpoint.get_scopes_mapping(scopes), expected_scopes)
                self.assertEqual(endpoint.get_scopes_mapping(scopes2), expected_scope2)

    @patch.dict(
        os.environ,
        {"DATABRICKS_AZURE_TENANT_ID": "052ee82f-b79d-443c-8682-3ec1749e56b0"},
    )
    def test_azure_oauth_scope_mappings_from_different_tenant_id(self):
        scopes = ["offline_access", "sql", "all"]
        endpoint = get_oauth_endpoints(azure_host, True)
        self.assertEqual(
            endpoint.get_scopes_mapping(scopes),
            [
                "052ee82f-b79d-443c-8682-3ec1749e56b0/user_impersonation",
                "offline_access",
            ],
        )
