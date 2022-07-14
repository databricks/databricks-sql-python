# Copyright 2022 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import base64
import hashlib
import os
import webbrowser
import json
from datetime import datetime, timedelta, tzinfo

import logging

import oauthlib.oauth2
from oauthlib.oauth2.rfc6749.errors import OAuth2Error

import requests
from requests.exceptions import RequestException


try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
except ImportError:
    from http.server import BaseHTTPRequestHandler, HTTPServer

logger = logging.getLogger(__name__)


# This could use 'import secrets' in Python 3
def token_urlsafe(nbytes=32):
    tok = os.urandom(nbytes)
    return base64.urlsafe_b64encode(tok).rstrip(b"=").decode("ascii")


# This could be datetime.timezone.utc in Python 3
class UTCTimeZone(tzinfo):
    """UTC"""
    def utcoffset(self, dt):
        #pylint: disable=unused-argument
        return timedelta(0)

    def tzname(self, dt):
        #pylint: disable=unused-argument
        return "UTC"

    def dst(self, dt):
        #pylint: disable=unused-argument
        return timedelta(0)


# Some constant values
OIDC_REDIRECTOR_PATH = "oidc"
REDIRECT_PORT = 8020
UTC = UTCTimeZone()


def get_redirect_url(port=REDIRECT_PORT):
    return f"http://localhost:{port}"


def fetch_well_known_config(idp_url):
    known_config_url = f"{idp_url}/.well-known/oauth-authorization-server"
    try:
        response = requests.get(url=known_config_url)
    except RequestException as e:
        logger.error(f"Unable to fetch OAuth configuration from {idp_url}.\n"
                     "Verify it is a valid workspace URL and that OAuth is "
                     "enabled on this account.")
        raise e

    if response.status_code != 200:
        msg = (f"Received status {response.status_code} OAuth configuration from "
               f"{idp_url}.\n Verify it is a valid workspace URL and "
               "that OAuth is enabled on this account."
               )
        logger.error(msg)
        raise RuntimeError(msg)
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"Unable to decode OAuth configuration from {idp_url}.\n"
                     "Verify it is a valid workspace URL and that OAuth is "
                     "enabled on this account.")
        raise e


def get_idp_url(host):
    maybe_scheme = "https://" if not host.startswith("https://") else ""
    maybe_trailing_slash = "/" if not host.endswith("/") else ""
    return f"{maybe_scheme}{host}{maybe_trailing_slash}{OIDC_REDIRECTOR_PATH}"


def get_challenge(verifier_string=token_urlsafe(32)):
    digest = hashlib.sha256(verifier_string.encode("UTF-8")).digest()
    challenge_string = base64.urlsafe_b64encode(digest).decode("UTF-8").replace("=", "")
    return verifier_string, challenge_string


# This is a janky global that is used to store the path of the single request the HTTP server
# will receive.
global_request_path = None


def set_request_path(path):
    global global_request_path
    global_request_path = path


class SingleRequestHandler(BaseHTTPRequestHandler):
    RESPONSE_BODY = """<html>
<head>
  <title>Close this Tab</title>
  <style>
    body {
      font-family: "Barlow", Helvetica, Arial, sans-serif;
      padding: 20px;
      background-color: #f3f3f3;
    }
  </style>
</head>
<body>
  <h1>Please close this tab.</h1>
  <p>
    The Databricks Python Sql Connector received a response. You may close this tab.
  </p>
</body>
</html>""".encode("utf-8")

    def do_GET(self):  # nopep8
        self.send_response(200, "Success")
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(self.RESPONSE_BODY)
        set_request_path(self.path)

    def log_message(self, format, *args):
        #pylint: disable=redefined-builtin
        #pylint: disable=unused-argument
        return


def get_authorization_code(client, auth_url, redirect_url, scope, state, challenge, port):
    (auth_req_uri, _, _) = client.prepare_authorization_request(
        authorization_url=auth_url,
        redirect_url=redirect_url,
        scope=scope,
        state=state,
        code_challenge=challenge,
        code_challenge_method="S256")
    logger.info(f"Opening {auth_req_uri}")

    with HTTPServer(("", port), SingleRequestHandler) as httpd:
        webbrowser.open_new(auth_req_uri)
        logger.info(f"Listening for OAuth authorization callback at {redirect_url}")
        httpd.handle_request()

    if not global_request_path:
        msg = f"No path parameters were returned to the callback at {redirect_url}"
        logger.error(msg)
        raise RuntimeError(msg)
    # This is a kludge because the parsing library expects https callbacks
    # We should probably set it up using https
    full_redirect_url = f"https://localhost:{port}/{global_request_path}"
    try:
        authorization_code_response = \
            client.parse_request_uri_response(full_redirect_url, state=state)
    except OAuth2Error as e:
        logger.error(f"OAuth Token Request error {e.description}")
        raise e
    return authorization_code_response


def send_auth_code_token_request(client, token_request_url, redirect_url, code, verifier):
    token_request_body = client.prepare_request_body(code=code, redirect_uri=redirect_url)
    data = f"{token_request_body}&code_verifier={verifier}"
    return send_token_request(token_request_url, data)


def send_token_request(token_request_url, data):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(url=token_request_url, data=data, headers=headers)
    return response.json()


def send_refresh_token_request(hostname, client_id, refresh_token):
    idp_url = get_idp_url(hostname)
    oauth_config = fetch_well_known_config(idp_url)
    token_request_url = oauth_config["token_endpoint"]
    client = oauthlib.oauth2.WebApplicationClient(client_id)
    token_request_body = client.prepare_refresh_body(
        refresh_token=refresh_token, client_id=client.client_id)
    return send_token_request(token_request_url, token_request_body)


def get_tokens_from_response(oauth_response):
    access_token = oauth_response["access_token"]
    refresh_token = oauth_response["refresh_token"] if "refresh_token" in oauth_response else None
    return access_token, refresh_token


def check_and_refresh_access_token(hostname, access_token, refresh_token):
    now = datetime.now(tz=UTC)
    # If we can't decode an expiration time, this will be expired by default.
    expiration_time = now
    try:
        # This token has already been verified and we are just parsing it.
        # If it has been tampered with, it will be rejected on the server side.
        # This avoids having to fetch the public key from the issuer and perform
        # an unnecessary signature verification.
        decoded = json.loads(base64.standard_b64decode(access_token.split(".")[1]))
        expiration_time = datetime.fromtimestamp(decoded["exp"], tz=UTC)
    except Exception as e:
        logger.error(e)
        raise e

    if expiration_time > now:
        # The access token is fine. Just return it.
        return access_token, refresh_token, False

    if not refresh_token:
        msg = f"OAuth access token expired on {expiration_time}."
        logger.error(msg)
        raise RuntimeError(msg)

    # Try to refresh using the refresh token
    logger.debug(f"Attempting to refresh OAuth access token that expired on {expiration_time}")
    oauth_response = send_refresh_token_request(hostname, refresh_token)
    fresh_access_token, fresh_refresh_token = get_tokens_from_response(oauth_response)
    return fresh_access_token, fresh_refresh_token, True


def get_tokens(hostname, client_id, scope=None):
    idp_url = get_idp_url(hostname)
    oauth_config = fetch_well_known_config(idp_url)
    # We are going to override oauth_config["authorization_endpoint"] use the
    # /oidc redirector on the hostname, which may inject additional parameters.
    auth_url = f"{hostname}oidc/v1/authorize"
    state = token_urlsafe(16)
    (verifier, challenge) = get_challenge()
    client = oauthlib.oauth2.WebApplicationClient(client_id)
    redirect_url = get_redirect_url()
    try:
        auth_response = get_authorization_code(
            client,
            auth_url,
            redirect_url,
            scope,
            state,
            challenge,
            REDIRECT_PORT)
    except OAuth2Error as e:
        msg = f"OAuth Authorization Error: {e.description}"
        logger.error(msg)
        raise e

    token_request_url = oauth_config["token_endpoint"]
    code = auth_response["code"]
    oauth_response = \
        send_auth_code_token_request(client, token_request_url, redirect_url, code, verifier)
    return get_tokens_from_response(oauth_response)
