import logging
import json
logger = logging.getLogger(__name__)


class OAuthToken:
    def __init__(self, access_token, refresh_token):
        self._access_token = access_token
        self._refresh_token = refresh_token

    def get_access_token(self) -> str:
        return self._access_token

    def get_refresh_token(self) -> str:
        return self._refresh_token


class OAuthPersistence:
    def persist(self, oauth_token: OAuthToken):
        pass

    def read(self) -> OAuthToken:
        pass


# Note this is only intended to be used for development
class DevOnlyFilePersistence(OAuthPersistence):

    def __init__(self, file_path):
        self._file_path = file_path

    def persist(self, token: OAuthToken):
        logger.info(f"persisting token in {self._file_path}")

        # Data to be written
        dictionary = {
            "refresh_token": token.get_refresh_token(),
            "access_token": token.get_access_token()
        }

        # Serializing json
        json_object = json.dumps(dictionary, indent=4)

        with open(self._file_path, "w") as outfile:
            outfile.write(json_object)

    def read(self) -> OAuthToken:
        # TODO: validate the
        try:
            with open(self._file_path, "r") as infile:
                json_as_string = infile.read()

                token_as_json = json.loads(json_as_string)
                return OAuthToken(token_as_json['access_token'], token_as_json['refresh_token'])
        except Exception as e:
            return None
