import requests
import logging


class SigecaApiClient:
    LOGIN_URI = "token/"
    SYNC_URI = "sync/"

    def __init__(self, api_config: dict):
        api_url: str = api_config["api_url"]
        headers: str = api_config["headers"]
        self.credentials = api_config['credentials']
        self.skip_verification: bool = api_config.get('skip_verification') or False

        if api_url.endswith("/"):
            api_url = api_url[:-1]

        self.api_url = api_url
        self.headers = {
            # Required for avoid openLMIS 401 Unauthorized during login
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
            **headers,
        }

    def sync(self, json_data=None):
        if not isinstance(json_data, list):
            logging.error(
                f"Attempt to sync invalid payload: {json_data}"
            )
            raise Exception("Attempt to sync invalid payload")

        print(self.skip_verification)

        self._get_token()
        url = f"{self.api_url}/{self.SYNC_URI}"

        response = requests.post(url, json=json_data, headers=self.headers, verify=not self.skip_verification)

        if response.status_code == 200:
            return True
        else:
            logging.error(
                f"Failed to sync data with sigeca central API: {response.status_code} {response}"
            )
            raise Exception("Failed to sync data with sigeca central API")

    def _get_token(self):
        """Login to get access token"""
        url = f"{self.api_url}/{self.LOGIN_URI}"
        data = self.credentials

        response = requests.post(url, headers=self.headers, data=data, verify=not self.skip_verification)

        if response.status_code == 200:
            self.token = response.json().get("access_token")
            self.headers['Authorization'] = F"Bearer {self.token}"
        else:
            logging.error(
                f"Failed to log into OpenLMIS API: {response.status_code} {response}"
            )
            raise Exception("Failed to log into sigeca central API")