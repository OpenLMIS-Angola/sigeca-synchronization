import requests
import json
import logging
from requests.auth import HTTPBasicAuth


class SigecaApiClient:
    LOGIN_URI = "token/"
    FACILITIES_URI = "facilities/"
    GEOGRAPHICAL_ZONES_URI = "geographicZones"

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

    def fetch_facilities(self, **kwargs):
        # TODO: Add implementation of the functionality after the API Endpoint is avilable
        self._get_token()
        url = f"{self.api_url}/{self.FACILITIES_URI}"

        response = requests.get(url, headers=self.headers, verify=not self.skip_verification)

        if response.status_code == 200:
            return response.json() 
        else:
            logging.error(
                f"Failed to log into OpenLMIS API: {response.status_code} {response}"
            )
            raise Exception("Failed to log into sigeca central API")


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