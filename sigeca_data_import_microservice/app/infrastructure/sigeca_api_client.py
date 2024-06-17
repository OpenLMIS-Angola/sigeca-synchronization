import requests
import json
import logging
from requests.auth import HTTPBasicAuth


class SigecaApiClient:
    LOGIN_URI = "oauth/token?grant_type=password"
    FACILITIES_URI = "facilities"
    GEOGRAPHICAL_ZONES_URI = "geographicZones"

    def __init__(self, api_config: dict):
        api_url: str = api_config["api_url"]
        headers: str = api_config["headers"]

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
        with open("./mocked_facilities.json", "r", encoding="UTF-8") as f:
            data = json.load(f)
            return data["facilities"]
