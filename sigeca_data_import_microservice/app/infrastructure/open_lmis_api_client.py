import requests
import json
import logging
from requests.auth import HTTPBasicAuth


class OpenLmisApiClient:
    LOGIN_URI = "oauth/token?grant_type=password"
    FACILITIES_URI = "facilities"
    GEOGRAPHICAL_ZONES_URI = "geographicZones"

    def __init__(self, api_config: dict):
        api_url: str = api_config["api_url"]
        username: str = api_config["username"]
        password: str = api_config["password"]
        login_token: str = api_config["login_token"]

        if api_url.endswith("/"):
            api_url = api_url[:-1]

        self.api_url = api_url
        self.username = username
        self.password = password
        self.login_token = login_token
        self.token = None
        self.headers = {
            # Required for avoid openLMIS 401 Unauthorized during login
            "Authentication": f"Basic {login_token}",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
        }

    def login(self):
        """Login to get access token"""
        url = f"{self.api_url}/{self.LOGIN_URI}"
        headers = {
            "Authorization": f"Basic {self.login_token}",
        }
        data = {"username": f"{self.username}", "password": f"{self.password}"}

        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            self.token = response.json().get("access_token")
            self.headers["Authorization"] = f"Bearer {self.token}"
            logging.info("Logged in successfully")
        else:
            logging.error(
                f"Failed to log into OpenLMIS API: {response.status_code} {response}"
            )
            raise Exception("Failed to log in")

    def send_post_request(self, uri, payload: str):
        url = f"{self.api_url}/{uri}"
        self.headers["Content-Type"] = "application/json"
        response = requests.post(url, data=payload, headers=self.headers)
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                f"Failed to POST {url}: {response.status_code} {response.text}"
            )

    def send_put_request(self, uri: str, id: str, payload: str):
        url = f"{self.api_url}/{uri}/{id}"
        self.headers["Content-Type"] = "application/json"
        response = requests.put(url, data=payload, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to PUT {url}: {response.status_code} {response.text}"
            )

    def send_delete_request(self, uri: str, id: str):
        url = f"{self.api_url}/{uri}/{id}"
        self.headers["Content-Type"] = "application/json"
        response = requests.delete(url, headers=self.headers)
        if response.status_code == 204:
            return
        else:
            raise Exception(
                f"Failed to DELETE {url}: {response.status_code} {response.text}"
            )
