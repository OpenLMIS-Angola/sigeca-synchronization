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

        print(response.status_code)

        if response.status_code == 200:
            self.token = response.json().get("access_token")
            self.headers["Authorization"] = f"Bearer {self.token}"
            logging.info("Logged in successfully")
        else:
            logging.error(
                f"Failed to log into OpenLMIS API: {response.status_code} {response}"
            )
            raise Exception("Failed to log in")

    def create_facility(self, facility_data):
        """Create a new facility"""
        url = f"{self.api_url}/{self.FACILITIES_URI}"
        response = requests.post(
            url, data=json.dumps(facility_data), headers=self.headers
        )
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                f"Failed to create facility: {response.status_code} {response.text}"
            )

    def get_facilities(self):
        """Create a new facility"""
        url = f"{self.api_url}/{self.FACILITIES_URI}"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 201:
            print(response.json())
        else:
            raise Exception(
                f"Failed to create facility: {response.status_code} {response.text}"
            )

    def create_geo_zone(self, geo_zone_data):
        """Create a new geographic zone"""
        url = f"{self.api_url}/{self.GEOGRAPHICAL_ZONES_URI}"
        response = requests.post(
            url, data=json.dumps(geo_zone_data), headers=self.headers
        )
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                f"Failed to create geographic zone: {response.status_code} {response.text}"
            )

    def update_facility(self, facility_id, facility_data):
        """Update an existing facility"""
        url = f"{self.api_url}/facilities/{facility_id}"
        response = requests.put(
            url, data=json.dumps(facility_data), headers=self.headers
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to update facility: {response.status_code} {response.text}"
            )

    def delete_facility(self, facility_id):
        """Delete an existing facility"""
        url = f"{self.api_url}/facilities/{facility_id}"
        response = requests.delete(url, headers=self.headers)
        if response.status_code == 204:
            return {"status": "success", "message": "Facility deleted successfully"}
        else:
            raise Exception(
                f"Failed to delete facility: {response.status_code} {response.text}"
            )
