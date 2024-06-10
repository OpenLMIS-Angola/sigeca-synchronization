import requests
from typing import Any, Dict
from .utils import ChangeLogOperationEnum

class ResourceAPIClient:
    def __init__(self, api_url: str, token: str):
        self.api_url = api_url
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'}

    def send_data(self, resource: str, operation: ChangeLogOperationEnum, data: Dict[str, Any]) -> Dict[str, Any]:
        endpoint = f"{self.api_url}/{resource}/{operation.value}"
        response = requests.post(endpoint, json=data, headers=self.headers)
        return response.json()
