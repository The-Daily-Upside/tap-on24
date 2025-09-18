import requests
from typing import Dict, Any, Optional

class ON24Client:
    """Client for ON24 REST API."""
    BASE_URL = "https://api.on24.com/v2/client/{client_id}/event"

    def __init__(self, client_id: str, access_token_key: str, access_token_secret: str):
        self.client_id = client_id
        self.access_token_key = access_token_key
        self.access_token_secret = access_token_secret

    def get_headers(self) -> Dict[str, str]:
        return {
            "accessTokenKey": self.access_token_key,
            "accessTokenSecret": self.access_token_secret,
            "Accept": "application/json"
        }