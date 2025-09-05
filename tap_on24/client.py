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

    def get_events(self, start_date: Optional[str] = None, end_date: Optional[str] = None,
                   items_per_page: int = 100, page_offset: int = 0) -> Dict[str, Any]:
        url = self.BASE_URL.format(client_id=self.client_id)
        params = {
            "itemsPerPage": items_per_page,
            "pageOffset": page_offset
        }
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        response = requests.get(url, headers=self.get_headers(), params=params)
        response.raise_for_status()
        return response.json()