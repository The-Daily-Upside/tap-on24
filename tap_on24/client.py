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
        import time, logging
        url = self.BASE_URL.format(client_id=self.client_id)
        params = {
            "itemsPerPage": items_per_page,
            "pageOffset": page_offset
        }
        if start_date:
            params["startDate"] = start_date
        MAX_RETRIES = 5
        backoff = 2
        for attempt in range(MAX_RETRIES):
            response = requests.get(url, headers=self.get_headers(), params=params)
            if response.status_code == 429:
                logging.warning(f"429 Too Many Requests for events (attempt {attempt+1}), backing off {backoff} seconds.")
                time.sleep(backoff)
                backoff *= 2
                continue
            response.raise_for_status()
            return response.json()
        raise Exception("Max retries exceeded for events endpoint due to throttling.")
    
    def get_attendees(self, event_id: int, items_per_page: int = 100, page_offset: int = 0) -> Dict[str, Any]:
        import time, logging
        url = f"{self.BASE_URL.format(client_id=self.client_id)}/{event_id}/attendee"
        params = {
            "itemsPerPage": items_per_page,
            "pageOffset": page_offset
        }
        MAX_RETRIES = 5
        backoff = 2
        for attempt in range(MAX_RETRIES):
            response = requests.get(url, headers=self.get_headers(), params=params)
            if response.status_code == 429:
                logging.warning(f"429 Too Many Requests for attendees (event {event_id}, page {page_offset}, attempt {attempt+1}), backing off {backoff} seconds.")
                time.sleep(backoff)
                backoff *= 2
                continue
            response.raise_for_status()
            return response.json()
        raise Exception(f"Max retries exceeded for attendees endpoint (event {event_id}) due to throttling.")

    def get_registrants(self, event_id: int, items_per_page: int = 100, page_offset: int = 0) -> Dict[str, Any]:
        import time, logging
        url = f"{self.BASE_URL.format(client_id=self.client_id)}/{event_id}/registrant"
        params = {
            "itemsPerPage": items_per_page,
            "pageOffset": page_offset
        }
        MAX_RETRIES = 5
        backoff = 2
        for attempt in range(MAX_RETRIES):
            response = requests.get(url, headers=self.get_headers(), params=params)
            if response.status_code == 429:
                logging.warning(f"429 Too Many Requests for registrants (event {event_id}, page {page_offset}, attempt {attempt+1}), backing off {backoff} seconds.")
                time.sleep(backoff)
                backoff *= 2
                continue
            response.raise_for_status()
            return response.json()
        raise Exception(f"Max retries exceeded for registrants endpoint (event {event_id}) due to throttling.")