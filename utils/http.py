import time
from typing import Any, Dict

import requests


def http_get(
    url: str,
    headers: Dict[str, str] = None,
    params: Dict[str, Any] = None,
    retries: int = 3,
    backoff: float = 1.0,
) -> Any:
    """
    Perform HTTP GET with retries and exponential backoff.
    """
    headers = headers or {}
    params = params or {}
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception:
            if attempt >= retries:
                raise
            time.sleep(backoff * attempt)