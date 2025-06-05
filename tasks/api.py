from typing import Any, Dict, List, Optional

from prefect import task

import json
from config import load_office_metadata, load_entity_metadata
from utils.http import http_get
from utils.pagination import chunk_list


@task
def search_ids(
    office_id: int,
    entity_meta: Dict[str, Any],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[int]:
    """
    Retrieve IDs for an entity via the /search endpoint, handling 50K pagination.
    """
    offices = load_office_metadata()
    office = offices.loc[offices.office_id == office_id].iloc[0]
    base_url = office.base_url
    params: Dict[str, Any] = {}
    if start_date:
        params[entity_meta["date_fields"][0]] = start_date
    if end_date:
        params[entity_meta["date_fields"][1]] = end_date
    all_ids: List[int] = []
    url = f"{base_url}/{entity_meta['endpoint']}/search"
    while True:
        resp = http_get(url, params=params)
        ids = resp.get("entityIds", [])
        all_ids.extend(ids)
        if len(ids) < 50000:
            break
        params['idGreaterThan'] = max(ids)
    return all_ids


@task
def get_records(
    office_id: int,
    entity_meta: Dict[str, Any],
    ids: List[int],
) -> List[Dict[str, Any]]:
    """
    Retrieve full records for IDs via the /get endpoint in 1000-ID batches.
    """
    offices = load_office_metadata()
    office = offices.loc[offices.office_id == office_id].iloc[0]
    base_url = office.base_url
    url = f"{base_url}/{entity_meta['endpoint']}/get"
    records: List[Dict[str, Any]] = []
    for chunk in chunk_list(ids, 1000):
        params = {"ids": ",".join(map(str, chunk))}
        resp = http_get(url, params=params)
        records.extend(resp.get("entities", []))
    return records