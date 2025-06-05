from typing import List, Optional

from prefect import flow

import pandas as pd

from config import load_office_metadata, load_entity_metadata
from tasks.api import search_ids, get_records
from tasks.snowflake import stage_raw_json, update_watermark


@flow(name="ETL_Full_or_CDC")
def etl_flow(
    office_ids: Optional[List[int]] = None,
    entity_names: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    """
    Run search+get ETL for specified offices and entities.
    """
    offices = load_office_metadata()
    entities = load_entity_metadata()
    if office_ids:
        offices = offices[offices.office_id.isin(office_ids)]
    if entity_names:
        entities = entities[entities.entity_name.isin(entity_names)]

    for _, office in offices.iterrows():
        for _, ent in entities.iterrows():
            ids = search_ids.submit(
                office.office_id, ent, start_date, end_date
            ).result()
            if not ids:
                continue
            records = get_records.submit(
                office.office_id, ent, ids
            ).result()
            stage_raw_json.submit(
                office.office_id, ent.entity_name, records
            )
            update_watermark.submit(
                office.office_id, ent.entity_name, max(ids)
            )