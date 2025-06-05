from typing import Any, Dict, List

import json
import snowflake.connector
from prefect import task

from config import get_snowflake_connection_params
from utils.snowflake import bulk_insert_json


@task
def stage_raw_json(
    office_id: int,
    entity_name: str,
    records: List[Dict[str, Any]],
) -> None:
    """
    Stage raw JSON records into RAW_DB.RAW.<entity_name>.
    """
    rows = [json.dumps(rec) for rec in records]
    bulk_insert_json(f"RAW.{entity_name}", rows)


@task
def update_watermark(
    office_id: int,
    entity_name: str,
    last_id: int,
) -> None:
    """
    Update watermark (e.g. last processed ID) for office/entity in REF.office_entity_watermark.
    """
    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE office_entity_watermark
        SET last_id = %s
        WHERE office_id = %s
          AND entity_name = %s
        """,
        (last_id, office_id, entity_name),
    )
    conn.commit()
    cur.close()
    conn.close()