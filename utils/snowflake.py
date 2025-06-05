import json
from typing import Iterable

import snowflake.connector

from config import get_snowflake_connection_params


def bulk_insert_json(
    table: str,
    rows: Iterable[str],
) -> None:
    """
    Bulk insert JSON strings into RAW_DB.RAW.<table>.
    """
    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(
        **params, database=params.get("database", "RAW_DB"), schema="RAW"
    )
    cur = conn.cursor()
    values = [(row,) for row in rows]
    placeholders = ",".join(["(%s)" for _ in values])
    sql = f"INSERT INTO {table} (raw_json) VALUES " + placeholders
    cur.execute(sql, [v for val in values for v in val])
    cur.close()
    conn.close()