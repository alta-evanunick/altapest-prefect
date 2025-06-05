import os
from functools import lru_cache
import snowflake.connector


@lru_cache
def get_snowflake_connection_params():
    """
    Return Snowflake connection parameters from environment variables.
    """
    return {
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "database": os.environ.get("RAW_DB_DATABASE", "RAW_DB"),
        "schema": os.environ.get("RAW_DB_SCHEMA", "REF"),
    }


@lru_cache
def load_office_metadata():
    """
    Load office metadata (office_id, base_url, key_lookup, token_lookup)
    from the REF.offices_lookup table in Snowflake.
    """
    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cur = conn.cursor()
    cur.execute(
        "SELECT office_id, base_url, key_lookup, token_lookup FROM offices_lookup"
    )
    df = cur.fetch_pandas_all()
    cur.close()
    conn.close()
    return df


@lru_cache
def load_entity_metadata():
    """
    Load entity metadata (entity_name, endpoint, date_fields, is_global)
    from the REF.entity_lookup table in Snowflake.
    """
    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cur = conn.cursor()
    cur.execute(
        "SELECT entity_name, endpoint, date_fields, is_global FROM entity_lookup"
    )
    df = cur.fetch_pandas_all()
    cur.close()
    conn.close()
    return df