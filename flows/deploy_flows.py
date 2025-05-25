import datetime
from typing import Dict, List
from prefect import flow, task, get_run_logger
from prefect_snowflake import SnowflakeConnector
from prefect.task_runners import ConcurrentTaskRunner
from flows.fieldroutes_etl_flow import fetch_entity, ENTITY_META

# Define high-velocity entities for CDC
CDC_ENTITIES = [
    "appointment", "ticket", "ticketItem", "payment", 
    "appliedPayment", "route", "subscription"
]

@flow(name="FieldRoutes_Nightly_ETL", task_runner=ConcurrentTaskRunner())
def run_nightly_fieldroutes_etl():
    """Prefect flow to perform a full nightly extract for all offices and entities."""
    logger = get_run_logger()
    now = datetime.datetime.now(datetime.UTC)
    window_end = now
    window_start = now - datetime.timedelta(days=1)
    
    # Retrieve offices and watermarks
    sf_block = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_block.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT o.office_id, o.office_name, o.base_url,
                   o.secret_block_name_key, o.secret_block_name_token,
                   w.entity_name, w.last_run_utc
            FROM   RAW.REF.offices_lookup o
            JOIN   RAW.REF.office_entity_watermark w USING (office_id)
            WHERE  o.active = TRUE;
        """)
        rows = cur.fetchall()
    
    # Organize office data
    offices: Dict[int, Dict] = {}
    for office_id, name, url, key_blk, tok_blk, entity_name, last_run in rows:
        from prefect.blocks.system import Secret
        offices.setdefault(office_id, {
            "office_id": office_id,
            "office_name": name,
            "base_url": url,
            "auth_key": Secret.load(key_blk).get(),
            "auth_token": Secret.load(tok_blk).get(),
            "watermarks": {}
        })
        offices[office_id]["watermarks"][entity_name] = last_run
    
    logger.info(f"Starting nightly FieldRoutes ETL for {len(offices)} offices")
    
    # Prepare entity metadata
    meta_dict = {
        meta[0]: {
            "endpoint": meta[0],
            "table": meta[1],
            "is_dim": meta[2],
            "small": meta[3],
            "date_field": meta[4]
        }
        for meta in ENTITY_META
    }
    
    # Submit tasks concurrently
    futures = []
    for office in offices.values():
        for entity_name, meta in meta_dict.items():
            future = fetch_entity.submit(
                office=office,
                meta=meta,
                window_start=window_start,
                window_end=window_end
            )
            futures.append((office["office_id"], entity_name, future))
    
    # Wait for results - any failure will stop the entire flow
    # NOTE: This is intentional - if one office fails due to API issues,
    # all offices are likely to fail, so we fail fast to avoid hammering the API
    for office_id, entity, future in futures:
        future.result()  # Will raise if any task failed
    
    logger.info("✅ Nightly FieldRoutes ETL completed successfully")

@flow(name="FieldRoutes_CDC_ETL", task_runner=ConcurrentTaskRunner())
def run_cdc_fieldroutes_etl():
    """CDC flow for high-velocity tables using watermarks."""
    logger = get_run_logger()
    
    # Get offices and their CDC watermarks
    sf_block = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_block.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT o.office_id, o.office_name, o.base_url,
                   o.secret_block_name_key, o.secret_block_name_token,
                   w.entity_name, w.last_run_utc
            FROM   RAW.REF.offices_lookup o
            JOIN   RAW.REF.office_entity_watermark w USING (office_id)
            WHERE  o.active = TRUE
              AND  w.entity_name IN ({})
        """.format(','.join(f"'{e}'" for e in CDC_ENTITIES)))
        rows = cur.fetchall()
    
    # Organize office data with CDC-specific watermarks
    offices: Dict[int, Dict] = {}
    for office_id, name, url, key_blk, tok_blk, entity_name, last_run in rows:
        from prefect.blocks.system import Secret
        offices.setdefault(office_id, {
            "office_id": office_id,
            "office_name": name,
            "base_url": url,
            "auth_key": Secret.load(key_blk).get(),
            "auth_token": Secret.load(tok_blk).get(),
            "watermarks": {}
        })
        offices[office_id]["watermarks"][entity_name] = last_run
    
    # Prepare CDC entity metadata
    meta_dict = {
        meta[0]: {
            "endpoint": meta[0],
            "table": meta[1],
            "is_dim": meta[2],
            "small": meta[3],
            "date_field": meta[4]
        }
        for meta in ENTITY_META if meta[0] in CDC_ENTITIES
    }
    
    now = datetime.datetime.now(datetime.UTC)
    
    # Submit CDC tasks with proper windows based on watermarks
    futures = []
    for office in offices.values():
        for entity_name, meta in meta_dict.items():
            # Use watermark as start time, with 15-minute overlap for safety
            last_run = office["watermarks"].get(entity_name)
            if last_run:
                window_start = last_run - datetime.timedelta(minutes=15)
            else:
                # First run - get last 2 hours
                window_start = now - datetime.timedelta(hours=2)
            
            future = fetch_entity.submit(
                office=office,
                meta=meta,
                window_start=window_start,
                window_end=now
            )
            futures.append((office["office_id"], entity_name, future))
    
    logger.info(f"Running CDC for {len(futures)} office-entity combinations")
    
    # Wait for results - any failure will stop the entire flow
    # NOTE: This is intentional - if one office fails due to API issues,
    # all offices are likely to fail, so we fail fast to avoid hammering the API
    for office_id, entity, future in futures:
        future.result()  # Will raise if any task failed
    
    logger.info("✅ CDC FieldRoutes ETL completed successfully")

if __name__ == "__main__":
    # For local testing
    run_nightly_fieldroutes_etl()