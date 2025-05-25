from __future__ import annotations
"""
FieldRoutes CDC (Change Data Capture) Flow
Handles incremental updates for high-velocity entities during business hours
"""

import datetime
from typing import Dict, List
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeConnector
from .fieldroutes_etl_flow import fetch_entity, ENTITY_META

# Define which entities are "high-velocity" and need CDC
HIGH_VELOCITY_ENTITIES = {
    "appointment", "ticket", "ticketItem", "payment", 
    "appliedPayment", "task", "note", "flagAssignment"
}

@flow(name="FieldRoutes_CDC_Flow")
def run_cdc_fieldroutes_etl():
    """
    CDC flow for incremental updates every 2 hours during business hours.
    Only processes high-velocity entities that change frequently.
    """
    logger = get_run_logger()
    
    # CDC window: last 2 hours with 15-minute overlap for safety
    now = datetime.datetime.now(datetime.UTC)
    window_start = now - datetime.timedelta(hours=2, minutes=15)
    window_end = now
    
    logger.info(f"Starting CDC FieldRoutes ETL. Window: {window_start} to {window_end}")
    
    # Get office configuration
    sf = SnowflakeConnector.load("snowflake-altapestdb").get_connection()
    with sf as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT o.office_id, o.office_name, o.base_url,
                   o.secret_block_name_key, o.secret_block_name_token
            FROM   Ref.offices_lookup o
            WHERE  o.is_active = TRUE;
        """)
        offices = []
        for office_id, name, url, key_blk, tok_blk in cur.fetchall():
            offices.append({
                "office_id": office_id,
                "office_name": name,
                "base_url": url,
                "auth_key": Secret.load(key_blk).get(),
                "auth_token": Secret.load(tok_blk).get(),
            })
    
    # Filter entity metadata to only high-velocity entities
    cdc_entities = [
        {"endpoint": ep, "table": tbl, "is_dim": dim, "small": small, "date_field": df}
        for ep, tbl, dim, small, df in ENTITY_META
        if ep in HIGH_VELOCITY_ENTITIES
    ]
    
    logger.info(f"Processing {len(cdc_entities)} high-velocity entities for {len(offices)} offices")
    
    # Submit tasks for parallel execution
    futures = []
    for office in offices:
        for meta in cdc_entities:
            fut = fetch_entity.submit(
                office=office,
                meta=meta,
                window_start=window_start,
                window_end=window_end,
            )
            futures.append(fut)
    
    # Wait for all tasks to complete
    failed_count = 0
    for fut in futures:
        try:
            fut.result()
        except Exception as exc:
            logger.error(f"CDC task failed: {exc}")
            failed_count += 1
    
    if failed_count > 0:
        logger.warning(f"CDC flow completed with {failed_count} failures")
    else:
        logger.info("✅ CDC FieldRoutes ETL flow completed successfully")

if __name__ == "__main__":
    run_cdc_fieldroutes_etl()