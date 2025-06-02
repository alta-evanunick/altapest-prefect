from __future__ import annotations
"""
FieldRoutes CDC (Change Data Capture) Flow - Direct to Snowflake Version
Handles incremental updates for high-velocity entities during business hours
"""
import datetime
from datetime import timezone
from typing import Dict, List
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeConnector
from .fieldroutes_etl_flow_snowflake import fetch_entity, ENTITY_META

# Define which entities are "high-velocity" and need CDC
# Updated based on FR_Entity_Matrix.csv velocity designations
HIGH_VELOCITY_ENTITIES = {
    "customer", "appointment", "ticket", "payment", 
    "appliedPayment", "task", "note", "knock",
    "subscription", "appliedPayment"
}

@flow(
    name="FieldRoutes_CDC_Flow_Snowflake",
    description="CDC flow for incremental updates directly to Snowflake"
)
def run_cdc_fieldroutes_etl(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window_hours: float = 2.25
):
    """
    CDC flow for incremental updates every 2 hours during business hours.
    Only processes high-velocity entities that change frequently.
    Writes directly to Snowflake, bypassing Azure.
    
    Args:
        start_date: Start date for data extraction (YYYY-MM-DD format)
        end_date: End date for data extraction (YYYY-MM-DD format) 
        window_hours: Hours to look back (default 2.25 for 2h15m window)
    """
    logger = get_run_logger()
    
    # Calculate time window
    now = datetime.datetime.now(timezone.utc)
    
    if start_date and end_date:
        # Use provided dates
        try:
            # Parse dates and add time components
            window_start = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
            )
            window_end = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
            )
            logger.info(f"Running CDC with custom date range: {start_date} to {end_date}")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Error: {e}")
    else:
        # Default CDC window: last 2 hours with 15-minute overlap for safety
        window_start = now - datetime.timedelta(hours=window_hours)
        window_end = now
    
    logger.info(f"Starting CDC FieldRoutes ETL (Direct to Snowflake). Window: {window_start} to {window_end}")
    
    # Get office configuration
    sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_connector.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT o.office_id, o.office_name, o.base_url,
                   o.secret_block_name_key, o.secret_block_name_token
            FROM RAW.REF.offices_lookup o
            WHERE o.active = TRUE
        """)
        offices = []
        for office_id, name, url, key_blk, tok_blk in cursor.fetchall():
            offices.append({
                "office_id": office_id,
                "office_name": name,
                "base_url": url,
                "auth_key": Secret.load(key_blk).get(),
                "auth_token": Secret.load(tok_blk).get(),
            })
    
    # Filter entity metadata to only high-velocity entities
    cdc_entities = []
    for meta in ENTITY_META:
        if meta[0] not in HIGH_VELOCITY_ENTITIES:
            continue
            
        # Unpack metadata tuple - all tuples have exactly 7 elements
        entity_dict = {
            "endpoint": meta[0], 
            "table": meta[1], 
            "is_dim": meta[2], 
            "small": meta[3], 
            "primary_date": meta[4],
            "secondary_date": meta[5],
            "unique_params": meta[6]
        }
        cdc_entities.append(entity_dict)
    
    logger.info(f"Processing {len(cdc_entities)} high-velocity entities for {len(offices)} offices")
    
    # Track metrics
    total_fetched = 0
    total_loaded = 0
    failed_count = 0
    
    # Process each office/entity pair
    for office in offices:
        logger.info(f"🏢 Processing office {office['office_id']} ({office['office_name']})")
        
        for meta in cdc_entities:
            try:
                fetched, loaded = fetch_entity(
                    office=office,
                    meta=meta,
                    window_start=window_start,
                    window_end=window_end,
                )
                
                total_fetched += fetched
                total_loaded += loaded
                
                logger.info(
                    f"✅ CDC {meta['endpoint']} completed for office {office['office_id']}: "
                    f"{fetched} fetched, {loaded} loaded"
                )
            except Exception as exc:
                logger.error(
                    f"❌ CDC {meta['endpoint']} failed for office {office['office_id']}: {exc}"
                )
                failed_count += 1
    
    # Summary
    logger.info(f"""
    ========== CDC COMPLETE ==========
    Total records fetched: {total_fetched}
    Total records loaded: {total_loaded}
    Failed tasks: {failed_count}
    Window: {window_start} to {window_end}
    ==================================
    """)
    
    if failed_count > 0:
        raise RuntimeError(f"{failed_count} CDC tasks failed. Check logs for details.")


if __name__ == "__main__":
    run_cdc_fieldroutes_etl()