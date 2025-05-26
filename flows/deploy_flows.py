import datetime
import time
from typing import Dict, List
from prefect import flow, task, get_run_logger
from prefect_snowflake import SnowflakeConnector
from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
from flows.fieldroutes_etl_flow import fetch_entity, ENTITY_META

# Define high-velocity entities for CDC
CDC_ENTITIES = [
    "appointment", "ticket", "ticketItem", "payment", 
    "appliedPayment", "route", "subscription"
]

@flow(name="FieldRoutes_Nightly_ETL", task_runner=SequentialTaskRunner())
def run_nightly_fieldroutes_etl():
    """Prefect flow to perform a full nightly extract for all offices and entities.
    Processes offices sequentially to avoid overwhelming the FieldRoutes API."""
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
            WHERE  o.active = TRUE
            ORDER BY o.office_id;
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
    
    # Sort offices by ID for predictable processing order
    sorted_offices = sorted(offices.values(), key=lambda x: x["office_id"])
    
    logger.info(f"Starting nightly FieldRoutes ETL for {len(sorted_offices)} offices")
    logger.info(f"Offices to process: {[f'{o['office_id']} ({o['office_name']})' for o in sorted_offices]}")

    
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

    total_success = 0
    total_failed = 0
    
    # Process offices sequentially
    for office_idx, office in enumerate(sorted_offices, 1):
        logger.info(f"🏢 Processing office {office_idx}/{len(sorted_offices)}: {office['office_id']} ({office['office_name']})")
        
        office_success = 0
        office_failed = 0

        # Process each entity sequentially for this office
        for entity_name, meta in meta_dict.items():
            try:
                fetch_entity(
                    office=office,
                    meta=meta,
                    window_start=window_start,
                    window_end=window_end
                )
                office_success += 1
                logger.info(
                    f"✅ {entity_name} completed for office {office['office_id']}"
                )
            except Exception as exc:
                office_failed += 1
                logger.error(
                    f"❌ {entity_name} failed for office {office['office_id']}: {exc}"
                )


        # Log office completion summary
        logger.info(f"🏢 Office {office['office_id']} completed: {office_success} success, {office_failed} failed")
        total_success += office_success
        total_failed += office_failed
        
        # Brief pause between offices to be API-friendly
        if office_idx < len(sorted_offices):  # Don't sleep after the last office
            logger.info("⏸️  Pausing 30 seconds before next office...")
            time.sleep(30)
    
    # Final summary
    logger.info("="*60)
    logger.info("🎯 NIGHTLY ETL SUMMARY")
    logger.info("="*60)
    logger.info(f"Total entities processed: {total_success + total_failed}")
    logger.info(f"✅ Successful: {total_success}")
    logger.info(f"❌ Failed: {total_failed}")
    
    if total_failed > 0:
        logger.warning(f"⚠️  Nightly ETL completed with {total_failed} failures")
        # Don't raise exception - let partial success stand
    else:
        logger.info("🎉 Nightly FieldRoutes ETL completed successfully")
    

@flow(name="FieldRoutes_CDC_ETL", task_runner=SequentialTaskRunner())
def run_cdc_fieldroutes_etl():
    """CDC flow for high-velocity tables using watermarks.
    Processes offices sequentially to avoid overwhelming the FieldRoutes API."""
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
            ORDER BY o.office_id
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

    # Sort offices by ID
    sorted_offices = sorted(offices.values(), key=lambda x: x["office_id"])
        
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
    
    logger.info(f"Starting CDC FieldRoutes ETL for {len(sorted_offices)} offices")
    logger.info(f"CDC entities: {list(meta_dict.keys())}")
    
    total_success = 0
    total_failed = 0
    
    # Process offices sequentially
    for office_idx, office in enumerate(sorted_offices, 1):
        logger.info(f"🏢 CDC processing office {office_idx}/{len(sorted_offices)}: {office['office_id']} ({office['office_name']})")
        
        office_success = 0
        office_failed = 0
        
        # Process CDC tasks sequentially with proper windows based on watermarks
        for entity_name, meta in meta_dict.items():
            # Use watermark as start time, with 15-minute overlap for safety
            last_run = office["watermarks"].get(entity_name)
            if last_run:
                window_start = last_run - datetime.timedelta(minutes=15)
            else:
                # First run - get last 2 hours
                window_start = now - datetime.timedelta(hours=2)
            

            try:
                fetch_entity(
                    office=office,
                    meta=meta,
                    window_start=window_start,
                    window_end=now
                )
                office_success += 1
                logger.info(
                    f"✅ CDC {entity_name} completed for office {office['office_id']}"
                )
            except Exception as exc:
                office_failed += 1
                logger.error(
                    f"❌ CDC {entity_name} failed for office {office['office_id']}: {exc}"
                )
        
        # Log office completion summary
        logger.info(f"🏢 Office {office['office_id']} CDC completed: {office_success} success, {office_failed} failed")
        total_success += office_success
        total_failed += office_failed
        
        # Brief pause between offices
        if office_idx < len(sorted_offices):
            logger.info("⏸️  Pausing 15 seconds before next office...")
            time.sleep(15)
    
    # Final summary
    logger.info("="*60)
    logger.info("🎯 CDC ETL SUMMARY")
    logger.info("="*60)
    logger.info(f"Total CDC entities processed: {total_success + total_failed}")
    logger.info(f"✅ Successful: {total_success}")
    logger.info(f"❌ Failed: {total_failed}")
    
    if total_failed > 0:
        logger.warning(f"⚠️  CDC ETL completed with {total_failed} failures")
        # Don't raise exception - let partial success stand
    else:
        logger.info("🎉 CDC FieldRoutes ETL completed successfully")

if __name__ == "__main__":
    # For local testing
    run_nightly_fieldroutes_etl()