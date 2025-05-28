"""
Deployment script for FieldRoutes to Snowflake Direct Load
Creates Prefect deployments for both nightly and CDC flows
"""
import datetime
import time
from typing import Dict, Optional
from prefect import flow, get_run_logger
from prefect_snowflake import SnowflakeConnector
from flows.fieldroutes_etl_flow_snowflake import fetch_entity, ENTITY_META

# Define high-velocity entities for CDC
CDC_ENTITIES = [
    "appointment", "ticket", "ticketItem", "payment", 
    "appliedPayment", "route", "subscription"
]

@flow(name="FieldRoutes_Nightly_ETL_Snowflake")
def run_nightly_fieldroutes_etl(
    test_office_id: Optional[int] = None,
    test_entity: Optional[str] = None
):
    """Prefect flow to perform a full nightly extract for all offices and entities.
    Writes directly to Snowflake, bypassing Azure."""
    logger = get_run_logger()
    now = datetime.datetime.utcnow()
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
    
    # Apply test filter
    if test_office_id:
        sorted_offices = [o for o in sorted_offices if o['office_id'] == test_office_id]
        logger.info(f"TEST MODE: Only processing office {test_office_id}")
    
    logger.info(f"Starting nightly FieldRoutes ETL (Direct to Snowflake) for {len(sorted_offices)} offices")
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
    
    # Apply entity test filter
    if test_entity:
        meta_dict = {k: v for k, v in meta_dict.items() if k == test_entity}
        logger.info(f"TEST MODE: Only processing entity {test_entity}")
    
    total_success = 0
    total_failed = 0
    
    # Process offices sequentially
    for office_idx, office in enumerate(sorted_offices, 1):
        logger.info(f"Processing office {office_idx}/{len(sorted_offices)}: {office['office_id']} ({office['office_name']})")
        
        office_success = 0
        office_failed = 0

        # Process each entity sequentially for this office
        for entity_name, meta in meta_dict.items():
            try:
                fetched, loaded = fetch_entity(
                    office=office,
                    meta=meta,
                    window_start=window_start,
                    window_end=window_end
                )
                office_success += 1
                logger.info(
                    f"✅ {entity_name} completed for office {office['office_id']}: "
                    f"{fetched} fetched, {loaded} loaded"
                )
            except Exception as exc:
                office_failed += 1
                logger.error(
                    f"❌ {entity_name} failed for office {office['office_id']}: {exc}"
                )

        # Log office completion summary
        logger.info(f"Office {office['office_id']} completed: {office_success} success, {office_failed} failed")
        total_success += office_success
        total_failed += office_failed
        
        # Brief pause between offices to be API-friendly
        if office_idx < len(sorted_offices):  # Don't sleep after the last office
            logger.info("Pausing 30 seconds before next office...")
            time.sleep(30)
    
    # Final summary
    logger.info("="*60)
    logger.info("NIGHTLY ETL SUMMARY (Direct to Snowflake)")
    logger.info("="*60)
    logger.info(f"Total entities processed: {total_success + total_failed}")
    logger.info(f"Successful: {total_success}")
    logger.info(f"Failed: {total_failed}")
    
    if total_failed > 0:
        logger.warning(f"Nightly ETL completed with {total_failed} failures")
        # Don't raise exception - let partial success stand
    else:
        logger.info("Nightly FieldRoutes ETL completed successfully")


@flow(name="FieldRoutes_CDC_ETL_Snowflake")
def run_cdc_fieldroutes_etl():
    """CDC flow for high-velocity tables using watermarks.
    Processes offices sequentially to avoid overwhelming the FieldRoutes API.
    Writes directly to Snowflake, bypassing Azure."""
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
    
    now = datetime.datetime.utcnow()
    
    logger.info(f"Starting CDC FieldRoutes ETL (Direct to Snowflake) for {len(sorted_offices)} offices")
    logger.info(f"CDC entities: {list(meta_dict.keys())}")
    
    total_success = 0
    total_failed = 0
    
    # Process offices sequentially
    for office_idx, office in enumerate(sorted_offices, 1):
        logger.info(f"CDC processing office {office_idx}/{len(sorted_offices)}: {office['office_id']} ({office['office_name']})")
        
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
                fetched, loaded = fetch_entity(
                    office=office,
                    meta=meta,
                    window_start=window_start,
                    window_end=now
                )
                office_success += 1
                logger.info(
                    f"✅ CDC {entity_name} completed for office {office['office_id']}: "
                    f"{fetched} fetched, {loaded} loaded"
                )
            except Exception as exc:
                office_failed += 1
                logger.error(
                    f"❌ CDC {entity_name} failed for office {office['office_id']}: {exc}"
                )
        
        # Log office completion summary
        logger.info(f"Office {office['office_id']} CDC completed: {office_success} success, {office_failed} failed")
        total_success += office_success
        total_failed += office_failed
        
        # Brief pause between offices
        if office_idx < len(sorted_offices):
            logger.info("Pausing 15 seconds before next office...")
            time.sleep(15)
    
    # Final summary
    logger.info("="*60)
    logger.info("CDC ETL SUMMARY (Direct to Snowflake)")
    logger.info("="*60)
    logger.info(f"Total CDC entities processed: {total_success + total_failed}")
    logger.info(f"Successful: {total_success}")
    logger.info(f"Failed: {total_failed}")
    
    if total_failed > 0:
        logger.warning(f"CDC ETL completed with {total_failed} failures")
        # Don't raise exception - let partial success stand
    else:
        logger.info("CDC FieldRoutes ETL completed successfully")


if __name__ == "__main__":
    """
    Create Prefect deployments for the flows
    """
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule
    
    # Create nightly deployment
    nightly_deployment = Deployment.build_from_flow(
        flow=run_nightly_fieldroutes_etl,
        name="fieldroutes-nightly-snowflake-direct",
        work_queue_name="default",
        schedule=CronSchedule(cron="0 3 * * *", timezone="America/Los_Angeles"),  # 3 AM PT daily
        tags=["fieldroutes", "nightly", "snowflake", "direct"],
        description="Nightly ETL from FieldRoutes API directly to Snowflake (bypassing Azure)"
    )
    nightly_deployment.apply()
    print("✅ Created nightly deployment: fieldroutes-nightly-snowflake-direct")
    
    # Create CDC deployment
    cdc_deployment = Deployment.build_from_flow(
        flow=run_cdc_fieldroutes_etl,
        name="fieldroutes-cdc-snowflake-direct",
        work_queue_name="default",
        schedule=CronSchedule(cron="0 8-18/2 * * 1-5", timezone="America/Los_Angeles"),  # Every 2 hours, 8 AM - 6 PM PT, weekdays
        tags=["fieldroutes", "cdc", "snowflake", "direct"],
        description="CDC ETL for high-velocity entities from FieldRoutes API directly to Snowflake"
    )
    cdc_deployment.apply()
    print("✅ Created CDC deployment: fieldroutes-cdc-snowflake-direct")
    
    print("\n🎉 Deployments created successfully!")
    print("\nTo run deployments manually:")
    print("  prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct'")
    print("  prefect deployment run 'FieldRoutes_CDC_ETL_Snowflake/fieldroutes-cdc-snowflake-direct'")
    
    print("\nTo test with a single office/entity:")
    print("  python -m flows.deploy_flows_snowflake")
    
    # For local testing - uncomment to test with a single office/entity
    # run_nightly_fieldroutes_etl(test_office_id=1, test_entity="customer")