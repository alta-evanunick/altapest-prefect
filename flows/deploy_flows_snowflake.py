"""
Deployment script for FieldRoutes to Snowflake Direct Load
Creates Prefect deployments for both nightly and CDC flows
"""
import datetime
from datetime import timezone
import time
from typing import Dict, Optional
from prefect import flow, get_run_logger
from prefect_snowflake import SnowflakeConnector
from flows.fieldroutes_etl_flow_snowflake import fetch_entity, ENTITY_META

# Define high-velocity entities for CDC
CDC_ENTITIES = [
    "customer", "appointment", "ticket", "payment", 
    "appliedPayment", "task", "note", "knock",
    "subscription", "appliedPayment"
]

@flow(name="FieldRoutes_Nightly_ETL_Snowflake")
def run_nightly_fieldroutes_etl(
    test_office_id: Optional[int] = None,
    test_entity: Optional[str] = None
):
    """Prefect flow to perform a full nightly extract for all offices and entities.
    Writes directly to Snowflake, bypassing Azure."""
    logger = get_run_logger()
    now = datetime.datetime.now(timezone.utc)
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
            FROM   RAW_DB.REF.offices_lookup o
            JOIN   RAW_DB.REF.office_entity_watermark w USING (office_id)
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
            "primary_date": meta[4],
            "secondary_date": meta[5],
            "unique_params": meta[6]
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
                    f"{entity_name} failed for office {office['office_id']}: {exc}"
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
            FROM   RAW_DB.REF.offices_lookup o
            JOIN   RAW_DB.REF.office_entity_watermark w USING (office_id)
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
    
    now = datetime.datetime.now(timezone.utc)
    
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
                    f"CDC {entity_name} failed for office {office['office_id']}: {exc}"
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
    Optimized for 5-deployment limit
    """
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule
    from transform_to_analytics_flow import transform_raw_to_staging
    
    print("Creating FieldRoutes ETL and Staging deployments...")
    
    # Deployment 1: Nightly ETL (3 AM PT daily)
    nightly_deployment = Deployment.build_from_flow(
        flow=run_nightly_fieldroutes_etl,
        name="fieldroutes-nightly-etl",
        work_queue_name="default",
        schedule=CronSchedule(cron="0 3 * * *", timezone="America/Los_Angeles"),
        tags=["fieldroutes", "nightly", "etl"],
        description="Nightly ETL from FieldRoutes API to RAW_DB.FIELDROUTES"
    )
    nightly_deployment.apply()
    print("✅ [1/3] Created: fieldroutes-nightly-etl")
    
    # Deployment 2: CDC ETL (Every 2 hours, business hours)
    cdc_deployment = Deployment.build_from_flow(
        flow=run_cdc_fieldroutes_etl,
        name="fieldroutes-cdc-etl",
        work_queue_name="default",
        schedule=CronSchedule(cron="0 8-18/2 * * 1-5", timezone="America/Los_Angeles"),
        tags=["fieldroutes", "cdc", "etl"],
        description="Change Data Capture ETL for high-velocity entities"
    )
    cdc_deployment.apply()
    print("✅ [2/3] Created: fieldroutes-cdc-etl")
    
    # Deployment 3: Staging Transformation (Every hour)
    staging_deployment = Deployment.build_from_flow(
        flow=transform_raw_to_staging,
        name="raw-to-staging-transform-complete",
        work_queue_name="default",
        schedule=CronSchedule(cron="15 * * * *", timezone="America/Los_Angeles"),  # 15 minutes after the hour
        parameters={"incremental": True, "run_quality_checks": True},
        tags=["staging", "transformation", "snowflake"],
        description="Transform RAW_DB data to STAGING_DB.FIELDROUTES for analytics"
    )
    staging_deployment.apply()
    print("✅ [3/3] Created: raw-to-staging-transform-complete")
    
    print("\n🎉 All deployments created successfully!")
    print(f"\nUsing 3 of your 5 available deployment slots")
    print("\nSchedule Summary:")
    print("  • Nightly ETL:     3:00 AM PT daily")
    print("  • CDC ETL:         Every 2 hours (8 AM, 10 AM, 12 PM, 2 PM, 4 PM, 6 PM PT, weekdays)")
    print("  • Staging:         Every hour at :15 minutes")
    
    print("\nManual execution commands:")
    print("  prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-etl'")
    print("  prefect deployment run 'FieldRoutes_CDC_ETL_Snowflake/fieldroutes-cdc-etl'")
    print("  prefect deployment run 'transform-raw-to-staging-complete/raw-to-staging-transform-complete'")
    
    # Remove local testing line to keep it clean