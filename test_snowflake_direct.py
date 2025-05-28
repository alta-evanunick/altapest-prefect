"""
Test script for FieldRoutes API to Snowflake direct load
Tests a single entity for a single office to verify the pipeline works
"""
import sys
import os
import logging
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import directly from the snowflake flow module
import flows.fieldroutes_etl_flow_snowflake as snowflake_flow
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_single_entity(office_id: int = None, entity_name: str = "customer"):
    """Test loading a single entity from a single office"""
    
    logger.info(f"Starting test for entity: {entity_name}")
    
    # First validate schema
    logger.info("Validating Snowflake schema...")
    if not snowflake_flow.validate_snowflake_schema():
        logger.error("Schema validation failed!")
        return False
    
    # Get office configuration
    sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_connector.get_connection() as conn:
        cursor = conn.cursor()
        
        if office_id:
            cursor.execute("""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW.REF.offices_lookup
                WHERE office_id = %s AND active = TRUE
            """, (office_id,))
        else:
            cursor.execute("""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW.REF.offices_lookup
                WHERE active = TRUE
                LIMIT 1
            """)
        
        row = cursor.fetchone()
        if not row:
            logger.error("No active office found!")
            return False
            
        office_id, name, base_url, key_block, token_block = row
    
    # Build office dict
    office = {
        "office_id": office_id,
        "office_name": name,
        "base_url": base_url,
        "auth_key": Secret.load(key_block).get(),
        "auth_token": Secret.load(token_block).get(),
    }
    
    logger.info(f"Testing office {office_id} - {name}")
    logger.info(f"Base URL: {base_url}")
    
    # Find entity metadata
    ENTITY_META = snowflake_flow.ENTITY_META
    
    meta = None
    for ep, tbl, dim, small, df in ENTITY_META:
        if ep == entity_name:
            meta = {
                "endpoint": ep,
                "table": tbl,
                "is_dim": dim,
                "small": small,
                "date_field": df
            }
            break
    
    if not meta:
        logger.error(f"Entity {entity_name} not found in ENTITY_META")
        return False
    
    # Test with a small time window (last 7 days)
    now = datetime.now()
    window_start = now - timedelta(days=7)
    window_end = now
    
    logger.info(f"Testing incremental load from {window_start} to {window_end}")
    
    try:
        fetched, loaded = snowflake_flow.fetch_entity(
            office=office,
            meta=meta,
            window_start=window_start,
            window_end=window_end
        )
        
        logger.info(f"✅ SUCCESS: Fetched {fetched} records, loaded {loaded} records")
        
        # Verify data in Snowflake
        with sf_connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) as record_count,
                       MIN(_loaded_at) as earliest_load,
                       MAX(_loaded_at) as latest_load
                FROM RAW.fieldroutes.{meta['table']}
                WHERE OfficeID = %s
                AND _loaded_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
            """, (office_id,))
            
            count, earliest, latest = cursor.fetchone()
            logger.info(f"Verification: {count} records in Snowflake, loaded between {earliest} and {latest}")
            
            # Show sample record
            cursor.execute(f"""
                SELECT RawData
                FROM RAW.fieldroutes.{meta['table']}
                WHERE OfficeID = %s
                ORDER BY _loaded_at DESC
                LIMIT 1
            """, (office_id,))
            
            sample = cursor.fetchone()
            if sample:
                import json
                logger.info(f"Sample record: {json.dumps(sample[0], indent=2)[:500]}...")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ FAILED: {str(e)}", exc_info=True)
        return False


def test_full_pipeline():
    """Test the full pipeline with multiple entities"""
    
    logger.info("Running full pipeline test...")
    
    # Test dimension tables first (they're smaller)
    test_entities = ["office", "region", "serviceType", "customer", "appointment"]
    
    results = {}
    for entity in test_entities:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing entity: {entity}")
        logger.info(f"{'='*60}")
        
        success = test_single_entity(entity_name=entity)
        results[entity] = success
        
        if not success:
            logger.warning(f"Skipping remaining tests due to failure in {entity}")
            break
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    
    for entity, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        logger.info(f"{entity}: {status}")
    
    all_passed = all(results.values())
    if all_passed:
        logger.info("\n🎉 All tests passed!")
    else:
        logger.error("\n❌ Some tests failed. Check logs above for details.")
    
    return all_passed


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test FieldRoutes to Snowflake direct load")
    parser.add_argument("--office-id", type=int, help="Specific office ID to test")
    parser.add_argument("--entity", type=str, default="customer", help="Entity to test")
    parser.add_argument("--full", action="store_true", help="Run full pipeline test")
    
    args = parser.parse_args()
    
    if args.full:
        success = test_full_pipeline()
    else:
        success = test_single_entity(office_id=args.office_id, entity_name=args.entity)
    
    sys.exit(0 if success else 1)