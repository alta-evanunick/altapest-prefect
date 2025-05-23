import datetime
from prefect import flow, get_run_logger
from prefect_snowflake import SnowflakeConnector
from fieldroutes_etl_flow import process_office_data  # import the task defined above

@flow(name="FieldRoutes_Nightly_Flow")
def run_nightly_fieldroutes_etl():
    """Prefect flow to perform a full nightly extract for all offices and entities."""
    logger = get_run_logger()
    # 1. Retrieve all offices and last run timestamps from Snowflake config table
    offices = []
    with SnowflakeConnector.load("snowflake-altapestdb") as sf_conn:
        cur = sf_conn.cursor()
        cur.execute("SELECT OfficeID, OfficeName, LastSuccessfulRunUTC FROM Config.OfficeLastRun;")
        for row in cur.fetchall():
            office_id, office_name, last_run_ts = row
            offices.append({
                "id": int(office_id),
                "name": office_name,
                "last_run": last_run_ts  # datetime or None
            })
    logger.info(f"Starting nightly FieldRoutes ETL for {len(offices)} offices.")
    # 2. Launch parallel tasks for each office
    results = []
    for office in offices:
        # Submit the task concurrently for each office (ThreadPoolTaskRunner handles parallel threads)
        res = process_office_data.submit(office, update_config=True)
        results.append(res)
    # 3. Wait for all office tasks to complete and check for errors
    for res in results:
        try:
            res.result()  # will raise exception if the task failed
        except Exception as exc:
            logger.error(f"Office ETL sub-flow failed: {exc}")
            # We continue to let other offices finish; the flow can be marked failed at end if needed
    logger.info("Nightly FieldRoutes ETL flow completed.")

@flow(name="FieldRoutes_CDC_Flow")
def run_cdc_fieldroutes_etl():
    """Prefect flow to perform a 2-hour interval CDC for high-velocity tables during business hours."""
    logger = get_run_logger()
    # Determine the CDC window: last 2 hours (with a small overlap of 15 min for safety)
    now = datetime.datetime.utcnow()
    window_start = now - datetime.timedelta(hours=2, minutes=15)
    window_end = now
    # Optionally, we could filter offices or entities here if needed
    offices = []
    with SnowflakeConnector.load("snowflake-altapestdb") as sf_conn:
        cur = sf_conn.cursor()
        cur.execute("SELECT OfficeID, OfficeName FROM Config.OfficeLastRun;")
        for row in cur.fetchall():
            office_id, office_name = row
            offices.append({
                "id": int(office_id),
                "name": office_name,
                "last_run": None  # not used in CDC mode
            })
    logger.info(f"Starting CDC FieldRoutes ETL for {len(offices)} offices. Window: {window_start} to {window_end}")
    results = []
    for office in offices:
        # Only process selected high-velocity entities: we pass the window to the task
        res = process_office_data.submit(office, from_time=window_start, to_time=window_end, update_config=False)
        results.append(res)
    for res in results:
        try:
            res.result()
        except Exception as exc:
            logger.error(f"Office CDC sub-flow failed: {exc}")
    logger.info("CDC FieldRoutes ETL flow completed.")
