from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

from flows.etl import etl_flow
from flows.transform import transform_flow

# Nightly full ETL
Deployment.build_from_flow(
    flow=etl_flow,
    name="nightly_etl",
    schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),
)

# Business-hours CDC every 2 hours Mon-Fri
Deployment.build_from_flow(
    flow=etl_flow,
    name="cdc_2h_business",
    schedule=CronSchedule(cron="0 */2 * * MON-FRI", timezone="UTC"),
    parameters={"start_date": None, "end_date": None},
)

# Transform flow after raw load
Deployment.build_from_flow(
    flow=transform_flow,
    name="transform_raw_to_staging",
)