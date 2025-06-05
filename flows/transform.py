from prefect import flow

from tasks.transform import transform_records


@flow(name="Transform_Raw_to_Staging")
def transform_flow():
    """
    Transform raw JSON in RAW_DB.RAW into structured tables in STAGING_DB.
    """
    # TODO: orchestrate staging transformation logic
    pass