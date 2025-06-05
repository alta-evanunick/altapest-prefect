from typing import List, Dict, Any

from prefect import task


@task
def transform_records(
    office_id: int,
    entity_name: str,
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Transform raw records into flattened dicts for staging.
    """
    # TODO: implement entity-specific flatten/transform logic
    return records